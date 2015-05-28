%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(clocksi_readitem_fsm).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([read_data_item/5,
	 start_read_servers/1,
	 stop_read_servers/1]).

%% Spawn

-record(state, {partition,id,ops_cache,snapshot_cache,prepared_cache,self}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition,Id) ->
    gen_server:start_link({global,generate_server_name(Partition,Id)}, ?MODULE, [Partition,Id], []).

start_read_servers(Partition) ->
    start_read_servers_internal(Partition, ?READ_CONCURRENCY).

stop_read_servers(Partition) ->
    stop_read_servers_internal(Partition, ?READ_CONCURRENCY).


read_data_item({Partition,_Node},Key,Type,Transaction,Updates) ->
    try
	gen_server:call({global,generate_random_server_name(Partition)},
			{perform_read,Key,Type,Transaction,Updates})
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.




%%%===================================================================
%%% Internal
%%%===================================================================

start_read_servers_internal(_Partition,0) ->
    ok;
start_read_servers_internal(Partition, Num) ->
    clocksi_readitem_sup:start_fsm(Partition,Num),
    start_read_servers_internal(Partition, Num-1).

stop_read_servers_internal(_Partition,0) ->
    ok;
stop_read_servers_internal(Partition, Num) ->
    gen_server:call({global,generate_server_name(Partition,Num)},{go_down}),
    stop_read_servers_internal(Partition, Num-1).


generate_server_name(Partition, Id) ->
    list_to_atom(integer_to_list(Id) ++ "rs" ++ integer_to_list(Partition)).

generate_random_server_name(Partition) ->
    generate_server_name(Partition, random:uniform(?READ_CONCURRENCY)).

init([Partition, Id]) ->
    OpsCache = materializer_vnode:get_cache_name(Partition,ops_cache),
    SnapshotCache = materializer_vnode:get_cache_name(Partition,snapshot_cache),
    PreparedCache = clocksi_vnode:get_cache_name(Partition,prepared),
    Self = generate_server_name(Partition,Id),
    {ok, #state{partition=Partition, id=Id, ops_cache=OpsCache,
		snapshot_cache=SnapshotCache,
		prepared_cache=PreparedCache,self=Self}}.

handle_call({perform_read, Key, Type, Transaction, Updates},Coordinator,
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,self=Self}) ->
    perform_read_internal(Coordinator,Key,Type,Transaction,Updates,OpsCache,SnapshotCache,PreparedCache,Self),
    {noreply,SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,handoff,ok,SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction, Updates},
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,self=Self}) ->
    perform_read_internal(Coordinator,Key,Type,Transaction,Updates,OpsCache,SnapshotCache,PreparedCache,Self),
    {noreply,SD0}.

perform_read_internal(Coordinator,Key,Type,Transaction,Updates,OpsCache,SnapshotCache,PreparedCache,Self) ->
    case check_clock(Transaction,PreparedCache) of
	not_ready ->
	    gen_server:cast({global,Self},{perform_read_cast,Coordinator,Key,Type,Transaction,Updates});
	ready ->
	    return(Coordinator,Key,Type,Transaction,Updates,OpsCache,SnapshotCache)
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
check_clock(Transaction,PreparedCache) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = clocksi_vnode:now_microsec(erlang:now()),
    case T_TS > Time of
        true ->
	    %% dont sleep
            timer:sleep((T_TS - Time) div 1000 +1 );
        false ->
		ok	    
    end,
    waiting1(Transaction,PreparedCache).

waiting1(Transaction,PreparedCache) ->
    LocalClock = clocksi_vnode:now_microsec(erlang:now()),
    TxId = Transaction#transaction.txn_id,
    SnapshotTime = TxId#tx_id.snapshot_time,
    case LocalClock > SnapshotTime of
        false ->
	    not_ready;
        true ->
	    check_prepared(Transaction,PreparedCache)
    end.


check_prepared(Transaction,PreparedCache) ->
    TxId = Transaction#transaction.txn_id,
    SnapshotTime = TxId#tx_id.snapshot_time,
    ActiveTxs = 
	case ets:lookup(PreparedCache, active) of
	    [] ->
		[];
	    [{active,AList}] ->
		AList
	end,
    case ActiveTxs of
	[] ->
	    ready;
	List ->
	    {_TxId,Time} = lists:last(List),
	    case Time =< SnapshotTime of
		true ->
		    %% How long should sleep here?
		    not_ready;
		false ->
		    ready
	    end
    end.


%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Coordinator,Key,Type,Transaction,Updates,OpsCache,SnapshotCache) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    TxId = Transaction#transaction.txn_id,
    case materializer_vnode:read(Key, Type, VecSnapshotTime, TxId,OpsCache,SnapshotCache) of
        {ok, Snapshot} ->
            Updates2=filter_updates_per_key(Updates, Key),
            Snapshot2=clocksi_materializer:materialize_eager
                        (Type, Snapshot, Updates2),
            Reply={ok, Snapshot2};
        {error, Reason} ->
            Reply={error, Reason}
    end,
    gen_server:reply(Coordinator, Reply).

handle_info(_Info, StateData) ->
    {no_reply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

%% Internal functions
filter_updates_per_key(Updates, Key) ->
    FilterMapFun = fun ({KeyPrime, _Type, Op}) ->
        case KeyPrime == Key of
            true  -> {true, Op};
            false -> false
        end
    end,
    lists:filtermap(FilterMapFun, Updates).


-ifdef(TEST).

%% @doc Testing filter_updates_per_key.
filter_updates_per_key_test()->
    Op1 = {update, {{increment,1}, actor1}},
    Op2 = {update, {{increment,2}, actor1}},
    Op3 = {update, {{increment,3}, actor1}},
    Op4 = {update, {{increment,4}, actor1}},

    ClockSIOp1 = {a, crdt_pncounter, Op1},
    ClockSIOp2 = {b, crdt_pncounter, Op2},
    ClockSIOp3 = {c, crdt_pncounter, Op3},
    ClockSIOp4 = {a, crdt_pncounter, Op4},

    ?assertEqual([Op1, Op4], 
        filter_updates_per_key([ClockSIOp1, ClockSIOp2, ClockSIOp3, ClockSIOp4], a)).

-endif.
