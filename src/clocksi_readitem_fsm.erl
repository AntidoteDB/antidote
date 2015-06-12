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
	 check_servers_ready/0,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([read_data_item/4,
	 check_partition_ready/3,
	 start_read_servers/1,
	 stop_read_servers/1]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
		id :: non_neg_integer(),
		ops_cache :: cache_id(),
		snapshot_cache :: cache_id(),
		prepared_cache :: cache_id(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition,Id) ->
    Addr = node(),
    gen_server:start_link({global,generate_server_name(Addr,Partition,Id)}, ?MODULE, [Partition,Id], []).

start_read_servers(Partition) ->
    Addr = node(),
    start_read_servers_internal(Addr, Partition, ?READ_CONCURRENCY).

stop_read_servers(Partition) ->
    Addr = node(),
    stop_read_servers_internal(Addr, Partition, ?READ_CONCURRENCY).


read_data_item({Partition,Node},Key,Type,Transaction) ->
    try
	gen_server:call({global,generate_random_server_name(Node,Partition)},
			{perform_read,Key,Type,Transaction},infinity)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.


check_servers_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_server_ready(PartitionList).

check_server_ready([]) ->
    true;
check_server_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_servers_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	false ->
	    false;
	true ->
	    check_server_ready(Rest)
    end.

check_partition_ready(_Node,_Partition,0) ->
    true;
check_partition_ready(Node,Partition,Num) ->
    case global:whereis_name(generate_server_name(Node,Partition,Num)) of
	undefined ->
	    false;
	_Res ->
	    check_partition_ready(Node,Partition,Num-1)
    end.



%%%===================================================================
%%% Internal
%%%===================================================================

start_read_servers_internal(_Node,_Partition,0) ->
    ok;
start_read_servers_internal(Node, Partition, Num) ->
    {ok,_Id} = clocksi_readitem_sup:start_fsm(Partition,Num),
    start_read_servers_internal(Node, Partition, Num-1).

stop_read_servers_internal(_Node,_Partition,0) ->
    ok;
stop_read_servers_internal(Node,Partition, Num) ->
    try
	gen_server:call({global,generate_server_name(Node,Partition,Num)},{go_down})
    catch
	_:_Reason->
	    ok
    end,
    stop_read_servers_internal(Node, Partition, Num-1).


generate_server_name(Node, Partition, Id) ->
    list_to_atom(integer_to_list(Id) ++ integer_to_list(Partition) ++ atom_to_list(Node)).

generate_random_server_name(Node, Partition) ->
    generate_server_name(Node, Partition, random:uniform(?READ_CONCURRENCY)).

init([Partition, Id]) ->
    Addr = node(),
    OpsCache = materializer_vnode:get_cache_name(Partition,ops_cache),
    SnapshotCache = materializer_vnode:get_cache_name(Partition,snapshot_cache),
    PreparedCache = clocksi_vnode:get_cache_name(Partition,prepared),
    Self = generate_server_name(Addr,Partition,Id),
    {ok, #state{partition=Partition, id=Id, ops_cache=OpsCache,
		snapshot_cache=SnapshotCache,
		prepared_cache=PreparedCache,self=Self}}.

handle_call({perform_read, Key, Type, Transaction},Coordinator,
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,self=Self}) ->
    perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self),
    {noreply,SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction},
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,self=Self}) ->
    perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self),
    {noreply,SD0}.

perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self) ->
    case check_clock(Key,Transaction,PreparedCache) of
	not_ready ->
	    spin_wait(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self);
	    %%perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self);
	ready ->
	    return(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache)
    end.

spin_wait(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self) ->
    {message_queue_len,Length} = process_info(self(), message_queue_len),
    case Length of
	0 ->
	    timer:sleep(?SPIN_WAIT),
	    perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self);
	_ ->
	    gen_server:cast({global,Self},{perform_read_cast,Coordinator,Key,Type,Transaction})
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
check_clock(Key,Transaction,PreparedCache) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = clocksi_vnode:now_microsec(erlang:now()),
    case T_TS > Time of
        true ->
	    %% dont sleep in case there is another read waiting
            %% timer:sleep((T_TS - Time) div 1000 +1 );
	    not_ready;
        false ->
	    check_prepared(Key,Transaction,PreparedCache)
    end.


check_prepared(Key,Transaction,PreparedCache) ->
    TxId = Transaction#transaction.txn_id,
    SnapshotTime = TxId#tx_id.snapshot_time,
    ActiveTxs = 
	case ets:lookup(PreparedCache, Key) of
	    [] ->
		[];
	    [{Key,AList}] ->
		AList
	end,
    check_prepared_list(Key,SnapshotTime,ActiveTxs).

check_prepared_list(_Key,_SnapshotTime,[]) ->
    ready;
check_prepared_list(Key,SnapshotTime,[{_TxId,Time}|Rest]) ->
    case Time =< SnapshotTime of
	true ->
	    not_ready;
	false ->
	    check_prepared_list(Key,SnapshotTime,Rest)
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    TxId = Transaction#transaction.txn_id,
    case materializer_vnode:read(Key, Type, VecSnapshotTime, TxId,OpsCache,SnapshotCache) of
        {ok, Snapshot} ->
            Reply={ok, Snapshot};
        {error, Reason} ->
            Reply={error, Reason}
    end,
    gen_server:reply(Coordinator, Reply).

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

