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
-export([read_data_item/6,
	 check_partition_ready/3,
	 start_read_servers/2,
	 stop_read_servers/2,
	 reverse_write_set_to_updates/4]).

%% Spawn
-record(state, {partition :: partition_id(),
		id :: non_neg_integer(),
		ops_cache :: cache_id(),
		snapshot_cache :: cache_id(),
		prepared_cache :: cache_id(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc This starts a gen_server responsible for servicing reads to key
%%      handled by this Partition.  To allow for read concurrency there
%%      can be multiple copies of these servers per parition, the Id is
%%      used to distinguish between them.  Since these servers will be
%%      reading from ets tables shared by the clock_si and materializer
%%      vnodes, they should be started on the same physical nodes as
%%      the vnodes with the same partition.
-spec start_link(partition_id(),non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Partition,Id) ->
    Addr = node(),
    gen_server:start_link({global,generate_server_name(Addr,Partition,Id)}, ?MODULE, [Partition,Id], []).

-spec start_read_servers(partition_id(),non_neg_integer()) -> non_neg_integer().
start_read_servers(Partition, Count) ->
    Addr = node(),
    start_read_servers_internal(Addr, Partition, Count).

-spec stop_read_servers(partition_id(),non_neg_integer()) -> ok.
stop_read_servers(Partition, Count) ->
    Addr = node(),
    stop_read_servers_internal(Addr, Partition, Count).

-spec read_data_item(index_node(), key(), type(), tx(), atom(), boolean()) -> {error, term()} | {ok, snapshot()}.
read_data_item({Partition,Node},Key,Type,Transaction,IsLocal,Replicated) ->
    case Replicated of
	true ->
	    try
		gen_server:call({global,generate_random_server_name(Node,Partition)},
				{perform_read,Key,Type,Transaction,IsLocal},infinity)
	    catch
		_:Reason ->
		    lager:error("Exception caught: ~p, starting read server to fix", [Reason]),
		    check_server_ready([{Partition,Node}]),
		    read_data_item({Partition,Node},Key,Type,Transaction,IsLocal,Replicated)
	    end;
	false ->
	    send_external_read(Key, Type, Transaction)
    end.

%% @doc This checks all partitions in the system to see if all read
%%      servers have been started up.
%%      Returns true if they have been, false otherwise.
-spec check_servers_ready() -> boolean().
check_servers_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_server_ready(PartitionList).

-spec check_server_ready([index_node()]) -> boolean().
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

-spec check_partition_ready(node(), partition_id(), non_neg_integer()) -> boolean().
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

%% helper function
loop_reads([Dc|T], Type, Key, Transaction, AllDc) ->
    try
	case inter_dc_communication_sender:perform_external_read(Dc,Key,Type,Transaction) of
	    {ok, {error, Reason1}} ->
		loop_reads(T,Type,Key,Transaction,AllDc);
	    {ok, {{ok, SS1}, _Dc}} ->
		{ok, SS1};
	    Result ->
		loop_reads(T,Type,Key,Transaction,AllDc)
	end
    catch
	_:_Reason ->
	    loop_reads(T,Type,Key,Transaction,AllDc)
    end;
loop_reads([], Type, Key, Transaction, AllDc) ->
    timer:sleep(?CONNECTION_SLEEP_EXT_READ_LOOP),
    loop_reads(AllDc,Type,Key,Transaction,AllDc).

send_external_read(Key, Type, Transaction) ->
    Dcs = replication_check:get_dc_replicas_read(Key,noSelf),
    case loop_reads(Dcs, Type, Key, Transaction, Dcs) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        error ->
            {error, failed_read_at_all_dcs}
    end.


start_read_servers_internal(_Node,_Partition,0) ->
    0;
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

handle_call({perform_read, Key, Type, Transaction, IsLocal},Coordinator,
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,partition=Partition}) ->
    ok = perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Partition,IsLocal),
    {noreply,SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction, IsLocal},
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,partition=Partition}) ->
    ok = perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Partition,IsLocal),
    {noreply,SD0}.

perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Partition,IsLocal) ->
    case check_clock(Key,Transaction,PreparedCache,Partition,IsLocal) of
	{not_ready,Time} ->
	    lager:info("spinning read"),
	    %% spin_wait(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Self);
	    _Tref = erlang:send_after(Time, self(), {perform_read_cast,Coordinator,Key,Type,Transaction, IsLocal}),
	    ok;
	ready ->
	    lager:info("ready to read"),
	    return(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,Partition)
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
%% For partial-rep, when a read is coming from another DC it also
%% waits until all the dependencies of that read are satisfied
check_clock(Key,Transaction,PreparedCache,Partition,IsLocal) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = vectorclock:now_microsec(erlang:now()),
    case IsLocal of
	local -> 
	    case T_TS > Time of
		true ->
		    {not_ready, (T_TS - Time) div 1000 +1};
		false ->
		    check_prepared(Key,Transaction,PreparedCache,Partition)
	    end;
	external ->
	    %% This could cause really long waiting, because the time of the
	    %% origin DC is using time:now() for its time, instead of using
	    %% time based on one of its dependencies
	    %% Should fix this
	    lager:info("waiting for clock for external read"),
	    vectorclock:wait_for_clock(Transaction#transaction.vec_snapshot_time),
	    check_prepared(Key,Transaction,PreparedCache,Partition)
    end.

%% @doc check_prepared: Check if there are any transactions
%%      being prepared on the tranaction being read, and
%%      if they could violate the correctness of the read
check_prepared(Key,Transaction,PreparedCache,Partition) ->
    TxId = Transaction#transaction.txn_id,
    SnapshotTime = TxId#tx_id.snapshot_time,
    {ok, ActiveTxs} = clocksi_vnode:get_active_txns_key(Key,Partition,PreparedCache),
    check_prepared_list(Key,SnapshotTime,ActiveTxs).

check_prepared_list(_Key,_SnapshotTime,[]) ->
    ready;
check_prepared_list(Key,SnapshotTime,[{_TxId,Time}|Rest]) ->
    case Time =< SnapshotTime of
	true ->
	    lager:info("spin wait"),
	    {not_ready, ?SPIN_WAIT};
	false ->
	    check_prepared_list(Key,SnapshotTime,Rest)
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,Partition) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    TxId = Transaction#transaction.txn_id,
    Reply = case materializer_vnode:read(Key, Type, VecSnapshotTime, TxId,OpsCache,SnapshotCache, Partition) of
		{ok, Snapshot} ->
		    {ok, Snapshot};
		{error, Reason} ->
		    lager:error("error in return read ~p", [Reason]),
		    {error, Reason}
	    end,
    _Ignore=gen_server:reply(Coordinator, Reply),
    ok.


handle_info({perform_read_cast, Coordinator, Key, Type, Transaction, IsLocal},
	    SD0=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache,prepared_cache=PreparedCache,partition=Partition}) ->
    ok = perform_read_internal(Coordinator,Key,Type,Transaction,OpsCache,SnapshotCache,PreparedCache,Partition, IsLocal),
    {noreply,SD0};

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

reverse_write_set_to_updates(Txn, WriteSet, Key, ExtSnapshots) ->
    lists:foldl(fun({Replicated,KeyPrime,Type,Op}, Acc) ->
			case KeyPrime==Key of
			    true ->
				case Replicated of
				    isReplicated ->
					[Op | Acc];
				    notReplicated ->
					{ok, DownstreamRecord} = 
					    clocksi_downstream:generate_downstream_op(
					      Txn, {0,node()}, Key, Type, Op, Acc, ExtSnapshots, local),
					[DownstreamRecord | Acc]
				end;
			    false ->
				Acc
			end
		end,[],WriteSet).

%% Internal functions

%% get_stable_time(Key) ->
%%     Preflist = log_utilities:get_preflist_from_key(Key),
%%     Node = hd(Preflist),
%%     case riak_core_vnode_master:sync_command(
%%            Node, {get_active_txns}, ?CLOCKSI_MASTER) of
%%         {ok, Active_txns} ->
%%             lists:foldl(fun({_,{_TxId, Snapshot_time}}, Min_time) ->
%%                                 case Min_time > Snapshot_time of
%%                                     true ->
%%                                         Snapshot_time;
%%                                     false ->
%%                                         Min_time
%%                                 end
%%                         end,
%%                         vectorclock:now_microsec(erlang:now()),
%%                         Active_txns);
%%         _ -> 0
%%     end.

