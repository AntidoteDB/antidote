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
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 read_data_item/5,
	 get_cache_name/2,
         prepare/2,
         commit/3,
         single_commit/2,
         abort/2,
         now_microsec/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
	 check_tables_ready/0,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_tx: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          active_txs_per_key: a list of the active transactions that
%%              have updated a key (but not yet finished).
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                prepared_tx :: cache_id(),
                committed_tx :: cache_id(),
                active_txs_per_key :: cache_id()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type, Updates) ->
    case clocksi_readitem_fsm:read_data_item(Node,Key,Type,TxId) of
        {ok, Snapshot} ->
	    Updates2=filter_updates_per_key(Updates, Key),
	    Snapshot2=clocksi_materializer:materialize_eager
			(Type, Snapshot, Updates2),
	    {ok, Snapshot2};
	Other ->
	    Other
    end.


%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    lists:foldl(fun({Node,WriteSet},_Acc) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId,WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).


%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node,WriteSet}], TxId) ->
    riak_core_vnode_master:command(Node,
                                   {single_commit, TxId,WriteSet},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    lists:foldl(fun({Node,WriteSet},_Acc) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    lists:foldl(fun({Node,WriteSet},_Acc) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).


get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTx = open_table(Partition),
    CommittedTx = ets:new(committed_tx,[set]),
    ActiveTxsPerKey = ets:new(active_txs_per_key,[bag]),
    clocksi_readitem_fsm:start_read_servers(Partition),
    {ok, #state{partition=Partition,
                prepared_tx=PreparedTx,
                committed_tx=CommittedTx,
                active_txs_per_key=ActiveTxsPerKey}}.



check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_tables_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.


open_table(Partition) ->
    try
	ets:new(get_cache_name(Partition,prepared),
		[set,protected,named_table,?TABLE_CONCURRENCY])
    catch
	_:_Reason ->
	    %% Someone hasn't finished cleaning up yet
	    open_table(Partition)
    end.

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(get_cache_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};
    
handle_command({check_servers_ready},_Sender,SD0=#state{partition=Partition}) ->
    Node = node(),
    Result = clocksi_readitem_fsm:check_partition_ready(Node,Partition,?READ_CONCURRENCY),
    {reply, Result, SD0};

handle_command({prepare, Transaction, WriteSet}, _Sender,
               State = #state{partition=_Partition,
                              committed_tx=CommittedTx,
                              active_txs_per_key=ActiveTxPerKey,
                              prepared_tx=PreparedTx
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    {Result, NewPrepare} = prepare(Transaction, WriteSet, CommittedTx, ActiveTxPerKey, PreparedTx, PrepareTime),
    case Result of
        {ok, _} ->
            {reply, {prepared, NewPrepare}, State};
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, {error, no_tx_record}, State};
        {error, write_conflict} ->
            {reply, abort, State}
    end;

handle_command({single_commit, Transaction, WriteSet}, _Sender,
               State = #state{partition=_Partition,
                              committed_tx=CommittedTx,
                              active_txs_per_key=ActiveTxPerKey,
                              prepared_tx=PreparedTx
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    {Result,NewPrepare} = prepare(Transaction, WriteSet, CommittedTx, ActiveTxPerKey, PreparedTx, PrepareTime),
    case Result of
        {ok, _} ->
            ResultCommit = commit(Transaction, NewPrepare, WriteSet, PreparedTx, State),
            case ResultCommit of
                {ok, committed} ->
                    {reply, {committed, NewPrepare}, State};
                {error, materializer_failure} ->
                    {reply, {error, materializer_failure}, State};
                {error, timeout} ->
                    {reply, {error, timeout}, State};
                {error, no_updates} ->
                    {reply, no_tx_record, State}
            end;
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, {error, no_tx_record}, State};
        {error, write_conflict} ->
            {reply, abort, State}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, Transaction, TxCommitTime, Updates}, _Sender,
               #state{partition=_Partition,
                      committed_tx=CommittedTx
                      } = State) ->
    Result = commit(Transaction, TxCommitTime, Updates, CommittedTx, State),
    case Result of
        {ok, committed} ->
            {reply, committed, State};
        {error, materializer_failure} ->
            {reply, {error, materializer_failure}, State};
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, Transaction, Updates}, _Sender,
               #state{partition=_Partition} = State) ->
    TxId = Transaction#transaction.txn_id,
    case Updates of
    [{Key, _Type, {_Op, _Actor}} | _Rest] -> 
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            Result = logging_vnode:append(Node,LogId,{TxId, aborted}),
            case Result of
                {ok, _} ->
                    clean_and_notify(TxId, Updates, State);
                {error, timeout} ->
                    clean_and_notify(TxId, Updates, State)
            end,
            {reply, ack_abort, State};
        _ ->
            {reply, {error, no_tx_record}, State}
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_tx=Prepared} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

handle_command({start_read_servers}, _Sender,
               #state{partition=Partition} = State) ->
    clocksi_readitem_fsm:stop_read_servers(Partition),
    clocksi_readitem_fsm:start_read_servers(Partition),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName,Val}).

is_empty(State) ->
    {true,State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{partition=Partition} = _State) ->
    ets:delete(get_cache_name(Partition,prepared)),
    clocksi_readitem_fsm:stop_read_servers(Partition),    
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

prepare(Transaction, TxWriteSet, CommittedTx, ActiveTxPerKey, PreparedTx, PrepareTime)->
    TxId = Transaction#transaction.txn_id,
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
        true ->
            case TxWriteSet of 
                [{Key, Type, {Op, Actor}} | Rest] -> 
		    PrepList = set_prepared(PreparedTx,[{Key, Type, {Op, Actor}} | Rest],TxId,PrepareTime,[]),
		    NewPrepare = now_microsec(erlang:now()),
		    ok = reset_prepared(PreparedTx,[{Key, Type, {Op, Actor}} | Rest],TxId,NewPrepare,PrepList),
		    LogRecord = #log_record{tx_id=TxId,
					    op_type=prepare,
					    op_payload=NewPrepare},
                    LogId = log_utilities:get_logid_from_key(Key),
                    [Node] = log_utilities:get_preflist_from_key(Key),
		    NewUpdates = write_set_to_logrecord(TxId,TxWriteSet),
                    Result = logging_vnode:append_group(Node,LogId,NewUpdates ++ [LogRecord]),
		    {Result, NewPrepare};
		_ ->
		    {error, no_updates}
	    end
	    %% false ->
	    %%     {error, write_conflict}
    end.


set_prepared(_PreparedTx,[],_TxId,_Time,Acc) ->
    lists:reverse(Acc);
set_prepared(PreparedTx,[{Key, _Type, {_Op, _Actor}} | Rest],TxId,Time,Acc) ->
    ActiveTxs = case ets:lookup(PreparedTx, Key) of
		    [] ->
			[];
		    [{Key, List}] ->
			List
		end,
    true = ets:insert(PreparedTx, {Key, [{TxId, Time}|ActiveTxs]}),
    set_prepared(PreparedTx,Rest,TxId,Time,[ActiveTxs|Acc]).

reset_prepared(_PreparedTx,[],_TxId,_Time,[]) ->
    ok;
reset_prepared(PreparedTx,[{Key, _Type, {_Op, _Actor}} | Rest],TxId,Time,[ActiveTxs|Rest2]) ->
    true = ets:insert(PreparedTx, {Key, [{TxId, Time}|ActiveTxs]}),
    reset_prepared(PreparedTx,Rest,TxId,Time,Rest2).


commit(Transaction, TxCommitTime, Updates, _CommittedTx, State)->
    TxId = Transaction#transaction.txn_id,
    DcId = dc_utilities:get_my_dc_id(),
    LogRecord=#log_record{tx_id=TxId,
                          op_type=commit,
                          op_payload={{DcId, TxCommitTime},
                                      Transaction#transaction.vec_snapshot_time}},
    case Updates of
        [{Key, _Type, {_Op, _Param}} | _Rest] -> 
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            case logging_vnode:append(Node,LogId,LogRecord) of
                {ok, _} ->
                    case update_materializer(Updates, Transaction, TxCommitTime) of
                        ok ->
                            ok = clean_and_notify(TxId,Updates,State),
                            {ok, committed};
                        error ->
                            {error, materializer_failure}
                    end;
                {error, timeout} ->
                    {error, timeout}
            end;
        _ -> 
            {error, no_updates}
    end.




%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to:
%%      1. notify all read_fsms that are waiting for this transaction to finish
%%      2. clean the state of the transaction. Namely:
%%      a. ActiteTxsPerKey,
%%      b. PreparedTx
%%
clean_and_notify(TxId, Updates, #state{active_txs_per_key=_ActiveTxsPerKey,
			      prepared_tx=PreparedTx}) ->
    clean_prepared(PreparedTx,Updates,TxId).

clean_prepared(_PreparedTx,[],_TxId) ->
    ok;
clean_prepared(PreparedTx,[{Key, _Type, {_Op, _Actor}} | Rest],TxId) ->
    [{Key,ActiveTxs}] = ets:lookup(PreparedTx, Key),
    NewActive = lists:keydelete(TxId,1,ActiveTxs),
    true = ets:insert(PreparedTx, {Key, NewActive}),
    clean_prepared(PreparedTx,Rest,TxId).



%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_TxId, _H, _CommittedTx, _ActiveTxPerKey) ->
    true.

-spec update_materializer(DownstreamOps :: [{key(),type(),op()}],
                          Transaction::tx(),TxCommitTime:: {term(), term()}) ->
                                 ok | error.
update_materializer(DownstreamOps, Transaction, TxCommitTime) ->
    DcId = dc_utilities:get_my_dc_id(),
    UpdateFunction = fun ({Key, Type, Op}, AccIn) ->
                             CommittedDownstreamOp =
                                 #clocksi_payload{
                                    key = Key,
                                    type = Type,
                                    op_param = Op,
                                    snapshot_time = Transaction#transaction.vec_snapshot_time,
                                    commit_time = {DcId, TxCommitTime},
                                    txid = Transaction#transaction.txn_id},
                             AccIn++[materializer_vnode:update(Key, CommittedDownstreamOp)]
                     end,
    Results = lists:foldl(UpdateFunction, [], DownstreamOps),
    Failures = lists:filter(fun(Elem) -> Elem /= ok end, Results),
    case length(Failures) of
        0 ->
            ok;
        _ ->
            error
    end.

write_set_to_logrecord(TxId, WriteSet) ->
    lists:foldl(fun({Key,Type,Op}, Acc) ->
			Acc ++ [#log_record{tx_id=TxId, op_type=update,
					    op_payload={Key, Type, Op}}]
		end,[],WriteSet).


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
