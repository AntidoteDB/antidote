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
-record(state, {partition,
                prepared_tx,
                committed_tx,
                active_txs_per_key}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type, WriteSet) ->
    perform_read_vnode:read_data_item(Node,TxId,Key,Type,WriteSet).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    lists:foldl(fun({Node,WriteSet},_Acc) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId,WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, 0, ListofNodes).


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
		end, 0, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    lists:foldl(fun({Node,WriteSet},_Acc) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, 0, ListofNodes).


get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTx = ets:new(get_cache_name(Partition,prepared),
			 [set,protected,named_table,?TABLE_CONCURRENCY]),
    CommittedTx = ets:new(committed_tx,[set]),
    ActiveTxsPerKey = ets:new(active_txs_per_key,[bag]),
    {ok, #state{partition=Partition,
                prepared_tx=PreparedTx,
                committed_tx=CommittedTx,
                active_txs_per_key=ActiveTxsPerKey}}.


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
                    clean_and_notify(TxId, Key, State);
                {error, timeout} ->
                    clean_and_notify(TxId, Key, State)
            end,
            {reply, ack_abort, State};
        _ ->
            {reply, {error, no_tx_record}, State}
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_tx=Prepared, partition=_Partition} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

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

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

prepare(Transaction, TxWriteSet, CommittedTx, ActiveTxPerKey, PreparedTx, PrepareTime)->
    %% TODO, not waiting in updates anymore, need to wait here?
    TxId = Transaction#transaction.txn_id,
    case certification_check(TxId, TxWriteSet, CommittedTx, ActiveTxPerKey) of
        true ->
	    %% What is this for????
	    %% lists:foldl(fun({Key1,Type1,_Op}, _Acc) ->
	    %% 			true = ets:insert(ActiveTxPerKey, {Key1, Type1, TxId})
	    %% 		end, 0, TxWriteSet),
            LogRecord = #log_record{tx_id=TxId,
                                    op_type=prepare,
                                    op_payload=PrepareTime},
	    ActiveTxs = case ets:lookup(PreparedTx, active) of
			    [] ->
				[];
			    [{active, List}] ->
				List
			end,
            true = ets:insert(PreparedTx, {active, [{TxId, PrepareTime}|ActiveTxs]}),
	    NewPrepare = now_microsec(erlang:now()),
            true = ets:insert(PreparedTx, {active, [{TxId, NewPrepare}|ActiveTxs]}),
	    Updates = TxWriteSet,
            case Updates of 
                [{Key, _Type, {_Op, _Actor}} | _Rest] -> 
                    LogId = log_utilities:get_logid_from_key(Key),
                    [Node] = log_utilities:get_preflist_from_key(Key),
		    NewUpdates = write_set_to_logrecord(TxId,Updates),
                    Result = logging_vnode:append_group(Node,LogId,NewUpdates ++ [LogRecord]),
		    {Result, NewPrepare};
		_ ->
		    {error, no_updates}
	    end;
	false ->
	     {error, write_conflict}
    end.

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
                    %true = ets:insert(CommittedTx, {TxId, TxCommitTime}),
                    case update_materializer(Updates, Transaction, TxCommitTime) of
                        ok ->
                            clean_and_notify(TxId, Key, State),
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
clean_and_notify(TxId, _Key, #state{active_txs_per_key=_ActiveTxsPerKey,
                                    prepared_tx=PreparedTx}) ->
    [{active,ActiveTxs}] = ets:lookup(PreparedTx, active),
    NewActive = lists:keydelete(TxId,1,ActiveTxs),
    true = ets:insert(PreparedTx, {active, NewActive}).


%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_TxId, _H, _CommittedTx, _ActiveTxPerKey) ->
    true.
%% certification_check(_, [], _, _) ->
%%     true;
%% certification_check(TxId, [H|T], CommittedTx, ActiveTxPerKey) ->
%%     {Key, _Type, _} = H,
%%     TxsPerKey = ets:lookup(ActiveTxPerKey, Key),
%%     case check_keylog(TxId, TxsPerKey, CommittedTx) of
%%         true ->
%%             false;
%%         false ->
%%             certification_check(TxId, T, CommittedTx, ActiveTxPerKey)
%%     end.

%% check_keylog(_, [], _) ->
%%     false;
%% check_keylog(TxId, [H|T], CommittedTx)->
%%     {_Key, _Type, ThisTxId}=H,
%%     case ThisTxId > TxId of
%%         true ->
%%             CommitInfo = ets:lookup(CommittedTx, ThisTxId),
%%             case CommitInfo of
%%                 [{_, _CommitTime}] ->
%%                     true;
%%                 [] ->
%%                     check_keylog(TxId, T, CommittedTx)
%%             end;
%%         false ->
%%             check_keylog(TxId, T, CommittedTx)
%%     end.

-spec update_materializer(DownstreamOps :: [{term(),key(),type(),op()}],
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
