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
-module(ec_vnode).
-behaviour(riak_core_vnode).

-include("ec_antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
    read_data_item/4,
    get_cache_name/2,
    prepare/2,
    commit/2,
    single_commit/1,
    single_commit_sync/1,
    abort/1,
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
%%          prepared_tx: the prepared txn for each key. Note that for
%%              each key, there can be at most one prepared txn in any
%%              time.
%%          committed_tx: the transaction id of the last committed
%%              transaction for each key.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: partition_id(),
    read_servers :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
%%      this does not actually touch the vnode, instead reads directly
%%      from the ets table to allow for concurrency
read_data_item(Node, Key, Type, Updates) ->
    case ec_readitem_fsm:read_data_item(Node, Key, Type) of
        {ok, Snapshot} ->
            lager:info("ec_vnode: got this snapshot from the read fsm ~p",[Snapshot]),
            %% All this should be done at the vnode.
            Updates2 = reverse_and_filter_updates_per_key(Updates, Key),
            lager:info("interactive_coord: about to call mat eager with type ~p, Snapshot ~p and Updates2 ~p",[Type, Snapshot, Updates2]),
            Snapshot2 = ec_materializer:materialize_eager(Type, Snapshot, Updates2),
            lager:info("ec_vnode: returning it"),
            {ok, Snapshot2};
        {error, Reason} ->
            lager:info("ec_vnode: got error"),
            {error, Reason}
    end.

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, CommitTime) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(Node,
            {prepare, CommitTime, WriteSet},
            {fsm, undefined, self()},
            ?EC_MASTER)
                end, ok, ListofNodes).


%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node, WriteSet}]) ->
    riak_core_vnode_master:command(Node,
        {single_commit, WriteSet},
        {fsm, undefined, self()},
        ?EC_MASTER).

single_commit_sync([{Node, WriteSet}]) ->
    riak_core_vnode_master:sync_command(Node,
        {single_commit, WriteSet},
        ?EC_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, CommitTime) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(Node,
            {commit, CommitTime, WriteSet},
            {fsm, undefined, self()},
            ?EC_MASTER)
                end, ok, ListofNodes).

%% @doc Sends an abort request to a Node involved in a tx identified by TxId
abort(ListofNodes) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(Node,
            {abort, WriteSet},
            {fsm, undefined, self()},
            ?EC_MASTER)
                end, ok, ListofNodes).


get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(node()) ++ atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    Num = ec_readitem_fsm:start_read_servers(Partition, ?READ_CONCURRENCY),
    {ok, #state{partition = Partition,
        read_servers = Num}}.


%% @doc The table holding the prepared transactions is shared with concurrent
%%      readers, so they can safely check if a key they are reading is being updated.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node} | Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition, Node},
        {check_tables_ready},
        ?EC_MASTER,
        infinity),
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

loop_until_started(_Partition, 0) ->
    0;
loop_until_started(Partition, Num) ->
    Ret = ec_readitem_fsm:start_read_servers(Partition, Num),
    loop_until_started(Partition, Ret).


handle_command({check_tables_ready}, _Sender, SD0 = #state{partition = Partition}) ->
    Result = case ets:info(get_cache_name(Partition, prepared)) of
                 undefined ->
                     false;
                 _ ->
                     true
             end,
    {reply, Result, SD0};


handle_command({check_servers_ready}, _Sender, SD0 = #state{partition = Partition, read_servers = Serv}) ->
    loop_until_started(Partition, Serv),
    Node = node(),
    Result = ec_readitem_fsm:check_partition_ready(Node, Partition, ?READ_CONCURRENCY),
    {reply, Result, SD0};

handle_command({prepare, CommitTime, WriteSet}, _Sender,
  State = #state{partition = _Partition}) ->
    lager:info("ec_vnode:handle_command_prepare: got this Writeset: ~p and this commit time ~p", [WriteSet, CommitTime]),
    case internal_prepare(WriteSet, CommitTime) of
        {ok, _} ->
            {reply, {prepared}, State};
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, {error, no_updates}, State};
        {error, write_conflict} ->
            {reply, abort, State}
    end;

%% @doc This is the only partition being updated by a transaction,
%%      thus this function performs both the prepare and commit for the
%%      coordinator that sent the request.
handle_command({single_commit, WriteSet}, _Sender,
  State = #state{partition = _Partition}) ->
    lager:info("ec_vnode:handle command single_commit: got this Writeset: ~p ", [WriteSet]),
    CommitTime = now_microsec(erlang:now()),
    Result = internal_prepare(WriteSet, CommitTime),
    case Result of
        {ok, _} ->
            ResultCommit = internal_commit(WriteSet, CommitTime),
            case ResultCommit of
                {ok, committed} ->
                    {reply, {committed, CommitTime}, State};
                {error, materializer_failure} ->
                    {reply, {error, materializer_failure}, State};
                {error, timeout} ->
                    {reply, {error, timeout}, State};
                {error, no_updates} ->
                    {reply, {error, no_updates}, State}
            end;
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, {error, no_updates}, State};
        {error, write_conflict} ->
            {reply, abort, State}
    end;


%% TODO: sending empty writeset to ec_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxCommitTime, Updates}, _Sender,
  #state{partition = _Partition
  } = State) ->
    lager:info("ec_vnode:handle_command_commit: got this commitTime: ~p and this writeSet ~p", [TxCommitTime, Updates]),
    Result = internal_commit(TxCommitTime, Updates),
    case Result of
        {ok, committed} ->
            {reply, committed, State};
        {error, materializer_failure} ->
            {reply, {error, materializer_failure}, State};
        {error, timeout} ->
            {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_updates, State}
    end;

handle_command({abort, Updates}, _Sender,
  #state{partition = _Partition} = State) ->
    case Updates of
        [{Key, _Type, {_Op, _Actor}} | _Rest] ->
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            case logging_vnode:append(Node, LogId, {Updates, aborted}) of
                {ok, _} ->
                    {reply, ack_abort, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        _ ->
            {reply, {error, no_updates}, State}
    end;

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
    term_to_binary({StatName, Val}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{partition = Partition} = _State) ->
    try
        ets:delete(get_cache_name(Partition, prepared))
    catch
        _:Reason ->
            lager:error("Error closing table ~p", [Reason])
    end,
    ec_readitem_fsm:stop_read_servers(Partition, ?READ_CONCURRENCY),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

internal_prepare(TxWriteSet, CommitTime) ->
    lager:info("ec_vnode:internal_prepare: got this Writeset: ~p and this commit time ~p", [TxWriteSet, CommitTime]),
    case TxWriteSet of
        [{Key, _, {_Op, _Actor}} | _] ->
            LogRecord = #log_record{tx_id = undefined,
                op_type = prepare,
                op_payload = CommitTime},
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            logging_vnode:append(Node, LogId, LogRecord);
        _ ->
            {error, no_updates}
    end.

internal_commit(TxCommitTime, Updates) ->
    LogRecord = #log_record{tx_id = undefined,
        op_type = commit,
        op_payload = TxCommitTime},
    case Updates of
        [{Key, _Type, {_Op, _Param}} | _Rest] ->
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            case logging_vnode:append_commit(Node, LogId, LogRecord) of
                {ok, _} ->
                    case update_materializer(Updates, TxCommitTime) of
                        ok ->
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
%%      transaction commits or aborts, it is necessary to clean the 
%%      prepared record of a transaction T. There are three possibility
%%      when trying to clean a record:
%%      1. The record is prepared by T (with T's TxId).
%%          If T is being committed, this is the normal. If T is being 
%%          aborted, it means T successfully prepared here, but got 
%%          aborted somewhere else.
%%          In both cases, we should remove the record.
%%      2. The record is empty.
%%          This can only happen when T is being aborted. What can only
%%          only happen is as follows: when T tried to prepare, someone
%%          else has already prepared, which caused T to abort. Then 
%%          before the partition receives the abort message of T, the
%%          prepared transaction gets processed and the prepared record
%%          is removed.
%%          In this case, we don't need to do anything.
%%      3. The record is prepared by another transaction M.
%%          This can only happen when T is being aborted. We can not
%%          remove M's prepare record, so we should not do anything
%%          either.
%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.


-spec update_materializer(DownstreamOps :: [{key(), type(), op()}], TxCommitTime :: {term(), term()}) ->
    ok.
update_materializer(DownstreamOps, TxCommitTime) ->
    ReversedDownstreamOps = lists:reverse(DownstreamOps),
    UpdateFunction = fun({Key, Type, Op}, AccIn) ->
        CommittedDownstreamOp =
            #ec_payload{
                key = Key,
                type = Type,
                op_param = Op,
                snapshot_time = undefined,
                commit_time = TxCommitTime,
                txid = undefined},
        [ec_materializer_vnode:update(Key, CommittedDownstreamOp) | AccIn]
                     end,
    Results = lists:foldl(UpdateFunction, [], ReversedDownstreamOps),
    Failures = lists:filter(fun(Elem) -> Elem /= ok end, Results),
    case Failures of
        [] ->
            ok;
        _ ->
            error
    end.

%% Internal functions
reverse_and_filter_updates_per_key(Updates, Key) ->
    lists:foldl(fun({KeyPrime, _Type, Op}, Acc) ->
        case KeyPrime == Key of
            true ->
                [Op | Acc];
            false ->
                Acc
        end
                end, [], Updates).

-ifdef(TEST).

%% @doc Testing filter_updates_per_key.
filter_updates_per_key_test() ->
    Op1 = {update, {{increment, 1}, actor1}},
    Op2 = {update, {{increment, 2}, actor1}},
    Op3 = {update, {{increment, 3}, actor1}},
    Op4 = {update, {{increment, 4}, actor1}},

    ClockSIOp1 = {a, crdt_pncounter, Op1},
    ClockSIOp2 = {b, crdt_pncounter, Op2},
    ClockSIOp3 = {c, crdt_pncounter, Op3},
    ClockSIOp4 = {a, crdt_pncounter, Op4},

    ?assertEqual([Op4, Op1],
        reverse_and_filter_updates_per_key([ClockSIOp1, ClockSIOp2, ClockSIOp3, ClockSIOp4], a)).

-endif.
