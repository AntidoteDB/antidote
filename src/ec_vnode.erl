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

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
         reply_coordinator/2,
         read_data_item/4,
         read_data_item/5,
         batch_read/3,
         pre_prepare/4,
         prepare/5,
         commit/3,
         single_commit/5,
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
%%          prepared_tx: a list of prepared tx_ids.
%%          committed_tx: a list of committed tx_ids.
%%          active_txs_per_key: a list of the active tx_ids that
%%              have updated a key (but not yet finished).
%%          downstream_set: a list of the downstream operations that the
%%              tx_ids generate.
%%          write_set: a list of the write sets that the tx_ids
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition,
                write_set}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read_data_item(Node, TxId, Key, Type) ->
    read_data_item(Node, TxId, Key, Type, []).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type, Updates) ->
    try
        riak_core_vnode_master:sync_command(Node,
                                            {read_data_item, TxId, Key, Type, Updates},
                                            ?CLOCKSI_MASTER,
                                            infinity)
    catch
        _:Reason ->
            {error, Reason}
    end.
    
    
%% @doc Sends a batch_read request to each node in ListOfNodes, for a list of keys in the 
%% dictionary Reads.
batch_read(Vnode, TxId, Reads) ->
	riak_core_vnode_master:command(Vnode,
                                   {batch_read, TxId, Reads},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
pre_prepare(ListofNodes, TxId, Updates, TxType) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {pre_prepare, TxId, Updates, TxType},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId, Updates, Coordinator, PrepareTime) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {prepare, TxId, Updates, Coordinator, PrepareTime},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).
%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit(Node, TxId, Updates, Coordinator) ->
    riak_core_vnode_master:command(Node,
                                   {single_commit, TxId, Updates, Coordinator},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {commit, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    riak_core_vnode_master:command(ListofNodes,
                                   {abort, TxId},
                                   {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Initializes all data structures that vnode needs to track information
%%      the tx_ids it participates on.
init([Partition]) ->
    WriteSet = ets:new(list_to_atom(atom_to_list(write_set) ++
                                        integer_to_list(Partition)),
                       [duplicate_bag, {write_concurrency, true}]),
    {ok, #state{partition=Partition,
                write_set=WriteSet}}.

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_data_item, Txn, Key, Type, Updates}, Sender,
               #state{partition=Partition}=State) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = ec_readitem_fsm:start_link(Vnode, Sender, Txn,
                                                 Key, Type, Updates),
    {noreply, State};
    
%% @doc starts a batch_read fsm to handle a batch_read operation.
handle_command({batch_read, TxId, Reads}, Sender,
               State = #state{partition=Partition}) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = ec_batch_read_fsm:start_link(Vnode, Sender, TxId, Reads),
    {noreply, State};    
    

handle_command({pre_prepare, TxId, Updates, TxType}, Sender,
               State = #state{partition=Partition}) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = ec_preprepare_fsm:start_link(Vnode, Sender, TxId, Updates, TxType),
    {noreply, State};

handle_command({single_commit, TxId, Updates, Coordinator}, _Sender,
               State = #state{partition=_Partition,
                              write_set=WriteSet}) ->
    case update_data_item(Updates, TxId, State) of
        ok ->
            Result = prepare(TxId, WriteSet),
            case Result of
                {ok, _} ->
                    ResultCommit = commit(TxId, WriteSet, State),
                    case ResultCommit of
                        {ok, committed} ->
                            reply_coordinator(Coordinator, committed});
                        {error, materializer_failure} ->
                            reply_coordinator(Coordinator, {error, materializer_failure});
                        {error, timeout} ->
                            reply_coordinator(Coordinator, {error, timeout});
                        {error, no_updates} ->
                            reply_coordinator(Coordinator, no_tx_record)
                    end;
                {error, timeout} ->
                    reply_coordinator(Coordinator, {error, timeout});
                {error, no_updates} ->
                    reply_coordinator(Coordinator, {error, no_tx_record});
                {error, write_conflict} ->
                    reply_coordinator(Coordinator, abort)
            end;
        error ->
            reply_coordinator(Coordinator, abort)
    end,
    {noreply, State};
    
handle_command({prepare, TxId, Updates, Coordinator}, _Sender,
               State = #state{partition=_Partition,
                              write_set=WriteSet}) ->
    case update_data_item(Updates, TxId, State) of
        ok ->
            Result = prepare(TxId, WriteSet),
            case Result of
                {ok, _} ->
                    reply_coordinator(Coordinator, prepared);
                {error, timeout} ->
                    reply_coordinator(Coordinator, {error, timeout});
                {error, no_updates} ->
                    reply_coordinator(Coordinator, {error, no_tx_record});
                {error, write_conflict} ->
                    reply_coordinator(Coordinator, abort)
            end;
        error ->
            reply_coordinator(Coordinator, abort)
    end,
    {noreply, State};

%% TODO: sending empty writeset to ec_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxId}, _Sender,
               #state{partition=_Partition,
                      write_set=WriteSet} = State) ->
    Result = commit(TxId, WriteSet, State),
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

handle_command({abort, TxId}, _Sender,
               #state{partition=_Partition, write_set=WriteSet} = State) ->
    Updates = ets:lookup(WriteSet, TxId),
    case Updates of
		[{_, {Key, _Type, {_Op, _Actor}}} | _Rest] -> 
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            Result = logging_vnode:append(Node,LogId,{TxId, aborted}),
            {reply, ack_abort, State};
        _ ->
            {reply, {error, no_tx_record}, State}
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
reply_coordinator(Coordinator, Reply) ->
    riak_core_vnode:reply(Coordinator, Reply).

update_data_item([], _Txn, _State) ->
    ok;

update_data_item([Op|Rest], TxId, State=#state{partition=_Partition,
                      write_set=WriteSet}) ->
    {Key, Type, DownstreamRecord} = Op,
    LogRecord = #log_record{tx_id=TxId, op_type=update, op_payload={Key, Type, DownstreamRecord}},
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    Result = logging_vnode:append(Node,LogId,LogRecord),
    case Result of
        {ok, _} ->
            true = ets:insert(WriteSet, {TxId, {Key, Type, DownstreamRecord}}),
            update_data_item(Rest, Txn, State);
        {error, _Reason} ->
            error
    end.

%% @doc Executes the prepare phase of this partition
prepare(TxId, WriteSet)->
    TxWriteSet = ets:lookup(WriteSet, TxId),
	LogRecord = #log_record{tx_id=TxId,
							op_type=prepare},
	Updates = ets:lookup(WriteSet, TxId),
	case Updates of 
		[{_, {Key, _Type, {_Op, _Actor}}} | _Rest] -> 
			LogId = log_utilities:get_logid_from_key(Key),
			[Node] = log_utilities:get_preflist_from_key(Key),
			logging_vnode:append(Node,LogId,LogRecord);
		_ -> 
			{error, no_updates}
    end.

%% @doc Executes the commit phase of this partition
commit(TxId, WriteSet, State)->
    DcId = dc_utilities:get_my_dc_id(),
    LogRecord=#log_record{tx_id=TxId,
                          op_type=commit,
                          op_payload=DcId},
    Updates = ets:lookup(WriteSet, TxId),
    case Updates of
        [{_, {Key, _Type, {_Op, _Param}}} | _Rest] -> 
            LogId = log_utilities:get_logid_from_key(Key),
            [Node] = log_utilities:get_preflist_from_key(Key),
            case logging_vnode:append(Node,LogId,LogRecord) of
                {ok, _} ->
                    case update_materializer(Updates, TxId) of
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


%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.



-spec update_materializer(DownstreamOps :: [{term(),{key(),type(),op()}}],
                          TxId::tx_id()) ->
                                 ok | error.
update_materializer(DownstreamOps, TxId) ->
    UpdateFunction = fun ({_, {Key, Type, Op}}, AccIn) ->
                             CommittedDownstreamOp =
                                 #ec_payload{
                                    key = Key,
                                    type = Type,
                                    op_param = Op,
                                    tx_id = TxId,
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
