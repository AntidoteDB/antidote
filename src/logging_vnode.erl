%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(logging_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("kernel/include/logger.hrl").


%% Expected time to wait until the inter_dc_log_sender_vnode is started
-define(LOG_SENDER_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         is_sync_log/0,
         set_sync_log/1,
         asyn_read/2,
         get_stable_time/1,
         read/2,
         asyn_append/4,
         append/3,
         append_commit/3,
         append_group/4,
         asyn_append_group/4,
         asyn_read_from/3,
         read_from/3,
         get_up_to_time/5,
         get_from_time/5,
           get_range/6,
           get_all/4,
           request_bucket_op_id/4,
           request_op_id/3]).

-export([init/1,
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
         handle_info/2,
         handle_exit/3,
         handle_overload_command/3,
         handle_overload_info/2]).

-ignore_xref([start_vnode/1]).

-type disklog() :: term(). %Actually: disklog(), which is not exported

-record(state, {partition :: partition_id(),
        logs_map :: dict:dict(log_id(), disklog()),
        enable_log_to_disk :: boolean(), %% this enables or disables logging to disk.
        op_id_table :: cache_id(),  %% Stores the count of ops appended to each log
        recovered_vector :: vectorclock(),  %% This is loaded on start, storing the version vector
                                          %% of the last operation appended to this log, this value
                                            %% is sent to the interdc dependency module, so it knows up to
                                        %% what time updates from other DCs have been received (after crash and restart)
        senders_awaiting_ack :: dict:dict(log_id(), sender()),
        last_read :: term()}).

%% API
-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a `threshold read' asynchronous command to the Logs in
%%      `Preflist' From is the operation id form which the caller wants to
%%      retrieve the operations.  The operations are retrieved in inserted
%%      order and the From operation is also included.
-spec asyn_read_from(preflist(), key(), op_id()) -> ok.
asyn_read_from(Preflist, Log, From) ->
    riak_core_vnode_master:command(Preflist,
                                   {read_from, Log, From},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc synchronous read_from operation
-spec read_from({partition(), node()}, log_id(), op_id()) -> {ok, [term()]} | {error, term()}.
read_from(Node, LogId, From) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read_from, LogId, From},
                                        ?LOGGING_MASTER).

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist'
-spec asyn_read(preflist(), key()) -> ok.
asyn_read(Preflist, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {read, Log},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc Sends a `get_stable_time' synchronous command to `Node'
-spec get_stable_time({partition(), node()}) -> ok.
get_stable_time(Node) ->
    riak_core_vnode_master:command(Node,
                                   {get_stable_time},
                                   ?LOGGING_MASTER).

%% @doc Sends a `read' synchronous command to the Logs in `Node'
-spec read({partition(), node()}, key()) -> {error, term()} | {ok, [term()]}.
read(Node, Log) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Log},
                                        ?LOGGING_MASTER).

%% @doc Sends an `append' asynchronous command to the Logs in `Preflist'
-spec asyn_append(index_node(), key(), log_operation(), sender()) -> ok.
asyn_append(IndexNode, Log, LogOperation, ReplyTo) ->
    riak_core_vnode_master:command(IndexNode,
                                   {append, Log, LogOperation, is_sync_log()},
                                   ReplyTo,
                                   ?LOGGING_MASTER).

%% @doc synchronous append operation payload
-spec append(index_node(), key(), log_operation()) -> {ok, op_id()} | {error, term()}.
append(IndexNode, LogId, LogOperation) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append, LogId, LogOperation, is_sync_log()},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc synchronous append operation payload
%% If enabled in antidote.hrl will ensure item is written to disk
-spec append_commit(index_node(), key(), log_operation()) -> {ok, op_id()} | {error, term()}.
append_commit(IndexNode, LogId, Payload) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append, LogId, Payload, is_sync_log()},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc synchronous append list of log records (note a log record is a payload (log_operation) with an operation number)
%% The IsLocal flag indicates if the operations in the transaction were handled by the local or remote DC.
-spec append_group(index_node(), key(), [log_record()], boolean()) -> {ok, op_id()} | {error, term()}.
append_group(IndexNode, LogId, LogRecordList, IsLocal) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append_group, LogId, LogRecordList, IsLocal, is_sync_log()},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc asynchronous append list of operations
-spec asyn_append_group(index_node(), key(), [log_record()], boolean()) -> ok.
asyn_append_group(IndexNode, LogId, LogRecordList, IsLocal) ->
    riak_core_vnode_master:command(IndexNode,
                                   {append_group, LogId, LogRecordList, IsLocal, is_sync_log()},
                                   ?LOGGING_MASTER,
                                   infinity).

%% @doc given the MaxSnapshotTime and the type, this method fetches from the log the
%% desired operations smaller than the time so a new snapshot can be created.
-spec get_up_to_time(index_node(), key(), vectorclock(), type(), key()) ->
         snapshot_get_response() | {error, reason()}.
get_up_to_time(IndexNode, LogId, MaxSnapshotTime, Type, Key) ->
    riak_core_vnode_master:sync_command(IndexNode,
                    {get, LogId, undefined, MaxSnapshotTime, Type, Key},
                    ?LOGGING_MASTER,
                    infinity).

%% @doc given the MinSnapshotTime and the type, this method fetches from the log the
%% desired operations so a new snapshot can be created.
%% It returns a snapshot_get_response() record which is defined in antidote.hrl
-spec get_from_time(index_node(), key(), vectorclock(), type(), key()) ->
         snapshot_get_response() | {error, reason()}.
get_from_time(IndexNode, LogId, MinSnapshotTime, Type, Key) ->
    riak_core_vnode_master:sync_command(IndexNode,
                    {get, LogId, MinSnapshotTime, undefined, Type, Key},
                    ?LOGGING_MASTER,
                    infinity).

%% @doc given the MinSnapshotTime, MaxSnapshotTime and the type, this method fetches from the log the
%% desired operations so a new snapshot can be created.
%% It returns a #log_get_response{} record which is defined in antidote.hrl
-spec get_range(index_node(), key(), vectorclock(), vectorclock(), type(), key()) ->
         snapshot_get_response() | {error, reason()}.
get_range(IndexNode, LogId, MinSnapshotTime, MaxSnapshotTime, Type, Key) ->
    riak_core_vnode_master:sync_command(IndexNode,
                    {get, LogId, MinSnapshotTime, MaxSnapshotTime, Type, Key},
                    ?LOGGING_MASTER,
                    infinity).


%% @doc Given the logid and position in the log (given by continuation) and a dict
%% of non_committed operations up to this position returns
%% a tuple with three elements
%% the first is a dict with all operations that had been committed until the next chunk in the log
%% the second contains those without commit operations
%% the third is the location of the next chunk
%% Otherwise if the end of the file is reached it returns a tuple
%% where the first element is 'eof' and the second is a dict of committed operations
-spec get_all(index_node(), log_id(), start | disk_log:continuation(), dict:dict(key(), [{non_neg_integer(), clocksi_payload()}])) ->
             {disk_log:continuation(), dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [{non_neg_integer(), clocksi_payload()}])}
             | {error, reason()} | {eof, dict:dict(key(), [{non_neg_integer(), clocksi_payload()}])}.
get_all(IndexNode, LogId, Continuation, PrevOps) ->
    riak_core_vnode_master:sync_command(IndexNode, {get_all, LogId, Continuation, PrevOps},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc Gets the last id of operations stored in the log for the given DCID
-spec request_op_id(index_node(), dcid(), partition()) -> {ok, non_neg_integer()}.
request_op_id(IndexNode, DCID, Partition) ->
    riak_core_vnode_master:sync_command(IndexNode, {get_op_id, DCID, Partition},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc Gets the last id of operations stored in the log for the given bucket from the given DCID
-spec request_bucket_op_id(index_node(), dcid(), bucket(), partition()) -> {ok, non_neg_integer()}.
request_bucket_op_id(IndexNode, DCID, Bucket, Partition) ->
    riak_core_vnode_master:sync_command(IndexNode, {get_op_id, DCID, Bucket, Partition},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc Returns true if synchronous logging is enabled
%%      False otherwise.
%%      Uses environment variable "sync_log" set in antidote.app.src
-spec is_sync_log() -> boolean().
is_sync_log() ->
    dc_meta_data_utilities:get_env_meta_data(sync_log, false).

%% @doc Takes as input a boolean to set whether or not items will
%%      be logged synchronously at this DC (sends a broadcast to update
%%      the environment variable "sync_log" to all nodes).
%%      If true, items will be logged synchronously
%%      If false, items will be logged asynchronously
-spec set_sync_log(boolean()) -> ok.
set_sync_log(Value) ->
    dc_meta_data_utilities:store_env_meta_data(sync_log, Value).

%% @doc Opens the persistent copy of the Log.
%%      The name of the Log in disk is a combination of the the word
%%      `log' and the partition identifier.
init([Partition]) ->
    LogFile = integer_to_list(Partition),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    OpIdTable = create_op_id_table(),
    Preflists = lists:filter(fun(X) -> preflist_member(Partition, X) end, GrossPreflists),
    ?LOG_DEBUG("Opening logs for partition ~w", [Partition]),
    case open_logs(LogFile, Preflists, dict:new(), OpIdTable, vectorclock:new()) of
        {error, Reason} ->
            ?LOG_ERROR("ERROR: opening logs for partition ~w, reason ~w", [Partition, Reason]),
            {error, Reason};
        {Map, MaxVector} ->
            {ok, EnableLoggingToDisk} = application:get_env(antidote, enable_logging),
            {ok, #state{partition=Partition,
                        logs_map=Map,
                        op_id_table=OpIdTable,
                        recovered_vector=MaxVector,
                        senders_awaiting_ack=dict:new(),
                        enable_log_to_disk=EnableLoggingToDisk,
                        last_read=start}}
    end.

%% Used to check if the vnode is up
handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({get_op_id, DCID, Partition}, _Sender, State=#state{op_id_table = OpIdTable}) ->
    OpId = get_op_id(OpIdTable, {[Partition], DCID}),
    #op_number{local = Local, global = _Global} = OpId,
    {reply, {ok, Local}, State};

handle_command({get_op_id, DCID, Bucket, Partition}, _Sender, State=#state{op_id_table = OpIdTable}) ->
    OpId = get_op_id(OpIdTable, {[Partition], Bucket, DCID}),
    #op_number{local = Local, global = _Global} = OpId,
    {reply, {ok, Local}, State};

%% Let the log sender know the last log id that was sent so the receiving DCs
%% don't think they are getting old messages
handle_command({start_timer, undefined}, Sender, State) ->
    handle_command({start_timer, Sender}, Sender, State);
handle_command({start_timer, Sender}, _, State = #state{partition=Partition, op_id_table=OpIdTable, recovered_vector=MaxVector}) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    OpId = get_op_id(OpIdTable, {[Partition], MyDCID}),
    IsReady = try
                  ok = inter_dc_dep_vnode:set_dependency_clock(Partition, MaxVector),
                  ok = inter_dc_log_sender_vnode:update_last_log_id(Partition, OpId),
                  ok = inter_dc_log_sender_vnode:start_timer(Partition),
                  true
              catch
                  _:Reason ->
                      ?LOG_DEBUG("Error updating inter_dc_log_sender_vnode last sent log id: ~w, will retry", [Reason]),
                      false
              end,
    case IsReady of
        true ->
            riak_core_vnode:reply(Sender, ok);
        false ->
            riak_core_vnode:send_command_after(?LOG_SENDER_STARTUP_WAIT, {start_timer, Sender})
    end,
    {noreply, State};

%% @doc Read command: Returns the physical time of the
%%      clocksi vnode for which no transactions will commit with smaller time
%%      Output: {ok, Time}
handle_command({send_min_prepared, Time}, _Sender,
               #state{partition=Partition}=State) ->
    ok = inter_dc_log_sender_vnode:send_stable_time(Partition, Time),
    {noreply, State};

%% @doc Read command: Returns the operations logged for Key
%%          Input: The id of the log to be read
%%      Output: {ok, {vnode_id, Operations}} | {error, Reason}
handle_command({read, LogId}, _Sender,
               #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            %% TODO should continue reading with the continuation??
            ok = disk_log:sync(Log),
            {Continuation, Ops} = read_internal(Log, start, []),
            case Continuation of
                error -> {reply, {error, Ops}, State};
                eof -> {reply, {ok, Ops}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Threshold read command: Returns the operations logged for Key
%%      from a specified op_id-based threshold.
%%
%%      Input:  From: the oldest op_id to return
%%              LogId: Identifies the log to be read
%%      Output: {vnode_id, Operations} | {error, Reason}
%%
handle_command({read_from, LogId, _From}, _Sender,
               #state{partition=Partition, logs_map=Map, last_read=Lastread}=State) ->
    ?STATS(log_read_from),
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            ok = disk_log:sync(Log),
            %% TODO should continue reading with the continuation??
            {Continuation, Ops} =
                case disk_log:chunk(Log, Lastread) of
                    {error, Reason} -> {error, Reason};
                    {C, O} -> {C, O};
                    {C, O, _} -> {C, O};
                    eof -> {eof, []}
                end,
            case Continuation of
                error -> {reply, {error, Ops}, State};
                eof -> {reply, {ok, Ops}, State};
                _ -> {reply, {ok, Ops}, State#state{last_read=Continuation}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Append command: Appends a new op to the Log of Key
%%      Input:  LogId: Identifies which log the operation has to be
%%              appended to.
%%              LogOperation of the operation
%%              OpId: Unique operation id
%%      Output: {ok, {vnode_id, op_id}} | {error, Reason}
%%
%% -spec handle_command({append, log_id(), log_operation(), boolean()}, pid(), #state{}) ->
%%                      {reply, {ok, op_number()} #state{}} | {reply, error(), #state{}}.
handle_command({append, LogId, LogOperation, Sync}, _Sender,
               #state{logs_map=Map,
                      op_id_table=OpIdTable,
                      partition=Partition,
              enable_log_to_disk=EnableLog}=State) ->
    ?STATS(operation_update_internal),
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            MyDCID = dc_utilities:get_my_dc_id(),
            %% all operations update the per log, operation id
            OpId = get_op_id(OpIdTable, {LogId, MyDCID}),
            #op_number{local = Local, global = Global} = OpId,
            NewOpId = OpId#op_number{local =  Local + 1, global = Global + 1},
            true = update_ets_op_id({LogId, MyDCID}, NewOpId, OpIdTable),
            %% non commit operations update the bucket id number to keep track
            %% of the number of updates per bucket
            NewBucketOpId =
            case LogOperation#log_operation.op_type of
                update ->
                    Bucket = (LogOperation#log_operation.log_payload)#update_log_payload.bucket,
                    BOpId = get_op_id(OpIdTable, {LogId, Bucket, MyDCID}),
                    #op_number{local = BLocal, global = BGlobal} = BOpId,
                    NewBOpId = BOpId#op_number{local = BLocal + 1, global = BGlobal + 1},
                    true = update_ets_op_id({LogId, Bucket, MyDCID}, NewBOpId, OpIdTable),
                    NewBOpId;
                _ ->
                    NewOpId
            end,
            LogRecord = #log_record{
              version = log_utilities:log_record_version(),
              op_number = NewOpId,
              bucket_op_number = NewBucketOpId,
              log_operation = LogOperation},
            case insert_log_record(Log, LogId, LogRecord, EnableLog) of
                {ok, NewOpId} ->
                    inter_dc_log_sender_vnode:send(Partition, LogRecord),
                    case Sync of
                    true ->
                        case disk_log:sync(Log) of
                        ok ->
                            {reply, {ok, OpId}, State};
                        {error, Reason} ->
                            {reply, {error, Reason}, State}
                        end;
                    false ->
                        {reply, {ok, OpId}, State}
                    end;
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% Currently this should be only used for external operations
%% That already have their operation id numbers assigned
%% That is why IsLocal is hard coded to false
%% Might want to support appending groups of local operations in the future
%% for efficiency
%% -spec handle_command({append_group, log_id(), [log_record()], false, boolean()}, pid(), #state{}) ->
%%                      {reply, {ok, op_number()} #state{}} | {reply, error(), #state{}}.
handle_command({append_group, LogId, LogRecordList, _IsLocal = false, Sync}, _Sender,
               #state{logs_map=Map,
                      op_id_table=OpIdTable,
                      partition=Partition,
                      enable_log_to_disk=EnableLog}=State) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    {ErrorList, SuccList, UpdatedLogs} =
        lists:foldl(fun(LogRecordOrg, {AccErr, AccSucc, UpdatedLogs}) ->
                        LogRecord = log_utilities:check_log_record_version(LogRecordOrg),
                        case get_log_from_map(Map, Partition, LogId) of
                            {ok, Log} ->
                                %% Generate the new operation ID
                                %% This is only stored in memory to count the total number
                                %% of operations, since the input operations should
                                %% have already been assigned an op id number since
                                %% they are coming from an external DC
                                OpId = get_op_id(OpIdTable, {LogId, MyDCID}),
                                #op_number{local = _Local, global = Global} = OpId,
                                NewOpId = OpId#op_number{global = Global + 1},
                                %% Should assign the opid as follows if this function starts being
                                %% used for operations generated locally
                                %% NewOpId =
                                %%     case IsLocal of
                                %%         true ->
                                %%         OpId#op_number{local =  Local + 1, global = Global + 1};
                                %%         false ->
                                %%         OpId#op_number{global = Global + 1}
                                %%     end,
                                true = update_ets_op_id({LogId, MyDCID}, NewOpId, OpIdTable),
                                LogOperation = LogRecord#log_record.log_operation,
                                case LogOperation#log_operation.op_type of
                                    update ->
                                        Bucket = (LogOperation#log_operation.log_payload)#update_log_payload.bucket,
                                        BOpId = get_op_id(OpIdTable, {LogId, Bucket, MyDCID}),
                                        #op_number{local = _BLocal, global = BGlobal} = BOpId,
                                        NewBOpId = BOpId#op_number{global = BGlobal + 1},
                                        true = update_ets_op_id({LogId, Bucket, MyDCID}, NewBOpId, OpIdTable);
                                    _ ->
                                        true
                                end,
                                ExternalOpNum = LogRecord#log_record.op_number,
                                case insert_log_record(Log, LogId, LogRecord, EnableLog) of
                                    {ok, ExternalOpNum} ->
                                        %% Would need to uncomment this is local ops are sent to this function
                                        %% case IsLocal of
                                        %%     true -> inter_dc_log_sender_vnode:send(Partition, Operation);
                                        %%     false -> ok
                                        %% end,
                                        {AccErr, AccSucc ++ [NewOpId], ordsets:add_element(Log, UpdatedLogs)};
                                    {error, Reason} ->
                                        {AccErr ++ [{reply, {error, Reason}, State}], AccSucc, UpdatedLogs}
                                end;
                            {error, Reason} ->
                                {AccErr ++ [{reply, {error, Reason}, State}], AccSucc, UpdatedLogs}
                        end
                end, {[], [], ordsets:new()}, LogRecordList),
    %% Sync the updated logs if necessary
    case Sync of
        true ->
            ordsets:fold(fun(Log, _Acc) ->
                             ok = disk_log:sync(Log)
                         end, ok, UpdatedLogs);
        false ->
            ok
    end,
    case ErrorList of
        [] ->
            [SuccId|_T] = SuccList,
            {reply, {ok, SuccId}, State};
        [Error|_T] ->
            %%Error
            {reply, Error, State}
    end;

handle_command({get, LogId, MinSnapshotTime, MaxSnapshotTime, Type, Key}, _Sender,
    #state{logs_map = Map, partition = Partition} = State) ->
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            ok = disk_log:sync(Log),
            case get_ops_from_log(Log, {key, Key}, start, MinSnapshotTime, MaxSnapshotTime, dict:new(), dict:new(), load_all) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {eof, CommittedOpsForKeyDict} ->
                    CommittedOpsForKey =
                        case dict:find(Key, CommittedOpsForKeyDict) of
                            {ok, Val} ->
                            Val;
                            error ->
                            []
                        end,
                    {reply, #snapshot_get_response{number_of_ops = length(CommittedOpsForKey), ops_list = CommittedOpsForKey,
                                                   materialized_snapshot = #materialized_snapshot{last_op_id = 0, value = clocksi_materializer:new(Type)},
                                                   snapshot_time = vectorclock:new(), is_newest_snapshot = false},
                     State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% This will reply with all downstream operations that have
%% been stored in the log given by LogId
%% The result is a dict, with a list of ops per key
%% The following spec is only for reference
%% -spec handle_command({get_all, log_id(), disk_log:continuation() | start, dict:dict()}, term(), #state{}) ->
%%                {reply, {error, reason()} | dict:dict(), #state{}}.
handle_command({get_all, LogId, Continuation, Ops}, _Sender,
           #state{logs_map = Map, partition = Partition} = State) ->
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            ok = disk_log:sync(Log),
        case get_ops_from_log(Log, undefined, Continuation, undefined, undefined, Ops, dict:new(), load_per_chunk) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                CommittedOpsForKeyDict ->
                    {reply, CommittedOpsForKeyDict, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

-spec read_internal(log_id(), disk_log:continuation() | start | eof | error, [{non_neg_integer(), clocksi_payload()}]) ->
               {error | eof, [{non_neg_integer(), clocksi_payload()}]}.
read_internal(_Log, error, Ops) ->
    {error, Ops};
read_internal(_Log, eof, Ops) ->
    {eof, Ops};
read_internal(Log, Continuation, Ops) ->
    ?STATS(log_read_read),
    {NewContinuation, NewOps} =
        case disk_log:chunk(Log, Continuation) of
            {C, O} -> {C, O};
            {C, O, _} -> {C, O};
            eof -> {eof, []}
        end,
    read_internal(Log, NewContinuation, Ops ++ NewOps).

-spec reverse_and_add_op_id([clocksi_payload()], non_neg_integer(), [{non_neg_integer(), clocksi_payload()}]) ->
                   [{non_neg_integer(), clocksi_payload()}].
reverse_and_add_op_id([], _Id, Acc) ->
    Acc;
reverse_and_add_op_id([Next|Rest], Id, Acc) ->
    reverse_and_add_op_id(Rest, Id+1, [{Id, Next}|Acc]).

%% Gets the id of the last operation that was put in the log
%% and the maximum vectorclock of the committed transactions stored in the log
-spec get_last_op_from_log(log_id(), disk_log:continuation() | start, cache_id(), vectorclock()) -> {eof, vectorclock()} | {error, term()}.
get_last_op_from_log(Log, Continuation, ClockTable, PrevMaxVector) ->
    ok = disk_log:sync(Log),
    case disk_log:chunk(Log, Continuation) of
        eof ->
            {eof, PrevMaxVector};
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewTerms} ->
            NewMaxVector = get_max_op_numbers(NewTerms, ClockTable, PrevMaxVector),
            get_last_op_from_log(Log, NewContinuation, ClockTable, NewMaxVector);
        {NewContinuation, NewTerms, BadBytes} ->
            case BadBytes > 0 of
                true -> {error, bad_bytes};
                false ->
                NewMaxVector = get_max_op_numbers(NewTerms, ClockTable, PrevMaxVector),
                get_last_op_from_log(Log, NewContinuation, ClockTable, NewMaxVector)
            end
    end.

%% This is called when the vnode starts and loads into the cache
%% the id of the last operation appended to the log, so that new ops will
%% be assigned correct ids (after crash and restart)
-spec get_max_op_numbers([{log_id(), log_record()}], cache_id(), vectorclock()) -> vectorclock().
get_max_op_numbers([], _ClockTable, MaxVector) ->
    MaxVector;
get_max_op_numbers([{LogId, LogRecord}|Rest], ClockTable, PrevMaxVector) ->
    #log_record{op_number = NewOp, bucket_op_number = NewBucketOp, log_operation = LogOperation}
        = log_utilities:check_log_record_version(LogRecord),
    #log_operation{op_type = OpType,
                   log_payload = LogPayload
           } = LogOperation,
    #op_number{node = {_, DCID}} = NewBucketOp,
    NewMaxVector =
        case OpType of
            commit ->
                #commit_log_payload{commit_time = {DCID, TxCommitTime}} = LogPayload,
                vectorclock:set(DCID, TxCommitTime, PrevMaxVector);
            update ->
                %% Update the per bucket opid count
                Bucket = LogPayload#update_log_payload.bucket,
                true = update_ets_op_id({LogId, Bucket, DCID}, NewBucketOp, ClockTable),
                PrevMaxVector;
            _ ->
                PrevMaxVector
        end,
    %% Update the total opid count
    true = update_ets_op_id({LogId, DCID}, NewOp, ClockTable),
    get_max_op_numbers(Rest, ClockTable, NewMaxVector).

%% After appeded an operation to the log, increment the op id
-spec update_ets_op_id({log_id(), dcid()} | {log_id(), bucket(), dcid()}, op_number(), cache_id()) -> true.
update_ets_op_id(Key, NewOp, ClockTable) ->
    #op_number{local = Num, global = GlobalNum} = NewOp,
    case get_op_number(ClockTable, Key) of
        not_found ->
            insert_op_number(ClockTable, Key, NewOp);
        {ok, #op_number{local = OldNum, global = OldGlobal}} ->
            case ((Num > OldNum) or (GlobalNum > OldGlobal)) of
                true ->
                    insert_op_number(ClockTable, Key, NewOp);
                false ->
                    true
            end
    end.

%% @doc This method successively calls disk_log:chunk so all the log is read.
%% With each valid chunk, filter_terms_for_key is called.
-spec get_ops_from_log(log_id(),
               key(),
               disk_log:continuation() | start,
               snapshot_time(),
               snapshot_time(),
                Ops :: dict:dict(txid(), [any_log_payload()]),
               CommittedOpsDict :: dict:dict(key(), [clocksi_payload()]),
               load_all | load_per_chunk) ->
                    {disk_log:continuation(),
                   dict:dict(txid(), [any_log_payload()]),
                   dict:dict(key(), [{non_neg_integer(), clocksi_payload()}])} |
                  {error, reason()} |
                  {eof, dict:dict(key(), [{non_neg_integer(), clocksi_payload()}])}.
get_ops_from_log(Log, Key, Continuation, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict, LoadAll) ->
    case disk_log:chunk(Log, Continuation) of
        eof ->
            {eof, finish_op_load(CommittedOpsDict)};
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewTerms} ->
            {NewOps, NewCommittedOps} = filter_terms_for_key(NewTerms, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict),
        case LoadAll of
        load_all ->
            get_ops_from_log(Log, Key, NewContinuation, MinSnapshotTime, MaxSnapshotTime, NewOps, NewCommittedOps, LoadAll);
        load_per_chunk ->
            {NewContinuation, NewOps, finish_op_load(NewCommittedOps)}
        end;
        {NewContinuation, NewTerms, BadBytes} ->
            case BadBytes > 0 of
                true -> {error, bad_bytes};
                false ->
            {NewOps, NewCommittedOps} = filter_terms_for_key(NewTerms, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict),
            case LoadAll of
            load_all ->
                get_ops_from_log(Log, Key, NewContinuation, MinSnapshotTime, MaxSnapshotTime, NewOps, NewCommittedOps, LoadAll);
            load_per_chunk ->
                {NewContinuation, NewOps, finish_op_load(NewCommittedOps)}
            end
            end
    end.

-spec finish_op_load(dict:dict(key(), [clocksi_payload()])) -> dict:dict(key(), [{non_neg_integer(), clocksi_payload()}]).
finish_op_load(CommittedOpsDict) ->
    dict:fold(fun(Key1, CommittedOps, Acc) ->
                  dict:store(Key1, reverse_and_add_op_id(CommittedOps, 0, []), Acc)
              end, dict:new(), CommittedOpsDict).

%% @doc Given a list of log_records, this method filters the ones corresponding to Key.
%% If key is undefined then is returns all records for all keys
%% It returns a dict corresponding to all the ops matching Key and
%% a list of the committed operations for that key which have a smaller commit time than MinSnapshotTime.
-spec filter_terms_for_key([{non_neg_integer(), log_record()}], key(), snapshot_time(), snapshot_time(),
                dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])) ->
                   {dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])}.
filter_terms_for_key([], _Key, _MinSnapshotTime, _MaxSnapshotTime, Ops, CommittedOpsDict) ->
    {Ops, CommittedOpsDict};
filter_terms_for_key([{_, LogRecord}|T], Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
    #log_record{log_operation = LogOperation} = log_utilities:check_log_record_version(LogRecord),
    #log_operation{tx_id = TxId, op_type = OpType, log_payload = OpPayload} = LogOperation,
    case OpType of
        update ->
            handle_update(TxId, OpPayload, T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict);
        commit ->
            handle_commit(TxId, OpPayload, T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict);
        _ ->
            filter_terms_for_key(T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
    end.

-spec handle_update(txid(), update_log_payload(), [{non_neg_integer(), log_record()}], key(), snapshot_time() | undefined,
             snapshot_time(), dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])) ->
                {dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])}.
handle_update(TxId, OpPayload,  T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
    #update_log_payload{key = Key1} = OpPayload,
    case (Key == {key, Key1}) or (Key == undefined) of
        true ->
            filter_terms_for_key(T, Key, MinSnapshotTime, MaxSnapshotTime,
                dict:append(TxId, OpPayload, Ops), CommittedOpsDict);
        false ->
            filter_terms_for_key(T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
    end.

-spec handle_commit(txid(), commit_log_payload(), [{non_neg_integer(), log_record()}], key(), snapshot_time() | undefined,
             snapshot_time(), dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])) ->
                {dict:dict(txid(), [any_log_payload()]), dict:dict(key(), [clocksi_payload()])}.
handle_commit(TxId, OpPayload, T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict) ->
    #commit_log_payload{commit_time = {DcId, TxCommitTime}, snapshot_time = SnapshotTime} = OpPayload,
    case dict:find(TxId, Ops) of
        {ok, OpsList} ->
        NewCommittedOpsDict =
        lists:foldl(fun(#update_log_payload{key = KeyInternal, type = Type, op = Op}, Acc) ->
                    case (check_min_time(SnapshotTime, MinSnapshotTime) andalso
                      check_max_time(SnapshotTime, MaxSnapshotTime)) of
                    true ->
                        CommittedDownstreamOp =
                        #clocksi_payload{
                           key = KeyInternal,
                           type = Type,
                           op_param = Op,
                           snapshot_time = SnapshotTime,
                           commit_time = {DcId, TxCommitTime},
                           txid = TxId},
                        dict:append(KeyInternal, CommittedDownstreamOp, Acc);
                    false ->
                        Acc
                    end
                end, CommittedOpsDict, OpsList),
        filter_terms_for_key(T, Key, MinSnapshotTime, MaxSnapshotTime, dict:erase(TxId, Ops),
                 NewCommittedOpsDict);
    error ->
        filter_terms_for_key(T, Key, MinSnapshotTime, MaxSnapshotTime, Ops, CommittedOpsDict)
    end.

check_min_time(SnapshotTime, MinSnapshotTime) ->
    ((MinSnapshotTime == undefined) orelse (vectorclock:ge(SnapshotTime, MinSnapshotTime))).

check_max_time(SnapshotTime, MaxSnapshotTime) ->
    ((MaxSnapshotTime == undefined) orelse (vectorclock:le(SnapshotTime, MaxSnapshotTime))).

handle_handoff_command(?FOLD_REQ{foldfun = FoldFun, acc0 = OldHandoffState}, _Sender,
                       #state{logs_map = Map, partition = Partition} = State) ->
    ?LOG_DEBUG("Fold request for partition ~p", [Partition]),

    %% VisitElement is called for each element in this vnode's state
    %% here, just encode a {key, record} pair
    %% FoldFun will call encode_handoff_item, the arguments should match exactly + 1
    %% (the Acc argument is handled by riak_core)
    VisitElement = fun({Key, LogRecord}, HandoffState) -> FoldFun(Key, LogRecord, HandoffState) end,

    NewHandoffState = join_logs(dict:to_list(Map), VisitElement, OldHandoffState),
    {reply, NewHandoffState, State};


%% a vnode in the handoff livecycle stage will not accept handle_commands anymore
%% instead every command is redirected to the handle_handoff_command implementations
%% for simplicity, we ignore every command except the fold handoff itself
%% for extra availability, every handle_command needs to also be implemented as a handle_handoff_command
handle_handoff_command(Command, _Sender, State) ->
    ?LOG_INFO("Ignoring command in handoff lifecycle: ~p", [Command]),
    {noreply, State}.

encode_handoff_item(Key, LogRecord) ->
    term_to_binary({Key, LogRecord}).

handoff_starting(TargetNode, State=#state{partition = Partition}) ->
    ?LOG_DEBUG("Handoff starting ~p: ~p", [Partition, TargetNode]),
    {true, State}.

handoff_cancelled(State=#state{partition = Partition}) ->
    ?LOG_DEBUG("Handoff cancelled: ~p", [Partition]),
    {ok, State}.

handoff_finished(TargetNode, State=#state{partition = Partition}) ->
    ?LOG_INFO("Handoff finished ~p: ~p", [Partition, TargetNode]),
    {ok, State}.

handle_handoff_data(Data, #state{partition = Partition, logs_map = Map, enable_log_to_disk = EnableLog} = State) ->
    {LogId, LogRecord} = binary_to_term(Data),
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            %% Optimistic handling; crash otherwise.
            {ok, _OpId} = insert_log_record(Log, LogId, LogRecord, EnableLog),
            ok = disk_log:sync(Log),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.


is_empty(State = #state{logs_map=Map}) ->
    LogIds = dict:fetch_keys(Map),
    case no_elements(LogIds, Map) of
        true ->
            {true, State};
        false ->
            {false, State}
    end.

delete(State = #state{logs_map = _Map, partition = Partition}) ->
    ?LOG_INFO("Deleting partition ~p", [Partition]),
    %% TODO this only works without replication (e.g. N = 1)
    %% re-implement this and iterate over logs_map to delete all logs belonging to this note,
    %% not only the primary log
    %% the format of the primary log is primary_preflist--primary_preflist.LOG
    LogId = integer_to_list(Partition) ++ "--" ++ integer_to_list(Partition),
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogPath = filename:join(DataDir, LogId),
    %% best effort delete
    _ = file:delete(LogPath ++ ".LOG"),
    ?STATS({log_reset, LogPath}),
    {ok, State}.

handle_info({sync, Log, LogId},
            #state{senders_awaiting_ack=SendersAwaitingAck0}=State) ->
    case dict:find(LogId, SendersAwaitingAck0) of
        {ok, Senders} ->
            _ = case dets:sync(Log) of
                ok ->
                    [riak_core_vnode:reply(Sender, {ok, OpId}) || {Sender, OpId} <- Senders];
                {error, Reason} ->
                    [riak_core_vnode:reply(Sender, {error, Reason}) || {Sender, _OpId} <- Senders]
            end,
            ok;
        _ ->
            ok
    end,
    SendersAwaitingAck = dict:erase(LogId, SendersAwaitingAck0),
    {ok, State#state{senders_awaiting_ack=SendersAwaitingAck}}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
    ok.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================%%
%% Internal Functions %%
%%====================%%

%% @doc no_elements: checks whether any of the logs contains any data
%%      Input:  LogIds: Each logId is a preflist that represents one log
%%              Map: the dictionary that relates the preflist with the
%%              actual log
%%      Return: true if all logs are empty. false if at least one log
%%              contains data.
%%
-spec no_elements([log_id()], dict:dict(log_id(), disklog())) -> boolean().
no_elements([], _Map) ->
    true;
no_elements([LogId|Rest], Map) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
            case disk_log:chunk(Log, start) of
                eof ->
                    no_elements(Rest, Map);
                _ ->
                    false
            end;
        error ->
            {error, no_log_for_preflist}
    end.

%% @doc open_logs: open one log per partition in which the vnode is primary
%%      Input:  LogFile: Partition concat with the atom log
%%                      Preflists: A list with the preflist in which
%%                                 the vnode is involved
%%                      Initial: Initial log identifier. Non negative
%%                               integer. Consecutive ids for the logs.
%%                      Map: The ongoing map of preflist->log. dict:dict()
%%                           type.
%%      Return:         LogsMap: Maps the  preflist and actual name of
%%                               the log in the system. dict:dict() type.
%%                      MaxVector: The version vector time of the last
%%                               operation appended to the logs
-spec open_logs(string(), [preflist()], dict:dict(log_id(), disklog()), cache_id(), vectorclock()) -> {dict:dict(log_id(), disklog()), vectorclock()} | {error, reason()}.
open_logs(_LogFile, [], Map, _ClockTable, MaxVector) ->
    {Map, MaxVector};
open_logs(LogFile, [Next|Rest], Map, ClockTable, MaxVector)->
    PartitionList = log_utilities:remove_node_from_preflist(Next),
    PreflistString = string:join(
                       lists:map(fun erlang:integer_to_list/1, PartitionList), "-"),
    LogId = LogFile ++ "--" ++ PreflistString,
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogPath = filename:join(DataDir, LogId),
    ?STATS({log_append, LogPath, filelib:file_size(LogPath ++ ".LOG")}),
    case disk_log:open([{name, LogPath}]) of
        {ok, Log} ->
            {eof, NewMaxVector} = get_last_op_from_log(Log, start, ClockTable, MaxVector),
            ?LOG_DEBUG("Opened log ~p, last op ids are ~p, max vector is ~p", [Log, get_op_numbers(ClockTable), vectorclock:to_list(NewMaxVector)]),
            Map2 = dict:store(PartitionList, Log, Map),
            open_logs(LogFile, Rest, Map2, ClockTable, MaxVector);
        {repaired, Log, _, _} ->
            {eof, NewMaxVector} = get_last_op_from_log(Log, start, ClockTable, MaxVector),
            ?LOG_DEBUG("Repaired log ~p, last op ids are ~p, max vector is ~p", [Log, get_op_numbers(ClockTable), vectorclock:to_list(NewMaxVector)]),
            Map2 = dict:store(PartitionList, Log, Map),
            open_logs(LogFile, Rest, Map2, ClockTable, NewMaxVector);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc get_log_from_map: abstracts the get function of a key-value store
%%              currently using dict
%%      Input:  Map:  dict that represents the map
%%              LogId:  identifies the log.
%%      Return: The actual name of the log
%%
-spec get_log_from_map(dict:dict(log_id(), disklog()), partition(), log_id()) ->
                              {ok, log()} | {error, no_log_for_preflist}.
get_log_from_map(Map, _Partition, LogId) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
           {ok, Log};
        error ->
            {error, no_log_for_preflist}
    end.

%% @doc join_logs: Recursive fold of all the logs stored in the vnode
%%      Input:  Logs: A list of pairs {Preflist, Log}
%%                      F: Function to apply when folding the log (dets)
%%                      Acc: Folded data
%%      Return: Folded data of all the logs.
%%
-spec join_logs([{preflist(), log()}], fun(), term()) -> term().
join_logs([], _F, Acc) ->
    Acc;
join_logs([{_Preflist, Log}|T], F, Acc) ->
    JointAcc = fold_log(Log, start, F, Acc),
    join_logs(T, F, JointAcc).

fold_log(Log, Continuation, F, Acc) ->
    case  disk_log:chunk(Log, Continuation) of
        eof ->
            Acc;
        {Next, Ops} ->
            NewAcc = lists:foldl(F, Acc, Ops),
            fold_log(Log, Next, F, NewAcc)
    end.


%% @doc insert_log_record: Inserts an operation into the log only if the
%%      OpId is not already in the log
%%      Input:
%%          Log: The identifier log the log where the operation will be
%%               inserted
%%          LogId: Log identifier to which the operation belongs.
%%          OpId: Id of the operation to insert
%%          Payload: The payload of the operation to insert
%%      Return: {ok, OpId} | {error, Reason}
%%
-spec insert_log_record(log(), log_id(), log_record(), boolean()) -> {ok, op_number()} | {error, reason()}.
insert_log_record(Log, LogId, LogRecord, EnableLogging) ->
    Result = case EnableLogging of
                 true ->
                     BinaryRecord = term_to_binary({LogId, LogRecord}),
                     ?STATS({log_append, Log, erlang:byte_size(BinaryRecord)}),
                     ?LOG_DEBUG("Appending ~p bytes", [erlang:byte_size(BinaryRecord)]),
                     disk_log:blog(Log, term_to_binary({LogId, LogRecord}));
                 false ->
                     ok
             end,
    case Result of
        ok ->
            {ok, LogRecord#log_record.op_number};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc preflist_member: Returns true if the Partition identifier is
%%              part of the Preflist
%%      Input:  Partition: The partition identifier to check
%%              Preflist: A list of pairs {Partition, Node}
%%      Return: true | false
%%
-spec preflist_member(partition(), preflist()) -> boolean().
preflist_member(Partition, Preflist) ->
    lists:any(fun({P, _}) -> P =:= Partition end, Preflist).

-spec get_op_id(cache_id(), {log_id(), dcid()} | {log_id(), bucket(), dcid()}) -> op_number().
get_op_id(ClockTable, Key = {_, DCID}) ->
    case get_op_number(ClockTable, Key) of
        not_found ->
            #op_number{node = {node(), DCID}, global = 0, local = 0};
        {ok, Val} ->
            Val
    end;
get_op_id(ClockTable, Key = {_, _, DCID}) ->
    case get_op_number(ClockTable, Key) of
        not_found ->
            #op_number{node = {node(), DCID}, global = 0, local = 0};
        {ok, Val} ->
            Val
    end.

%%%===================================================================
%%%  Ets tables
%%%
%%%  op_id_table: Stores the count of ops appended to each log
%%%===================================================================

-spec create_op_id_table() -> ets:tab().
create_op_id_table() ->
    ets:new(op_id_table, [set]).

-spec get_op_number(cache_id(), {log_id(), dcid()} | {log_id(), bucket(), dcid()}) -> not_found | {ok, op_number()}.
get_op_number(ClockTable, Key) ->
    case ets:lookup(ClockTable, Key) of
        [] ->
            not_found;
        [{Key, Val}] ->
            {ok, Val}
    end.

-spec get_op_numbers(cache_id()) -> [{log_id(), dcid(), op_number()} | {log_id(), bucket(), dcid(), op_number()}].
get_op_numbers(ClockTable) ->
    ets:tab2list(ClockTable).

-spec insert_op_number(cache_id(), {log_id(), dcid()} | {log_id(), bucket(), dcid()}, op_number()) -> true.
insert_op_number(ClockTable, Key, NewOp) ->
    ets:insert(ClockTable, {Key, NewOp}).


-ifdef(TEST).

%% Testing get_log_from_map works in both situations, when the key
%% is in the map and when the key is not in the map
get_log_from_map_test() ->
    Dict = dict:new(),
    Dict2 = dict:store([antidote1, c], value1, Dict),
    Dict3 = dict:store([antidote2, c], value2, Dict2),
    Dict4 = dict:store([antidote3, c], value3, Dict3),
    Dict5 = dict:store([antidote4, c], value4, Dict4),
    ?assertEqual({ok, value3}, get_log_from_map(Dict5, undefined,
            [antidote3, c])),
    ?assertEqual({error, no_log_for_preflist}, get_log_from_map(Dict5,
            undefined, [antidote5, c])).

%% Testing that preflist_member returns true when there is a
%% match.
preflist_member_true_test() ->
    Preflist = [{partition1, node}, {partition2, node}, {partition3, node}],
    ?assertEqual(true, preflist_member(partition1, Preflist)).

%% Testing that preflist_member returns false when there is no
%% match.
preflist_member_false_test() ->
    Preflist = [{partition1, node}, {partition2, node}, {partition3, node}],
    ?assertEqual(false, preflist_member(partition5, Preflist)).

-endif.
