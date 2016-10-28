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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 3).
%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 50).
%% If after the op GC there are only this many or less spaces
%% free in the op list then increase the list size
-define(RESIZE_THRESHOLD, 5).
%% Only store the new SS if the following number of ops
%% were applied to the previous SS
-define(MIN_OP_STORE_SS, 1).
%% Expected time to wait until the logging vnode is up
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
	tuple_to_key/2,
	check_tables_ready/0,
	read/4,
	get_cache_name/2,
	store_ss/4,
	update/3,
	op_not_already_in_snapshot/2,
	create_empty_materialized_snapshot_record/2,
	log_number_of_non_applied_ops/2,
	merge_staleness_files_into_table/0]).

%% Callbacks
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
         handle_exit/3]).

-type op_and_id() :: {non_neg_integer(),operation_payload()}.

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), transaction(), mat_state()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, Transaction, MatState = #mat_state{ops_cache = OpsCache}) ->
    case ets:info(OpsCache) of
	undefined ->
	    riak_core_vnode_master:sync_command({MatState#mat_state.partition,node()},
						{read,Key,Type,Transaction},
						materializer_vnode_master,
						infinity);
	_ ->
	    internal_read(Key, Type, Transaction, MatState)
    end.

-spec get_cache_name(non_neg_integer(),atom()) -> atom().
get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), operation_payload(), transaction()) -> ok | {error, reason()}.
update(Key, DownstreamOp, Transaction) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp, Transaction},
        materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), #materialized_snapshot{}, snapshot_time(), boolean()) -> ok.
store_ss(Key, Snapshot, Params, ShouldGC) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command(IndexNode, {store_ss,Key, Snapshot, Params, ShouldGC},
        materializer_vnode_master).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    IsReady = case application:get_env(antidote,recover_from_log) of
		  {ok, true} ->
		      lager:info("Checking for logs to init materializer ~p", [Partition]),
		      riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
		      false;
		  _ ->
		      true
	      end,
	%% the following log is used in benchmarks to measure how old returned snapshots are.
	StalenessLog=case application:get_env(antidote, log_staleness) of
		{ok, true}->
			case open_staleness_log(Partition) of
				{ok, LogName} ->
					lager:info("Opened staleness log ~p", [LogName]),
					LogName;
				{error, Reason} ->
					lager:error("Failed to open log for partition ~p. ~n Error was ~p", [Partition, Reason]),
					staleness_log_disabled
			end;
		_->
			staleness_log_disabled
	end,
    {ok, #mat_state{is_ready = IsReady, partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache, staleness_log=StalenessLog}}.


%% opens the log file for staleness.
%% this happens if the environment variable log_staleness
%% is set to true in antidote.app.src
-spec open_staleness_log(non_neg_integer() | ets:tid()) -> {'error',reason()} | {'ok', string()}.
open_staleness_log(Partition) ->
	LogFile = "StalenessLog-" ++ integer_to_list(Partition) ++ "-" ++ integer_to_list(dc_utilities:now_microsec()),
	LogPath=filename:join(
		app_helper:get_env(riak_core, platform_data_dir), LogFile),
	case disk_log:open([{name, LogPath}]) of
		{ok, Log}->
			{ok, Log};
		{error, Reason}->
			{error, Reason}
	end.

%% @doc This function is used only for benchmark purposes.
%% It logs on disk the number of operations and snapshots
%% that it had to leave out to build the transaction's snapshot.
%% this is used to measure staleness.
-spec log_number_of_non_applied_ops(mat_state(), non_neg_integer())-> ok | {error, {no_such_log, atom()}}.
log_number_of_non_applied_ops(MatState = #mat_state{staleness_log=StalenessLog}, NumberOfNonAppliedOps)->
	case StalenessLog of
		staleness_log_disabled->
			ok;
		undefined->
			%% asynchronously send message to the right vnode.
			riak_core_vnode_master:command({MatState#mat_state.partition, node()},
				{log_number_of_non_applied_snapshot_and_ops, NumberOfNonAppliedOps},
				materializer_vnode_master);
		_->
			log_number_of_non_applied_snapshot_and_ops_internal(StalenessLog, NumberOfNonAppliedOps)
	end.

-spec log_number_of_non_applied_snapshot_and_ops_internal(atom(), non_neg_integer())-> ok | {error, {no_such_log, atom()}}.
log_number_of_non_applied_snapshot_and_ops_internal(StalenessLog, NumberOfNonAppliedOps)->
	case disk_log:alog(StalenessLog, NumberOfNonAppliedOps) of
		ok->
			ok;
		{error, no_such_log}->
			{error, {no_such_log, StalenessLog}}
	end.

-spec merge_staleness_files_into_table() -> atom() | {error, reason()}.
merge_staleness_files_into_table() ->
	case file:list_dir("./data/") of
		{error, Reason} ->
			{error, Reason};
		{ok, FileList} ->
			lager:info("got file list: ~p",[FileList]),
			StalenessTable = ets:new(staleness_table, [duplicate_bag, named_table, public]),
			lists:foreach(fun(FileName) ->
				ok = file_to_table(FileName, start, StalenessTable)
			end, FileList),
			StalenessTable
	end.
	

-spec file_to_table(string(), start | disk_log:continuation(), atom()) -> ok.
file_to_table(FileName, Continuation, TableName)->
	%% add the directory and remove the .LOG extension.
	DirAndFileName = "./data/" ++ lists:sublist(FileName, length(FileName)-4),
	lager:info("merging file ~p from Continuation ~p ", [DirAndFileName, Continuation]),
	case (lists:sublist(FileName, 1, length("StalenessLog")) == "StalenessLog") of
		true -> %% this is a staleness log file, process it
			case Continuation of
				start ->
					lager:info("openning file"),
					disk_log:open([{name, DirAndFileName}]);
				_->
					do_nothing
			end,
			case disk_log:chunk(DirAndFileName, Continuation) of
				eof ->
					lager:info("closing file"),
					disk_log:close(DirAndFileName),
					ok;
				{NextContinuation, List} ->
					lists:foreach(fun(Info) ->
						lager:info("inserting into table: ~p", [{Info, 1}]),
						ets:insert(TableName, {Info,1})
					end, List),
					file_to_table(DirAndFileName, NextContinuation, TableName)
			end;
		false -> %% nothing to do, not a staleness file.
			ok
	end.


-spec load_from_log_to_tables(partition_id(), mat_state()) -> ok | {error, reason()}.
load_from_log_to_tables(Partition, State) ->
    LogId = [Partition],
    Node = {Partition, log_utilities:get_my_node(Partition)},
    loop_until_loaded(Node, LogId, start, dict:new(), State).

-spec loop_until_loaded({partition_id(), node()}, log_id(), start | disk_log:continuation(), dict:dict(), mat_state()) -> ok | {error, reason()}.
loop_until_loaded(Node, LogId, Continuation, Ops, State) ->
    case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
	{error, Reason} ->
	    {error, Reason};
	{NewContinuation, NewOps, OpsDict} ->
	    load_ops(OpsDict, State),
	    loop_until_loaded(Node, LogId, NewContinuation, NewOps, State);
	{eof, OpsDict} ->
	    load_ops(OpsDict, State),
	    ok
    end.

-spec load_ops(dict:dict(), mat_state()) -> true.
load_ops(OpsDict, State) ->
    dict:fold(fun(Key, CommittedOps, _Acc) ->
		      lists:foreach(fun({_OpId,Op}) ->
					    #operation_payload{key = Key} = Op,
					    op_insert_gc(Key, Op, State, no_txn_inserting_from_log)
				    end, CommittedOps)
	      end, true, OpsDict).

-spec open_table(partition_id(), 'ops_cache' | 'snapshot_cache') -> atom() | ets:tid().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
	undefined ->
	    ets:new(get_cache_name(Partition, Name),
		    [set, protected, named_table, ?TABLE_CONCURRENCY]);
	_ ->
	    %% Other vnode hasn't finished closing tables
	    lager:debug("Unable to open ets table in materializer vnode, retrying"),
	    timer:sleep(100),
	    try
		ets:delete(get_cache_name(Partition, Name))
	    catch
		_:_Reason->
		    ok
	    end,
	    open_table(Partition, Name)
    end.

%% @doc The tables holding the updates and snapshots are shared with concurrent
%%      readers, allowing them to be non-blocking and concurrent.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_table_ready(PartitionList).

-spec check_table_ready([{partition_id(),node()}]) -> boolean().
check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result =
	try
	    riak_core_vnode_master:sync_command({Partition,Node},
						{check_ready},
						materializer_vnode_master,
						infinity)
	catch
	    _:_Reason ->
		false
	end,
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({check_ready},_Sender,State = #mat_state{partition=Partition, is_ready=IsReady}) ->
    Result = case ets:info(get_cache_name(Partition,ops_cache)) of
		 undefined ->
		     false;
		 _ ->
		     case ets:info(get_cache_name(Partition,snapshot_cache)) of
			 undefined ->
			     false;
			 _ ->
			     true
		     end
	     end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, Transaction}, _Sender, State) ->
    {reply, read(Key, Type, Transaction, State), State};

handle_command({log_number_of_non_applied_snapshot_and_ops, NumberOfNonAppliedOps}, _Sender, MatState) ->
	{reply, log_number_of_non_applied_ops(MatState, NumberOfNonAppliedOps), MatState};

handle_command({update, Key, DownstreamOp, Transaction}, _Sender, State) ->
%%	lager:info("~nInserting this downstreamop : ~n~p", [DownstreamOp]),
    case op_insert_gc(Key,DownstreamOp, State, Transaction) of
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command({store_ss, Key, Snapshot, Params, ShouldGC}, _Sender, State) ->
    internal_store_ss(Key,Snapshot,Params,ShouldGC,State),
    {noreply, State};

handle_command(load_from_log, _Sender, State=#mat_state{partition=Partition})->
	IsReady=try
		case load_from_log_to_tables(Partition, State) of
			ok->
				lager:info("Finished loading from log to materializer on partition ~w", [Partition]),
				true;
			{error, not_ready}->
				false;
			{error, Reason}->
				lager:error("Unable to load logs from disk: ~w, continuing", [Reason]),
				true
		end
	catch
		_:Reason1->
			lager:debug("Error loading from log ~w, will retry", [Reason1]),
			false
	end,
	ok=case IsReady of
		false->
			riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
			ok;
		true->
			ok
	end,
	{noreply, State#mat_state{is_ready=IsReady}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
                       _Sender,
                       State = #mat_state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
		[Key1|_] = tuple_to_list(Key),
                Fun(Key1, Key, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#mat_state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#mat_state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#mat_state{ops_cache=_OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#mat_state{ops_cache=OpsCache,snapshot_cache=SnapshotCache}) ->
    try
	ets:delete(OpsCache),
	ets:delete(SnapshotCache)
    catch
	_:_Reason->
	    ok
    end,
    ok.


%%---------------- Internal Functions -------------------%%

-spec internal_store_ss(key(), #materialized_snapshot{}, snapshot_time(), boolean(), mat_state()) -> boolean().
internal_store_ss(Key, Snapshot = #materialized_snapshot{last_op_id = NewOpId}, SnapshotParams, ShouldGc,
  State = #mat_state{snapshot_cache = SnapshotCache}) ->
	
	SnapshotDict = case ets:lookup(SnapshotCache, Key) of
									 [] ->
										 vector_orddict:new();
									 [{_, SnapshotDictA}] ->
										 SnapshotDictA
								 end,
%%	lager:debug("This is the current snapshotDict: ~p",[SnapshotDict]),
	%% Check if this snapshot is newer than the ones already in the cache. Since reads are concurrent multiple
	%% insert requests for the same snapshot could have occured
	ShouldInsert =
		case vector_orddict:size(SnapshotDict) > 0 of
			true ->
				{_Vector, #materialized_snapshot{last_op_id = OldOpId}} = vector_orddict:first(SnapshotDict),
				((NewOpId - OldOpId) >= ?MIN_OP_STORE_SS);
			false -> true
		end,
%%	lager:info("~nShouldGc ~p", [ShouldGc]),
	case (ShouldInsert or ShouldGc) of
		true ->
%%			lager:info("Inserting this snapshot: ~n~p ~nParams: ~p ~n SnapshotDict ~n~p", [Snapshot, SnapshotParams, SnapshotDict]),
			SnapshotDict1 = vector_orddict:insert_bigger(SnapshotParams, Snapshot, SnapshotDict),
%%			lager:info("~nSnapshotDict after ~n~p", [SnapshotDict1]),
%%			lager:info("~nShouldGc ~p", [ShouldGc]),
			snapshot_insert_gc(Key, SnapshotDict1, ShouldGc, State);
		false ->
%%			lager:info("~n~nShould NOT INSERT~n~n"),
			false
	end.

-spec internal_read(key(), type(), transaction(), mat_state()) -> {ok, {snapshot(), any()}}| {error, no_snapshot}.
internal_read(Key, Type, Transaction, MatState) ->
    internal_read(Key, Type, Transaction, MatState,false).
internal_read(Key, Type, Transaction, MatState, ShouldGc) ->
%%	lager:debug("called : ~p",[Transaction]),
	OpsCache = MatState#mat_state.ops_cache,
	SnapshotCache = MatState#mat_state.snapshot_cache,
    TxnId = Transaction#transaction.txn_id,
    Protocol = Transaction#transaction.transactional_protocol,
    case ets:lookup(OpsCache, Key) of
        [] ->
%%	        lager:debug("Cache for Key ~p is empty",[Key]),
	        {NewMaterializedSnapshotRecord, SnapshotCommitParams} = create_empty_materialized_snapshot_record(Transaction, Type),
	        NewSnapshot = NewMaterializedSnapshotRecord#materialized_snapshot.value,
%%	        lager:debug("internal read returning: ",[{ok, {NewSnapshot, SnapshotCommitParams}}]),
            {ok, {NewSnapshot, SnapshotCommitParams}};
        [Tuple] ->
	        {Key, Len, _OpId, _ListLen, OperationsForKey} = tuple_to_key(Tuple, false),
%%	        lager:debug("Key ~p~n, Len ~p~n, _OpId ~p~n, _ListLen ~p~n, OperationsForKey ~p~n", [Key, Len, _OpId, _ListLen, OperationsForKey]),
	        UpdatedTxnRecord =
		        case Protocol of
			        physics ->
				        case TxnId of no_txn_inserting_from_log -> Transaction;
					        _ ->
%%						        case define_snapshot_vc_for_transaction(Transaction, OperationsForKey) of
%%							        OpCommitParams = {OperationCommitVC, _OperationDependencyVC, _ReadVC} ->
%%								        {Transaction#transaction{snapshot_vc = OperationCommitVC}, OpCommitParams};
%%							        no_operation_to_define_snapshot ->
%%%%								        lager:debug("there no_operation_to_define_snapshot"),
						        NewVC=vectorclock:new(),
						        DepUpbound=Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
						        NewSnapshotVC=case DepUpbound==NewVC of
							        true->
								        _NowVC=vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), dc_utilities:now_microsec()-?PHYSICS_THRESHOLD, NewVC);
							        false->
								        DepUpbound
						        end,
						        Transaction#transaction{snapshot_vc=NewSnapshotVC}
%%							        no_compatible_operation_found ->
%%								        lager:info("there no_compatible_operation_found, Len = ~p", [Len]),
%%								        case Len of 1 ->
%%									        JokerVC = Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
%%									        {Transaction#transaction{snapshot_vc = JokerVC}, {JokerVC, JokerVC, JokerVC}};
%%									        _->   {Transaction, {error, no_compatible_operation_found}}
%%								        end
%%						        end
				        end;
			        Protocol when ((Protocol == clocksi) or (Protocol == gr)) ->
				        Transaction
		        end,
%%	        lager:info("~n UpdatedTxnRecord ~p  ",[UpdatedTxnRecord]),
            Result = case ets:lookup(SnapshotCache, Key) of
                         [] ->
	                         {BlankSSRecord, BlankSSCommitParams} = create_empty_materialized_snapshot_record(Transaction, Type),
                             {BlankSSRecord, BlankSSCommitParams, 1};
                         [{_, SnapshotDict}] ->
%%	                         lager:debug("SnapshotDict: ~p", [SnapshotDict]),
                             case vector_orddict:get_smaller(UpdatedTxnRecord#transaction.snapshot_vc, SnapshotDict) of
                                 {undefined, NumberOfSnap} ->
	                                 lager:info("~nno snapshot in the cache for key: ~p",[Key]),
%%	                                 lager:info("~n SnapshotDict is : ~p",[SnapshotDict]),
%%	                                 lager:info("~n TXN Snapshot is : ~p",[UpdatedTxnRecord#transaction.snapshot_vc]),
%%	                                 lager:info("~n Old DepUpbound Snapshot is : ~p",[Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound]),
	                                 {undefined, NumberOfSnap};
                                 {{SCP, LS}, NumberOfSnap} ->
	                                 case is_record(LS, materialized_snapshot) of
		                                 true -> {LS, SCP, NumberOfSnap};
		                                 false -> {error, bad_returned_record}
	                                 end
                             end
                     end,
	        SnapshotGetResponse =
                case Result of
	                {undefined, NumberOfSnapshot} ->
                        LogId = log_utilities:get_logid_from_key(Key),
                        [Node] = log_utilities:get_preflist_from_key(Key),
	                    SnapshotGetResponseRecord = logging_vnode:get(Node, LogId, UpdatedTxnRecord, Type, Key),
		                SnapshotGetResponseRecord#snapshot_get_response{is_newest_snapshot=NumberOfSnapshot};
                    {MatSnapshotRecord1, SnapshotCommitTime1, IsFirst1} ->
%%	                    lager:info("~nLatestSnapshot1 ~n~p, SnapshotCommitTime1,~n~p ~nOpsForKey ~n~p",[MatSnapshotRecord1, SnapshotCommitTime1, OperationsForKey]),
	                    #snapshot_get_response{
		                    number_of_ops = Len,
		                    ops_list = OperationsForKey,
		                    materialized_snapshot =MatSnapshotRecord1,
		                    commit_parameters= SnapshotCommitTime1, is_newest_snapshot = IsFirst1}
                end,
            case SnapshotGetResponse#snapshot_get_response.number_of_ops of
                0 ->
	                lager:info("this should not happen"),
                            {ok, {SnapshotGetResponse#snapshot_get_response.materialized_snapshot#materialized_snapshot.value,
	                              SnapshotGetResponse#snapshot_get_response.commit_parameters}};
                _ ->
%%	                lager:info("~nSnapshotGetResponse ~n~p", [SnapshotGetResponse]),
                    case clocksi_materializer:materialize(Type, UpdatedTxnRecord, SnapshotGetResponse, MatState) of
                        {ok, Snapshot, NewLastOp, CommitParameters, NewSS, OpAddedCount} ->
%%	                        lager:info("~n Snapshot ~p~n ~n NewLastOp ~p ~n CommitParameters ~p ~n NewSS ~p ~n OpAddedCount ~p", [Snapshot, NewLastOp, CommitParameters, NewSS, OpAddedCount]),
%%	                        lager:info("~nShouldGc ~p", [ShouldGc]),
%%	                        lager:info("~SnapshotGetResponse#snapshot_get_response.is_newest_snapshot ~p", [SnapshotGetResponse#snapshot_get_response.is_newest_snapshot]),
	                        
                            %% the following checks for the case there were no snapshots and there were operations, but none was applicable
                            %% for the given snapshot_time
                            %% But is the snapshot not safe?
                            case CommitParameters of
                                ignore ->
                                    {ok, {Snapshot, CommitParameters}};
                                _ ->
	                                case (NewSS and (SnapshotGetResponse#snapshot_get_response.is_newest_snapshot == 1) and
                                    (OpAddedCount >= ?MIN_OP_STORE_SS)) or ShouldGc of
                                        %% Only store the snapshot if it would be at the end of the list and has new operations added to the
                                        %% previous snapshot
                                        true ->
%%																					lager:info("~n~nStoring Snapshot~n~n"),
                                            case TxnId of
                                                Txid when ((Txid == eunit_test) orelse (Txid == no_txn_inserting_from_log)) ->
%%	                                                lager:info("~nShouldGc ~p", [ShouldGc]),
                                                    internal_store_ss(Key, #materialized_snapshot{last_op_id=NewLastOp, value=Snapshot},
	                                                    CommitParameters, ShouldGc, MatState);
                                                _ ->
                                                    store_ss(Key, #materialized_snapshot{last_op_id=NewLastOp, value=Snapshot}, CommitParameters, ShouldGc)
                                            end;
                                        false ->
                                            ok
                                    end,
                                    {ok, {Snapshot, CommitParameters}}
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

%% @doc This fuction is used by PhysiCS' causally consistent cut for defining
%% which is the latest operation that is compatible with the snapshot
%% the protocol uses the commit time of the operation as the "snapshot time"
%% of this particular read, whithin the transaction.
%%-spec define_snapshot_vc_for_transaction(TxRecord::transaction(), OpsTuple::tuple()) ->
%%	no_operation_to_define_snapshot | no_compatible_operation_found | {CommitVC::vectorclock(), DepVC::vectorclock(), ReadVC::vectorclock()}.
%%define_snapshot_vc_for_transaction(_Transaction, []) ->
%%    no_operation_to_define_snapshot;
%%define_snapshot_vc_for_transaction(Transaction, OperationTuple) ->
%%    LocalDCReadTime = dc_utilities:now_microsec(),
%%    define_snapshot_vc_for_transaction(Transaction, LocalDCReadTime, first_operation, OperationTuple, 0).
%%define_snapshot_vc_for_transaction(Transaction, LocalDCReadTime, ReadVC, OperationsTuple, PositionInOpList) ->
%%	{Length,_ListLen} = element(2, OperationsTuple),
%%%%	lager:debug("{Length,_ListLen} ~n ~p", [{Length,_ListLen}]),
%%	case PositionInOpList of
%%		Length ->
%%			no_compatible_operation_found;
%%		_->
%%			{_OpId, OperationRecord}= element((?FIRST_OP+Length-1) - PositionInOpList, OperationsTuple),
%%%%			{_OpId, OperationRecord}= element((?FIRST_OP+ PositionInOpList), OperationsTuple),
%%%%			lager:debug("~n~n~nOperation ~p in Record ~n ~p", [PositionInOpList, OperationRecord]),
%%%%			lager:debug("~n~n~nOperation List ~p", [OperationsTuple]),
%%%%			[{_OpId, Op} | Rest] = OperationsTuple,
%%			TxCTLowBound = Transaction#transaction.physics_read_metadata#physics_read_metadata.commit_time_lowbound,
%%			TxDepUpBound = Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
%%
%%%%			lager:info("~n~n~TxCTLowBound ~p ~n TxDepUpBound ~n ~p", [TxCTLowBound, TxDepUpBound]),
%%
%%			OperationDependencyVC = OperationRecord#operation_payload.dependency_vc,
%%			{OperationDC, OperationCommitTime} = OperationRecord#operation_payload.dc_and_commit_time,
%%			OperationCommitVC = vectorclock:set_clock_of_dc(OperationDC, OperationCommitTime, OperationDependencyVC),
%%
%%%%			lager:info("~n~n~OperationDependencyVC ~p ~n OperationCommitVC ~n ~p", [OperationDependencyVC, OperationCommitVC]),
%%
%%			AccReadVC= case ReadVC of
%%				first_operation -> %% newest operation in the list.
%%					LocalDC = dc_utilities:get_my_dc_id(),
%%					OPCommitVCLocalDC = vectorclock:get_clock_of_dc(LocalDC, OperationCommitVC),
%%					vectorclock:set_clock_of_dc(LocalDC, max(LocalDCReadTime, OPCommitVCLocalDC), OperationCommitVC);
%%				_ ->
%%					ReadVC
%%			end,
%%			case is_causally_compatible(AccReadVC, TxCTLowBound, OperationDependencyVC, TxDepUpBound) of
%%				true ->
%%%%					lager:debug("the final operation defining the snapshot will be: ~n~p", [{OperationCommitVC, OperationDependencyVC, FinalReadVC}]),
%%					{OperationCommitVC, OperationDependencyVC, AccReadVC};
%%				false ->
%%					NewOperationCommitVC = vectorclock:set_clock_of_dc(OperationDC, OperationCommitTime - 1, OperationCommitVC),
%%					define_snapshot_vc_for_transaction(Transaction, LocalDCReadTime, NewOperationCommitVC, OperationsTuple, PositionInOpList + 1)
%%			end
%%	end.
%%
%%is_causally_compatible(CommitClock, CommitTimeLowbound, DepClock, DepUpbound) ->
%%	NewVC = vectorclock:new(),
%%	(vectorclock:ge(CommitClock, CommitTimeLowbound)  orelse (CommitTimeLowbound == NewVC))
%%		and (vectorclock:le(DepClock, DepUpbound) orelse (DepUpbound == NewVC)).

-spec create_empty_materialized_snapshot_record(transaction(), type()) -> {#materialized_snapshot{}, vectorclock() | {vectorclock(), vectorclock(), vectorclock()}}.
create_empty_materialized_snapshot_record(Transaction, Type) ->
	NewVC = vectorclock:new(),
	case Transaction#transaction.transactional_protocol of
        physics ->
	        {#materialized_snapshot{last_op_id = 0, value = clocksi_materializer:new(Type)}, {NewVC, NewVC, NewVC}};
        Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
            {#materialized_snapshot{last_op_id = 0, value = clocksi_materializer:new(Type)}, NewVC}
    end.

%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec op_not_already_in_snapshot(snapshot_time() | ignore, vectorclock()) -> boolean().
op_not_already_in_snapshot(ignore, _) ->
    true;
op_not_already_in_snapshot(_, ignore) ->
    true;
op_not_already_in_snapshot(_, empty) ->
    true;
op_not_already_in_snapshot(empty, _) ->
    true;
op_not_already_in_snapshot(Parameters, CommitVC) ->
    not vectorclock:le(CommitVC, Parameters).


%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), vector_orddict:vector_orddict(),
                         boolean(),mat_state()) -> true.
snapshot_insert_gc(Key, SnapshotDict, ShouldGc, #mat_state{snapshot_cache = SnapshotCache, ops_cache = OpsCache})->
    %% Perform the garbage collection when the size of the snapshot dict passed the threshold
    %% or when a GC is forced (a GC is forced after every ?OPS_THRESHOLD ops are inserted into the cache)
    %% Should check op size here also, when run from op gc
%%	lager:info("~n~SHOULD  GC??? ~p ~p ", [vector_orddict:size(SnapshotDict) >= ?SNAPSHOT_THRESHOLD, ShouldGc]),
    case ((vector_orddict:size(SnapshotDict)) >= ?SNAPSHOT_THRESHOLD) orelse ShouldGc of
        true ->
%%	        lager:info("~n~nWILL GC!!!"),
            %% snapshots are no longer totally ordered
            PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            FirstOp = vector_orddict:last(PrunedSnapshots),
            {CommitParameters, _S} = FirstOp,
	        CT = case CommitParameters of
		        {CTa, _, _} -> CTa;
		        _-> CommitParameters
	        end,
	    CommitTime = lists:foldl(fun({CParams1,_Snapshot}, Acc) ->
						    CT1 = case CParams1 of
							    {CTb, _, _} -> CTb;
							    _-> CParams1
						    end,
					     vectorclock:min([CT1, Acc])
				     end, CT, vector_orddict:to_list(PrunedSnapshots)),
	    {Key,Length,OpId,ListLen,OpsDict} = case ets:lookup(OpsCache, Key) of
						    [] ->
							{Key, 0, 0, 0, {}};
						    [Tuple] ->
							tuple_to_key(Tuple,false)
						end,
            {NewLength,PrunedOps}=prune_ops({Length,OpsDict}, CommitTime),
            true = ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
	    %% Check if the pruned ops are larger or smaller than the previous list size
	    %% if so create a larger or smaller list (by dividing or multiplying by 2)
	    %% (Another option would be to shrink to a more "minimum" size, but need to test to see what is better)
	    NewListLen = case NewLength > ListLen - ?RESIZE_THRESHOLD of
			     true ->
				 ListLen * 2;
			     false ->
				 HalfListLen = ListLen div 2,
				 case HalfListLen =< ?OPS_THRESHOLD of
				     true ->
					 %% Don't shrink list, already minimun size
					 ListLen;
				     false ->
					 %% Only shrink if shrinking would leave some space for new ops
					 case HalfListLen - ?RESIZE_THRESHOLD > NewLength of
					     true ->
						 HalfListLen;
					     false ->
						 ListLen
					 end
				 end
			 end,
	    NewTuple = erlang:make_tuple(?FIRST_OP+NewListLen,0,[{1,Key},{2,{NewLength,NewListLen}},{3,OpId}|PrunedOps]),
	    true = ets:insert(OpsCache, NewTuple);
	false ->
%%		lager:info("~n~WONT GC!!!"),
            true = ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops({non_neg_integer(),tuple()}, snapshot_time())->
		       {non_neg_integer(),[{non_neg_integer(),op_and_id()}]}.
prune_ops({Len,OpsTuple}, Threshold)->
    %% should write custom function for this in the vector_orddict
    %% or have to just traverse the entire list?
    %% since the list is ordered, can just stop when all values of
    %% the op is smaller (i.e. not concurrent)
    %% So can add a stop function to ordered_filter
    %% Or can have the filter function return a tuple, one vale for stopping
    %% one for including
%%	lager:info ("~n old tuple : ~n~p", [OpsTuple]),
	{NewSize, NewOps}=check_filter(fun({_OpId, Op})->
		BaseSnapshotVC=Op#operation_payload.dependency_vc,
		{DcId, CommitTime}=Op#operation_payload.dc_and_commit_time,
		CommitVC=vectorclock:set_clock_of_dc(DcId, CommitTime, BaseSnapshotVC),
		(op_not_already_in_snapshot(Threshold, CommitVC))
	%%	    case Result of
	%%		    true -> lager:info("~nHAVE TO KEEP THIS OP! ~n CommitVC = ~p ~n Oldest Snapshot CT = ~p",
	%%			                [CommitVC, BaseSnapshotVC]);
	%%		    false ->
	%%			    lager:info("~nWILL REMOVE THIS OP! ~n CommitVC = ~p ~n Oldest Snapshot CT = ~p",
	%%				    [CommitVC, BaseSnapshotVC])
	%%	    end,
	%%        Result
	end, ?FIRST_OP, ?FIRST_OP+Len, ?FIRST_OP, OpsTuple, 0, []),
%%	lager:info ("~n new tuple : ~n~p ~n ARE THEY THE SAME? ", [NewOps]),
    case NewSize of
	0 ->
	    First = element(?FIRST_OP+Len,OpsTuple),
	    {1,[{?FIRST_OP,First}]};
	_ -> {NewSize,NewOps}
    end.

%% This function will go through a tuple of operations, filtering out the operations
%% that are out of date (given by the input function Fun), and returning a list
%% of the remaining operations and the size of that list
%% It is used during garbage collection to filter out operations that are older than any
%% of the cached snapshots
-spec check_filter(fun(({non_neg_integer(),operation_payload()}) -> boolean()), non_neg_integer(), non_neg_integer(),
		   non_neg_integer(),tuple(),non_neg_integer(),[{non_neg_integer(),op_and_id()}]) ->
			  {non_neg_integer(),[{non_neg_integer(),op_and_id()}]}.
check_filter(_Fun,Id,Last,_NewId,_Tuple,NewSize,NewOps) when (Id == Last) ->
    {NewSize,NewOps};
check_filter(Fun,Id,Last,NewId,Tuple,NewSize,NewOps) ->
    Op = element(Id, Tuple),
    case Fun(Op) of
	true ->
	    check_filter(Fun,Id+1,Last,NewId+1,Tuple,NewSize+1,[{NewId,Op}|NewOps]);
	false ->
	    check_filter(Fun,Id+1,Last,NewId,Tuple,NewSize,NewOps)
    end.

%% This is an internal function used to convert the tuple stored in ets
%% to a tuple and list usable by the materializer
%% The second argument if true will convert the ops tuple to a list of ops
%% Otherwise it will be kept as a tuple
-spec tuple_to_key(tuple(),boolean()) -> {any(),integer(),non_neg_integer(),non_neg_integer(),
					  [op_and_id()]|tuple()}.
tuple_to_key(Tuple,ToList) ->
    Key = element(1, Tuple),
    {Length,ListLen} = element(2, Tuple),
    OpId = element(3, Tuple),
    Ops =
	case ToList of
	    true ->
		tuple_to_key_int(?FIRST_OP,Length+?FIRST_OP,Tuple,[]);
	    false ->
		Tuple
	end,
    {Key,Length,OpId,ListLen,Ops}.
tuple_to_key_int(Next,Next,_Tuple,Acc) ->
    Acc;
tuple_to_key_int(Next,Last,Tuple,Acc) ->
    tuple_to_key_int(Next+1,Last,Tuple,[element(Next,Tuple)|Acc]).

%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), operation_payload(), mat_state(), transaction() | no_txn_inserting_from_log) -> ok | {error, {op_gc_error, reason()}}.
op_insert_gc(Key, DownstreamOp, State = #mat_state{ops_cache = OpsCache}, Transaction)->
    case ets:member(OpsCache, Key) of
	false ->
%%		lager:debug("inserting key: ~p",[Key]),
	    ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP+?OPS_THRESHOLD,0,[{1,Key},{2,{0,?OPS_THRESHOLD}}]));
	true ->
	    ok
    end,
    NewId = ets:update_counter(OpsCache, Key, {3,1}),
    {Length,ListLen} = ets:lookup_element(OpsCache, Key, 2),
    %% Perform the GC incase the list is full, or every ?OPS_THRESHOLD operations (which ever comes first)
    case ((Length)>=ListLen) or ((NewId rem ?OPS_THRESHOLD) == 0) of
        true ->
	        Type = DownstreamOp#operation_payload.type,
	        NewTransaction = case Transaction of
		        no_txn_inserting_from_log -> %% the function is being called by the logging vnode at startup
			        {ok, Protocol} = application:get_env(antidote, txn_prot),
			        #transaction{snapshot_vc = DownstreamOp#operation_payload.dependency_vc,
				        transactional_protocol = Protocol, txn_id = no_txn_inserting_from_log};
		        _ ->
			        Transaction#transaction{
				        snapshot_vc = DownstreamOp#operation_payload.dependency_vc}
	        end,
%%	        lager:info("~nOp GC calling internal read: ~p",[NewTransaction#transaction.snapshot_vc]),
%%	        lager:info("~nOp is: ~p",[NewTransaction#transaction.snapshot_vc]),
	        %% Here is where the GC is done (with the 5th argument being "true", GC is performed by the internal read
%%	        lager:info("~n~n~nCALLING INTERNAL READ WITH SHOULDGC TRUE!!! ~n~n~n"),
	        case internal_read(Key, Type, NewTransaction, State, true) of
		        {ok, _} ->
			        %% Have to get the new ops dict because the interal_read can change it
			        {Length1, ListLen1} = ets:lookup_element(OpsCache, Key, 2),
%%			        lager:info("BEFORE GC: Key ~p,  Length ~p,  ListLen ~p",[Key, Length, ListLen]),
%%			        lager:info("AFTER GC: Key ~p,  Length ~p,  ListLen ~p",[Key, Length1, ListLen1]),
%%			        case Length1 == Length of
%%				        true ->
%%					        {error, operation_gc_did_not_clean_ops};
%%				        false ->
					        true = ets:update_element(OpsCache, Key, [{Length1 + ?FIRST_OP, {NewId, DownstreamOp}}, {2, {Length1 + 1, ListLen1}}]),
					        ok;
%%			        end;
		        {error, Reason} ->
			        {error, {op_gc_error, Reason}}
	        end;
        false ->
	    true = ets:update_element(OpsCache, Key, [{Length+?FIRST_OP,{NewId,DownstreamOp}}, {2,{Length+1,ListLen}}]),
        ok
    end.

-ifdef(TEST).

%% @doc Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
	CommitTime1a= 1,
	CommitTime2a= 1,
	CommitTime1b= 1,
	CommitTime2b= 7,
	SnapshotClockDC1 = 5,
	SnapshotClockDC2 = 5,
	CommitTime3a= 5,
	CommitTime4a= 5,
	CommitTime3b= 10,
	CommitTime4b= 10,

    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
    ?assertEqual(true, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime1a},{2,CommitTime1b}]),
        vectorclock:set_clock_of_dc(1, SnapshotClockDC1, SnapshotVC))),
    ?assertEqual(true, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime2a},{2,CommitTime2b}]),
        vectorclock:set_clock_of_dc(2, SnapshotClockDC2, SnapshotVC))),
    ?assertEqual(false, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime3a},{2,CommitTime3b}]),
        vectorclock:set_clock_of_dc(1, SnapshotClockDC1, SnapshotVC))),
    ?assertEqual(false, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime4a},{2,CommitTime4b}]),
        vectorclock:set_clock_of_dc(2, SnapshotClockDC2, SnapshotVC))).

%% @doc This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    MatState = #mat_state{ops_cache= ets:new(ops_cache, [set]),
                          snapshot_cache= ets:new(snapshot_cache, [set])},
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter,

    %% Make 10 snapshots
	{ok, {Res0, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2}])}, MatState),
	?assertEqual(0, Type:value(Res0)),
	
	op_insert_gc(Key, generate_payload(10,11,Res0,a1), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res1, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,12}])}, MatState),
	?assertEqual(1, Type:value(Res1)),
	
	op_insert_gc(Key, generate_payload(20,21,Res1,a2), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res2, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,22}])}, MatState),
	?assertEqual(2, Type:value(Res2)),
	
	op_insert_gc(Key, generate_payload(30,31,Res2,a3), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res3, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,32}])}, MatState),
	?assertEqual(3, Type:value(Res3)),
	
	op_insert_gc(Key, generate_payload(40,41,Res3,a4), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res4, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,42}])}, MatState),
	?assertEqual(4, Type:value(Res4)),
	
	op_insert_gc(Key, generate_payload(50,51,Res4,a5), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res5, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,52}])}, MatState),
	?assertEqual(5, Type:value(Res5)),
	
	op_insert_gc(Key, generate_payload(60,61,Res5,a6), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res6, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,62}])}, MatState),
	?assertEqual(6, Type:value(Res6)),
	
	op_insert_gc(Key, generate_payload(70,71,Res6,a7), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res7, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,72}])}, MatState),
	?assertEqual(7, Type:value(Res7)),
	
	op_insert_gc(Key, generate_payload(80,81,Res7,a8), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res8, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,82}])}, MatState),
	?assertEqual(8, Type:value(Res8)),
	
	op_insert_gc(Key, generate_payload(90,91,Res8,a9), MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res9, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,92}])}, MatState),
	?assertEqual(9, Type:value(Res9)),
	
	op_insert_gc(Key, generate_payload(100,101,Res9,a10), MatState, #transaction{txn_id = eunit_test}),
	
	%% Insert some new values
	
	op_insert_gc(Key, generate_payload(15,111,Res1,a11), MatState, #transaction{txn_id = eunit_test}),
	op_insert_gc(Key, generate_payload(16,121,Res1,a12), MatState, #transaction{txn_id = eunit_test}),
	
	%% Trigger the clean
	
	Tx = #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,102}])},
	
	{ok, {Res10, _}} = internal_read(Key, Type,
		Tx , MatState),
	?assertEqual(10, Type:value(Res10)),
	
	op_insert_gc(Key, generate_payload(102,131,Res9,a13), MatState, Tx),
	
	%% Be sure you didn't loose any updates
	{ok, {Res13, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,142}])}, MatState),
	?assertEqual(13, Type:value(Res13)).

%% @doc This tests to make sure operation lists can be large and resized
large_list_test() ->
	MatState = #mat_state{ops_cache= ets:new(ops_cache, [set]),
		snapshot_cache= ets:new(snapshot_cache, [set])},
	Key = mycount,
	DC1 = 1,
	Type = antidote_crdt_counter,
	
	%% Make 1000 updates to grow the list, whithout generating a snapshot to perform the gc
	{ok, {Res0, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2}])}, MatState),
	?assertEqual(0, Type:value(Res0)),
	%%    lager:info("Res0 = ~p", [Res0]),
	
	lists:foreach(fun(Val) ->
		Op = generate_payload(10,11+Val,Res0,Val),
		%%        lager:info("Op= ~p", [Op]),
		op_insert_gc(Key, Op, MatState,
			#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 11+Val}])} )
	end, lists:seq(1,1000)),
	
	{ok, {Res1000, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2000}])}, MatState),
	?assertEqual(1000, Type:value(Res1000)),
	
	%% Now check everything is ok as the list shrinks from generating new snapshots
	lists:foreach(fun(Val) ->
		op_insert_gc(Key, generate_payload(10+Val,11+Val,Res0,Val), MatState,
			#transaction{txn_id = eunit_test, transactional_protocol = clocksi,
				snapshot_vc = vectorclock:from_list([{DC1,2000}])}),
		{ok, {Res, _}} = internal_read(Key, Type,
			#transaction{txn_id = eunit_test, transactional_protocol = clocksi,
				snapshot_vc = vectorclock:from_list([{DC1,2000}])}, MatState),
		?assertEqual(Val, Type:value(Res))
	end, lists:seq(1001,1100)).


generate_payload(SnapshotTime,CommitTime,Prev,_Name) ->
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,

    {ok,Op1} = Type:downstream({increment, 1}, Prev),
    #operation_payload{key = Key,
		     type = Type,
		     op_param = Op1,
	         dependency_vc= vectorclock:from_list([{DC1,SnapshotTime}]),
	         dc_and_commit_time = {DC1,CommitTime},
		     txid = 1
		    }.

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    S1 = Type:new(),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment
    {ok,Op1} = Type:downstream({increment,1},S1),
    DownstreamOp1 = #operation_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     dependency_vc= vectorclock:from_list([{DC1,10}]),
                                     dc_and_commit_time= {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, MatState, #transaction{txn_id = eunit_test}),
    {ok, {Res1, _}} = internal_read(Key, Type,
	    #transaction{txn_id = eunit_test,
		    transactional_protocol = clocksi,
		    snapshot_vc = vectorclock:from_list([{DC1, 16}])},
	    MatState),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok,Op2} = Type:downstream({increment,1},S1),
    DownstreamOp2 = DownstreamOp1#operation_payload{
                      op_param = Op2,
                      dependency_vc=vectorclock:from_list([{DC1,16}]),
                      dc_and_commit_time = {DC1,20},
                      txid=2},
	
    op_insert_gc(Key,DownstreamOp2, MatState, #transaction{txn_id = eunit_test}),
    {ok, {Res2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 21}])}, MatState),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, {ReadOld, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 16}])}, MatState),
    ?assertEqual(1, Type:value(ReadOld)).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},


    %% Insert one increment in DC1
	{ok,Op1} = Type:downstream({increment,1},S1),
	DownstreamOp1 = #operation_payload{key = Key,
		type = Type,
		op_param = Op1,
		dependency_vc= vectorclock:from_list([{DC2,0}, {DC1,10}]),
		dc_and_commit_time = {DC1, 15},
		txid = 1
	},
	op_insert_gc(Key,DownstreamOp1,MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res1, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 16}, {DC2, 0}])}, MatState),
	?assertEqual(1, Type:value(Res1)),
	
	%% Insert second increment in other DC
	{ok,Op2} = Type:downstream({increment,1},S1),
	DownstreamOp2 = DownstreamOp1#operation_payload{
		op_param = Op2,
		dependency_vc=vectorclock:from_list([{DC2,16}, {DC1,16}]),
		dc_and_commit_time = {DC2,20},
		txid=2},
	
	op_insert_gc(Key,DownstreamOp2,MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res2, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,16}, {DC2,21}])}, MatState),
	?assertEqual(2, Type:value(Res2)),
	
	%% Read old version
	{ok, {ReadOld, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,15}, {DC2,15}])}, MatState),
	?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
	MatState = #mat_state{ops_cache= ets:new(ops_cache, [set]),
		snapshot_cache= ets:new(snapshot_cache, [set])},
	Key = mycount,
	Type = antidote_crdt_counter,
	DC1 = local,
	DC2 = remote,
	S1 = Type:new(),
	
	%% Insert one increment in DC1
	{ok,Op1} = Type:downstream({increment,1},S1),
	DownstreamOp1 = #operation_payload{key = Key,
		type = Type,
		op_param = Op1,
		dependency_vc= vectorclock:from_list([{DC1,0}, {DC2,0}]),
		dc_and_commit_time = {DC2, 1},
		txid = 1
	},
	op_insert_gc(Key,DownstreamOp1,MatState, #transaction{txn_id = eunit_test}),
	{ok, {Res1, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,0}, {DC2,1}])}, MatState),
	?assertEqual(1, Type:value(Res1)),
	
	%% Another concurrent increment in other DC
	{ok, Op2} = Type:downstream({increment,1},S1),
	DownstreamOp2 = #operation_payload{ key = Key,
		type = Type,
		op_param = Op2,
		dependency_vc=vectorclock:from_list([{DC1,0}, {DC2,0}]),
		dc_and_commit_time = {DC1, 1},
		txid=2},
	op_insert_gc(Key,DownstreamOp2,MatState, #transaction{txn_id = eunit_test}),
	
	%% Read different snapshots
	{ok, {ReadDC1, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,1}, {DC2,0}])}, MatState),
	?assertEqual(1, Type:value(ReadDC1)),
	io:format("Result1 = ~p", [ReadDC1]),
	{ok, {ReadDC2, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,0}, {DC2,1}])}, MatState),
	io:format("Result2 = ~p", [ReadDC2]),
	?assertEqual(1, Type:value(ReadDC2)),
	
	%% Read snapshot including both increments
	{ok, {Res2, _}} = internal_read(Key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,1}, {DC2,1}])}, MatState),
	?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
	MatState = #mat_state{ops_cache= ets:new(ops_cache, [set]),
		snapshot_cache= ets:new(snapshot_cache, [set])},
	Type = riak_dt_gcounter,
	{ok, {ReadResult, _}} = internal_read(key, Type,
		#transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{dc1,1}, {dc2,0}])}, MatState),
	?assertEqual(0, Type:value(ReadResult)).

%%is_causally_compatible_test() ->
%%
%%	NewVC = vectorclock:new(),
%%
%%	DepUpBound = [{dc1, 1}, {dc2, 2}],
%%	RTLowBound = [{dc1, 5}, {dc2, 10}],
%%
%%	OpDepVC1 = [{dc1, 2}, {dc2, 3}],
%%	OpCommitVC1 = [{dc1, 5}, {dc2, 10}],
%%
%%	true = is_causally_compatible(OpCommitVC1, NewVC, OpDepVC1, NewVC),
%%
%%
%%	false = is_causally_compatible(OpCommitVC1, RTLowBound, OpDepVC1, DepUpBound),
%%
%%	OpDepVC2 = [{dc1, 1}, {dc2, 2}],
%%	OpCommitVC2 = [{dc1, 5}, {dc2, 10}],
%%
%%	true = is_causally_compatible(OpCommitVC2, RTLowBound, OpDepVC2, DepUpBound),
%%
%%	OpDepVC3 = [],
%%	OpCommitVC3 = [{dc1, 5}, {dc2, 10}],
%%
%%	true = is_causally_compatible(OpCommitVC3, RTLowBound, OpDepVC3, DepUpBound),
%%
%%	OpDepVC4 = [],
%%	OpCommitVC4 = [{dc1, 4}, {dc2, 10}],
%%
%%	false = is_causally_compatible(OpCommitVC4, RTLowBound, OpDepVC4, DepUpBound),
%%	true = is_causally_compatible(OpCommitVC4, [], OpDepVC4, []),
%%
%%
%%	DepUpBound2 = [{dc1, 1}, {dc2, 2}],
%%	RTLowBound2 = [{dc1, 1}, {dc2, 2}],
%%
%%	OpDepVC21 = [{dc1, 1}, {dc2, 2}],
%%	OpCommitVC21 = [{dc1, 4}, {dc2, 10}],
%%
%%	true = is_causally_compatible(OpCommitVC21, RTLowBound2, OpDepVC21, DepUpBound2),
%%
%%	OpDepVC22 = [{dc1, 1}, {dc2, 2}],
%%	OpCommitVC22 = [{dc1, 0}, {dc2, 10}],
%%
%%	false = is_causally_compatible(OpCommitVC22, RTLowBound2, OpDepVC22, DepUpBound2).

-endif.
   
