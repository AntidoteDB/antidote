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


-define(SNAPSHOT_THRESHOLD, 10).
-define(SNAPSHOT_MIN, 3).
-define(OPS_THRESHOLD, 50).
-define(FIRST_OP, 4).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
	       check_tables_ready/0,
         read/7,
	       get_cache_name/2,
	       store_ss/3,
         update/2,
	       belongs_to_snapshot_op/3]).

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


-record(state, {
  partition :: partition_id(),
  ops_cache :: cache_id(),
  snapshot_cache :: cache_id()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), txid(),cache_id(), cache_id(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, TxId,OpsCache,SnapshotCache,Partition) ->
    case ets:info(OpsCache) of
	undefined ->
	    riak_core_vnode_master:sync_command({Partition,node()},
						{read,Key,Type,SnapshotTime,TxId},
						materializer_vnode_master,
						infinity);
	_ ->
	    internal_read(Key, Type, SnapshotTime, TxId, OpsCache, SnapshotCache)
    end.

-spec get_cache_name(non_neg_integer(),atom()) -> atom().
get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), snapshot(), snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command(IndexNode, {store_ss,Key, Snapshot, CommitTime},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

-spec open_table(partition_id(), 'ops_cache' | 'snapshot_cache') -> atom() | ets:tid().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
	undefined ->
	    ets:new(get_cache_name(Partition, Name),
		    [set, protected, named_table, ?TABLE_CONCURRENCY]);
	_ ->
	    %% Other vnode hasn't finished closing tables
	    lager:info("Unable to open ets table in materializer vnode, retrying"),
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
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_ready},
						 materializer_vnode_master,
						 infinity),
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.

handle_command({check_ready},_Sender,State = #state{partition=Partition}) ->
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
    {reply, Result, State};


handle_command({read, Key, Type, SnapshotTime, TxId}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache,partition=Partition})->
    {reply, read(Key, Type, SnapshotTime, TxId,OpsCache,SnapshotCache,Partition), State};

handle_command({update, Key, DownstreamOp}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    true = op_insert_gc(Key,DownstreamOp, OpsCache, SnapshotCache),
    {reply, ok, State};


handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    internal_store_ss(Key,Snapshot,CommitTime,OpsCache,SnapshotCache,false),
    {noreply, State};


handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0} ,
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
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

handle_handoff_data(Data, State=#state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{ops_cache=_OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache}) ->
    try
	ets:delete(OpsCache),
	ets:delete(SnapshotCache)
    catch
	_:_Reason->
	    ok
    end,
    ok.



%%---------------- Internal Functions -------------------%%

-spec internal_store_ss(key(), snapshot(), snapshot_time(), cache_id(), cache_id(),boolean()) -> true.
internal_store_ss(Key,Snapshot,CommitTime,OpsCache,SnapshotCache,ShouldGc) ->
    SnapshotDict = case ets:lookup(SnapshotCache, Key) of
		       [] ->
			   vector_orddict:new();
		       [{_, SnapshotDictA}] ->
			   SnapshotDictA
		   end,
    SnapshotDict1 = case ShouldGc of
			true ->
			    vector_orddict:insert_bigger(CommitTime,Snapshot, vector_orddict:new());
			false ->
			    vector_orddict:insert_bigger(CommitTime,Snapshot, SnapshotDict)
		    end,
    snapshot_insert_gc(Key,SnapshotDict1, SnapshotCache, OpsCache,ShouldGc).

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), snapshot_time(), txid() | ignore, cache_id(), cache_id()) -> {ok, snapshot()} | {error, no_snapshot}.
internal_read(Key, Type, MinSnapshotTime, TxId, OpsCache, SnapshotCache) ->
    internal_read(Key, Type, MinSnapshotTime, TxId, OpsCache, SnapshotCache,false).

internal_read(Key, Type, MinSnapshotTime, TxId, OpsCache, SnapshotCache,ShouldGc) ->
    Result = case ets:lookup(SnapshotCache, Key) of
		 [] ->
		     %% First time reading this key, store an empty snapshot in the cache
		     BlankSS = {0,clocksi_materializer:new(Type)},
		     case TxId of
			 ignore ->
			     internal_store_ss(Key,BlankSS,vectorclock:new(),OpsCache,SnapshotCache,false);
			 _ ->
			     materializer_vnode:store_ss(Key,BlankSS,vectorclock:new())
		     end,
		     {BlankSS,ignore,true};
		 [{_, SnapshotDict}] ->
		     case vector_orddict:get_smaller(MinSnapshotTime, SnapshotDict) of
			 {undefined, _IsF} ->
			     {error, no_snapshot};
			 {{SCT, LS},IsF}->
			     {LS,SCT,IsF}
		     end
	     end,
    {Length,Ops,{LastOp,LatestSnapshot},SnapshotCommitTime,IsFirst} =
	case Result of
	    {error, no_snapshot} ->
		LogId = log_utilities:get_logid_from_key(Key),
		[Node] = log_utilities:get_preflist_from_key(Key),
		Res = logging_vnode:get(Node, {get, LogId, MinSnapshotTime, Type, Key}),
		Res;
	    {LatestSnapshot1,SnapshotCommitTime1,IsFirst1} ->
		case ets:lookup(OpsCache, Key) of
		    [] ->
			{0, [], LatestSnapshot1,SnapshotCommitTime1,IsFirst1};
		    [Tuple] ->
			{Key,Length1,_OpId,AllOps} = tuple_to_key(Tuple),
			{Length1, AllOps, LatestSnapshot1, SnapshotCommitTime1, IsFirst1}
		end
	end,
    case Length of
	0 ->
	    {ok, LatestSnapshot};
	_Len ->
	    case clocksi_materializer:materialize(Type, LatestSnapshot, LastOp, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) of
		{ok, Snapshot, NewLastOp, CommitTime, NewSS} ->
		    %% the following checks for the case there were no snapshots and there were operations, but none was applicable
		    %% for the given snapshot_time
		    %% But is the snapshot not safe?
		    case CommitTime of
			ignore ->
			    {ok, Snapshot};
			_ ->
			    case (NewSS and IsFirst) orelse ShouldGc of
				%% Only store the snapshot if it would be at the end of the list and has new operations added to the
				%% previous snapshot
				true ->
				    case TxId of
					ignore ->
					    internal_store_ss(Key,{NewLastOp,Snapshot},CommitTime,OpsCache,SnapshotCache,ShouldGc);
					_ ->
					    materializer_vnode:store_ss(Key,{NewLastOp,Snapshot},CommitTime)
				    end;
				_ ->
				    ok
			    end,
			    {ok, Snapshot}
		    end;
		{error, Reason} ->
		    {error, Reason}
	    end
    end.

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, {_OpDc,_OpCommitTime}, _OpSs) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc,OpCommitTime}, OpSs) ->
    OpSs1 = dict:store(OpDc,OpCommitTime,OpSs),
    not vectorclock:le(OpSs1,SSTime).

%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), vector_orddict:vector_orddict(),
                         cache_id(),cache_id(),boolean()) -> true.
snapshot_insert_gc(Key, SnapshotDict, SnapshotCache, OpsCache,ShouldGc)->
    %% Should check op size here also, when run from op gc
    case ((vector_orddict:size(SnapshotDict))>=?SNAPSHOT_THRESHOLD) orelse ShouldGc of
        true ->
	    %% snapshots are no longer totally ordered
	    PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            FirstOp=vector_orddict:last(PrunedSnapshots),
            {CT, _S} = FirstOp,
	    CommitTime = lists:foldl(fun({CT1,_ST}, Acc) ->
					     vectorclock:min([CT1, Acc])
				     end, CT, vector_orddict:to_list(PrunedSnapshots)),
	    {Length,OpId,OpsDict} = case ets:lookup(OpsCache, Key) of
					[] ->
					    {0, 0, []};
					[Tuple] ->
					    {Key,Length1,OpId1,Ops} = tuple_to_key(Tuple),
					    {Length1, OpId1, Ops}
				    end,
            {NewLength,PrunedOps}=prune_ops({Length,OpsDict}, CommitTime),
            ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
	    true = ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP+?OPS_THRESHOLD,0,[{1,Key},{2,NewLength},{3,OpId}|PrunedOps]));
        false ->
            true = ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops({non_neg_integer(),[any(),...]}, snapshot_time())-> {non_neg_integer(),[any(),...]}.
prune_ops({_Len,OpsDict}, Threshold)->
%% should write custom function for this in the vector_orddict
%% or have to just traverse the entire list?
%% since the list is ordered, can just stop when all values of
%% the op is smaller (i.e. not concurrent)
%% So can add a stop function to ordered_filter
%% Or can have the filter function return a tuple, one vale for stopping
%% one for including
    Res = reverse_and_filter(fun({_OpId,Op}) ->
				     OpCommitTime=Op#clocksi_payload.commit_time,
				     (belongs_to_snapshot_op(Threshold,OpCommitTime,Op#clocksi_payload.snapshot_time))
			     end, OpsDict, ?FIRST_OP, []),
    case Res of
	{_,[]} ->
	    [First|_Rest] = OpsDict,
	    {1,[{?FIRST_OP,First}]};
	_ ->
	    Res
    end.

%% This is an internal function used to convert the tuple stored in ets
%% to a tuple and list usable by the materializer
-spec tuple_to_key(tuple()) -> {any(),non_neg_integer(),non_neg_integer(),list()}.
tuple_to_key(Tuple) ->
    Key = element(1, Tuple),
    Length = element(2, Tuple),
    OpId = element(3, Tuple),
    Ops = tuple_to_key_int(?FIRST_OP,Length+?FIRST_OP,Tuple,[]),
    {Key,Length,OpId,Ops}.
tuple_to_key_int(Next,Next,_Tuple,Acc) ->
    Acc;
tuple_to_key_int(Next,Last,Tuple,Acc) ->
    tuple_to_key_int(Next+1,Last,Tuple,[element(Next,Tuple)|Acc]).

%% This is an internal function used to filter ops and reverse the list
%% It returns a tuple where the first element is the lenght of the list returned
%% The elements in the list also include the location that they will be placed
%% in the tuple in the ets table, this way the list can be used
%% directly in the erlang:make_tuple function
-spec reverse_and_filter(fun(),list(),non_neg_integer(),list()) -> {non_neg_integer(),list()}.
reverse_and_filter(_Fun,[],Id,Acc) ->
    {Id-?FIRST_OP,Acc};
reverse_and_filter(Fun,[First|Rest],Id,Acc) ->
    case Fun(First) of
	true ->
	    reverse_and_filter(Fun,Rest,Id+1,[{Id,First}|Acc]);
	false ->
	    reverse_and_filter(Fun,Rest,Id,Acc)
    end.

%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), clocksi_payload(), cache_id(), cache_id()) -> true.
op_insert_gc(Key, DownstreamOp, OpsCache, SnapshotCache)->
    case ets:member(OpsCache, Key) of
	false ->
	    ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP+?OPS_THRESHOLD,0,[{1,Key}]));
	true ->
	    ok
    end,
    NewId = ets:update_counter(OpsCache, Key,
			       {3,1}),
    Length = ets:lookup_element(OpsCache, Key, 2),
    case (Length)>=?OPS_THRESHOLD of
        true ->
            Type=DownstreamOp#clocksi_payload.type,
            SnapshotTime=DownstreamOp#clocksi_payload.snapshot_time,
            {_, _} = internal_read(Key, Type, SnapshotTime, ignore, OpsCache, SnapshotCache, true),
	    %% Have to get the new ops dict because the interal_read can change it
	    Length1 = ets:lookup_element(OpsCache, Key, 2),
	    true = ets:update_element(OpsCache, Key, [{Length1+?FIRST_OP,{NewId,DownstreamOp}}, {2,Length1+1}]);
        false ->
	    true = ets:update_element(OpsCache, Key, [{Length+?FIRST_OP,{NewId,DownstreamOp}}, {2,Length+1}])
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
	?assertEqual(true, belongs_to_snapshot_op(
			     vectorclock:from_list([{1, CommitTime1a},{2,CommitTime1b}]), {1, SnapshotClockDC1}, SnapshotVC)),
	?assertEqual(true, belongs_to_snapshot_op(
			     vectorclock:from_list([{1, CommitTime2a},{2,CommitTime2b}]), {2, SnapshotClockDC2}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot_op(
			      vectorclock:from_list([{1, CommitTime3a},{2,CommitTime3b}]), {1, SnapshotClockDC1}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot_op(
			      vectorclock:from_list([{1, CommitTime4a},{2,CommitTime4b}]), {2, SnapshotClockDC2}, SnapshotVC)).


%% @doc This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = riak_dt_gcounter,

    %% Make 10 snapshots

    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1,2}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(Res0)),

    op_insert_gc(Key, generate_payload(10,11,Res0,a1), OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,12}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    op_insert_gc(Key, generate_payload(20,21,Res1,a2), OpsCache, SnapshotCache),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,22}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    op_insert_gc(Key, generate_payload(30,31,Res2,a3), OpsCache, SnapshotCache),
    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC1,32}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(3, Type:value(Res3)),

    op_insert_gc(Key, generate_payload(40,41,Res3,a4), OpsCache, SnapshotCache),
    {ok, Res4} = internal_read(Key, Type, vectorclock:from_list([{DC1,42}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(4, Type:value(Res4)),

    op_insert_gc(Key, generate_payload(50,51,Res4,a5), OpsCache, SnapshotCache),
    {ok, Res5} = internal_read(Key, Type, vectorclock:from_list([{DC1,52}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(5, Type:value(Res5)),

    op_insert_gc(Key, generate_payload(60,61,Res5,a6), OpsCache, SnapshotCache),
    {ok, Res6} = internal_read(Key, Type, vectorclock:from_list([{DC1,62}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(6, Type:value(Res6)),

    op_insert_gc(Key, generate_payload(70,71,Res6,a7), OpsCache, SnapshotCache),
    {ok, Res7} = internal_read(Key, Type, vectorclock:from_list([{DC1,72}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(7, Type:value(Res7)),

    op_insert_gc(Key, generate_payload(80,81,Res7,a8), OpsCache, SnapshotCache),
    {ok, Res8} = internal_read(Key, Type, vectorclock:from_list([{DC1,82}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(8, Type:value(Res8)),

    op_insert_gc(Key, generate_payload(90,91,Res8,a9), OpsCache, SnapshotCache),
    {ok, Res9} = internal_read(Key, Type, vectorclock:from_list([{DC1,92}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(9, Type:value(Res9)),

    op_insert_gc(Key, generate_payload(100,101,Res9,a10), OpsCache, SnapshotCache),

    %% Insert some new values

    op_insert_gc(Key, generate_payload(15,111,Res1,a11), OpsCache, SnapshotCache),
    op_insert_gc(Key, generate_payload(16,121,Res1,a12), OpsCache, SnapshotCache),

    %% Trigger the clean
    {ok, Res10} = internal_read(Key, Type, vectorclock:from_list([{DC1,102}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(10, Type:value(Res10)),

    op_insert_gc(Key, generate_payload(102,131,Res9,a13), OpsCache, SnapshotCache),

    %% Be sure you didn't loose any updates
    {ok, Res13} = internal_read(Key, Type, vectorclock:from_list([{DC1,142}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(13, Type:value(Res13)).




generate_payload(SnapshotTime,CommitTime,Prev,Name) ->
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,

    {ok,Op1} = Type:update(increment, Name, Prev),
    #clocksi_payload{key = Key,
		     type = Type,
		     op_param = {merge, Op1},
		     snapshot_time = vectorclock:from_list([{DC1,SnapshotTime}]),
		     commit_time = {DC1,CommitTime},
		     txid = 1
		    }.



seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    S1 = Type:new(),

    %% Insert one increment
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]),ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC1,16}]),
                      commit_time = {DC1,20},
                      txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).


multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC2,0}, {DC1,10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1,OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16},{DC2,0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok,Op2} = Type:update(increment, b, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
                      commit_time = {DC2,20},
                      txid=2},

    op_insert_gc(Key,DownstreamOp2,OpsCache, SnapshotCache),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,15}, {DC2,15}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,0}, {DC2,0}]),
                                     commit_time = {DC2, 1},
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1,OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2,1}, {DC1,0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:update(increment, b, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
				      type = Type,
				      op_param = {merge, Op2},
				      snapshot_time=vectorclock:from_list([{DC1,0}, {DC2,0}]),
				      commit_time = {DC1, 1},
				      txid=2},
    op_insert_gc(Key,DownstreamOp2,OpsCache, SnapshotCache),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1,1}, {DC2, 0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadDC1)),
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1,0},{DC2,1}]), ignore, OpsCache, SnapshotCache),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2,1}, {DC1,1}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
	OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Type = riak_dt_gcounter,
    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1,1}, {dc2, 0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(ReadResult)).


-endif.
