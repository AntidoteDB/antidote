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
-define(SNAPSHOT_MIN, 2).
-define(OPS_THRESHOLD, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_vnode/1,
         read/4,
         multi_read/4,
         update/2]).

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

-record(state, {partition, ops_cache, snapshot_cache}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time
-spec read(key(), type(), tx_id()) -> {ok, term()} | {error, atom()}.
read(Key, Type, TxId) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref,
                                        {read, Key, Type, TxId},
                                        materializer_vnode_master).

%% @doc Reads multiple keys by calling multiple times the read function above.                                   
multi_read(Vnode, Reads, TxId) ->
	riak_core_vnode_master:sync_command(Vnode,
                                        {multi_read, Reads, TxId},
                                        materializer_vnode_master).





%%@doc write operation to cache for future read
-spec update(key(), #ec_payload{}) -> ok | {error, atom()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

handle_command({read, Key, Type, TxId}, _Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    Reply=internal_read(Key, Type, TxId, OpsCache, SnapshotCache),
    %riak_core_vnode:reply(Sender, Reply);
    {reply, Reply, State};
    
handle_command({multi_read, Reads, TxId}, _Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    Reply= internal_multi_read(Reads, TxId, OpsCache, SnapshotCache),    
	%riak_core_vnode:reply(Sender, Reply);
	{reply, Reply, State};
	
handle_command({update, Key, DownstreamOp}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    true = op_insert(Key,DownstreamOp, OpsCache),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
    F = fun({Key,Operation}, A) ->
                Fun(Key, Operation, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State = #state{ops_cache = OpsCache}) ->
    {Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, {Key, Operation}),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache = OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{ops_cache=OpsCache}) ->
    true = ets:delete(OpsCache),
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.



%%---------------- Internal Functions -------------------%%

internal_multi_read(Reads, TxId, OpsCache, SnapshotCache)->
	internal_multi_read([], Reads, TxId, OpsCache, SnapshotCache).

internal_multi_read(ReadResults, [], _TxId, _OpsCache, _SnapshotCache)->
	{ok, ReadResults};
internal_multi_read(ReadResults, [H|T], TxId, OpsCache, SnapshotCache)->
    case H of
        {Key, Type} ->
			lager:info("Read the following: key ~p~n, type ~p~n", [Key, Type]),
            case internal_read(Key, Type, TxId, OpsCache, SnapshotCache) of
            {ok, Value} ->
            	Value2=Type:value(Value), 
				lager:info("Got value ~p~n", [Value2]),    
				internal_multi_read(lists:append(ReadResults, [Value2]), T, TxId, OpsCache, SnapshotCache);
			{error, Reason} ->
				{error, Reason}
			end;
        WrongFormat ->
            {error, {wrong_format, WrongFormat}}
    end.


%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(term(), atom(), vectorclock:vectorclock(), tx_id() | ignore, atom() atom() ) -> {ok, term()} | {error, no_snapshot}.
internal_read(Key, Type, TxId, OpsCache, SnapshotCache) ->
	lager:info("INTERNAL READ RECEIVED THE FOLLOWING PARAMETERS ~n Key ~p~n, Type ~p~n, SnapshotTime ~p~n, TxId ~p~n, OpsCache ~p~n, SnapshotCache ~p~n",[Key, Type, snapshottime, TxId, OpsCache, SnapshotCache]), 
    case ets:lookup(SnapshotCache, Key) of
	[] ->
		SnapshotDict=orddict:new(),
		Snapshot, =ec_materializer:new(Type),
		SnapshotCommitTime = ignore,
		lager:info("created new object:  ~p~n of type ~p~n with no commit time, EMPTY SNAPSHOTDICT", [Snapshot, Type]), 
		ExistsSnapshot=false;
	[{_, {SnapshotCommitTime, Snapshot}}] ->
			lager:info("there exists a snapshot:  ~p~n commit time ~p~n", [Snapshot, SnapshotCommitTime]), 
				ExistsSnapshot=true;
    end,
	case ets:lookup(OpsCache, Key) of
		[] ->
			case ExistsSnapshot of
			false ->  
				lager:info("no operations, returned that. snapshot"),       						
				{ok, Snapshot};
			true ->
				{error, no_snapshot}
			end;
		[{_, OpsDict}] ->
			{ok, Ops}= filter_ops(OpsDict),
			case Ops of
				[] ->
					{ok, Snapshot, };
				[H|T] ->
					case ec_materializer:materialize(Type, Snapshot, [H|T]) of
					{ok, Snapshot, CommitTime} ->
						%% the following checks for the case there was no snapshots and there were operations, but none was applicable
						%% for the given snapshot_time
						case (CommitTime==ignore) of 
						true->
							{error, no_snapshot};
						false->
							snapshot_insert(Key,{CommitTime,Snapshot}, OpsDict, SnapshotCache, OpsCache),
							{ok, Snapshot}
						end;
					{error, Reason} ->
						{error, Reason}
					end
			end
	end.




%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
snapshot_insert(Key, {CommitTime,Snapshot}, SnapshotCache)->
            ets:insert(SnapshotCache, {Key, {CommitTime,Snapshot}})
    end.



%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert(term(), ec_payload(),
                   atom() atom() )-> true.
op_insert(Key,DownstreamOp, OpsCache)->
	ets:insert(OpsCache, {Key, DownstreamOp#ec_payload.commit_time, DownstreamOp}).


-ifdef(TEST).

%% @doc Testing filter_ops works in both situations, when the function receives
%%      what it expects and when it receives something in an unexpected format.
filter_ops_test() ->
	Ops=orddict:new(),
	Ops1=orddict:append(key1, [a1, a2], Ops),
	Ops2=orddict:append(key2, [b1, b2], Ops1),
	Ops3=orddict:append(key3, [c1, c2], Ops2),
	Result=filter_ops(Ops3),
	?assertEqual(Result, {ok, [[a1,a2], [b1,b2], [c1,c2]]}),
	Result1=filter_ops({some, thing}),
	?assertEqual(Result1, {error, wrong_format}),
	Result2=filter_ops([anything]),
	?assertEqual(Result2, {error, wrong_format}).   
	
%% @doc Testing belongs_to_snapshot returns true when a commit time 
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
	CommitTime1= 1,
	CommitTime2= 1,
	SnapshotClockDC1 = 5,
	SnapshotClockDC2 = 5,
	CommitTime3= 10,
	CommitTime4= 10,

	SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
	?assertEqual(true, belongs_to_snapshot({1, CommitTime1}, SnapshotVC)),
	?assertEqual(true, belongs_to_snapshot({2, CommitTime2}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot({1, CommitTime3}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot({2, CommitTime4}, SnapshotVC)).


seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dtounter,
    DC1 = 1,
    S1 = Type:new(),

    %% Insert one increment
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #ec_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     tx_id = 1
                                    },
    op_insert(Key,DownstreamOp1, OpsCache, SnapshotCache),
    io:format("after making the first write: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]),ignore,  OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    io:format("after making the first read: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    %% Insert second increment
    {ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#ec_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC1,16}]),
                      commit_time = {DC1,20},
                      tx_id=2},

    op_insert(Key,DownstreamOp2, OpsCache, SnapshotCache),
    io:format("after making the second write: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),
    io:format("after making the second read: Opscache= ~p~n SnapCache=~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).
    

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dtounter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #ec_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     tx_id = 1
                                    },
    op_insert(Key,DownstreamOp1, OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok,Op2} = Type:update(increment, b, Res1),
    DownstreamOp2 = DownstreamOp1#ec_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
                      commit_time = {DC2,20},
                      tx_id=2},

    op_insert(Key,DownstreamOp2, OpsCache, SnapshotCache),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}, {DC2,16}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dtounter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #ec_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,0}, {DC2,0}]),
                                     commit_time = {DC2, 1},
                                     tx_id = 1
                                    },
    op_insert(Key,DownstreamOp1, OpsCache, SnapshotCache),
    io:format("after inserting the first op: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2,1}, {DC1,0}]), ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    io:format("after making the first read: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:update(increment, b, S1),
    DownstreamOp2 = #ec_payload{ key = Key,
									  type = Type,
									  op_param = {merge, Op2},
									  snapshot_time=vectorclock:from_list([{DC1,0}, {DC2,0}]),
									  commit_time = {DC1, 1},
									  tx_id=2},

    op_insert(Key,DownstreamOp2, OpsCache, SnapshotCache),
    io:format("after inserting the second op: Opscache= ~p ~n SnapCache= ~p", [orddict:to_list(OpsCache), orddict:to_list(SnapshotCache)]),
									  
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
-endif.
