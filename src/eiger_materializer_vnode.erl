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
-module(eiger_materializer_vnode).

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
-spec read(key(), type(), vectorclock:vectorclock(), txid()) -> {ok, term()} | {error, atom()}.
read(Key, Type, Time, TxId) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode,
                                        {read, Key, Type, Time, TxId},
                                        eiger_materializer_vnode_master).


%%@doc write operation to cache for future read
-spec update(key(), #clocksi_payload{}) -> ok | {error, atom()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        eiger_materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

handle_command({read, Key, Type, Time, TxId}, Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    _=internal_read(Sender, Key, Type, Time, TxId, OpsCache, SnapshotCache),
    {noreply, State};
   
handle_command({update, Key, Op}, _Sender,
               State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    true = op_insert_gc(Key, Op, OpsCache, SnapshotCache),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0} ,
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

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
internal_read(Sender, Key, Type, Time, TxId, OpsCache, SnapshotCache) ->
    {ExistsSnapshot, SnapshotDict, LatestSnapshot, SnapshotEvt, SnapshotTimestamp} = 
            case ets:lookup(SnapshotCache, Key) of
                [] ->
                    {false, [], clocksi_materializer:new(Type), 0, {ignore, 0}};
                [{_, Dict}] ->
                    %lager:info(user, "There is a dict ~w ~n", [Dict]),
                    case get_latest_snapshot(Dict, Time) of
                        {ok, {TEvt, CT, LSnapshot}}->
                            {true, Dict, LSnapshot, TEvt, CT};
                        {ok, no_snapshot} ->
                            {false, Dict, clocksi_materializer:new(Type), 0, {ignore, 0}}
                    end
            end,
    %lager:info(user, "Info is ~w, ~w, ~w, ~w, ~w ~n", [ExistsSnapshot, SnapshotDict, LatestSnapshot, SnapshotEvt,
    %        SnapshotTimestamp]),
	case ets:lookup(OpsCache, Key) of
		[] ->
			case ExistsSnapshot of
			    true ->        						
                    %lager:info(user, "Snapshot is ~w~n", [LatestSnapshot]),
				    riak_core_vnode:reply(Sender, {ok, LatestSnapshot, SnapshotEvt, SnapshotTimestamp}),
				    {ok, LatestSnapshot};
			    false ->
                    %lager:info(user, "no Snapshot ~w ~n", [ExistsSnapshot]),
				    riak_core_vnode:reply(Sender, {ok, clocksi_materializer:new(Type), 0, {ignore, 0}}),
				    {error, no_snapshot}
			end;
		[{_, OpsDict}] ->
			Ops = OpsDict,
			case Ops of
				[] ->
                    %lager:info("No op, snapshot is ~w", [LatestSnapshot]),
					riak_core_vnode:reply(Sender, {ok, LatestSnapshot, SnapshotEvt, SnapshotTimestamp}),
					{ok, LatestSnapshot};
				[_H|_T] ->
                    %lager:info(user, "Trying to apply ops ~w ~n", [Ops]),
                    %lager:info("Before applying ops"),
					case apply_ops_to_snapshot(Type, LatestSnapshot, SnapshotEvt, SnapshotTimestamp, Time, Ops, TxId) of
					    {ok, Snapshot, Evt, CommitTime} ->
			                case (Sender /= ignore) of
					            true ->
							        riak_core_vnode:reply(Sender, {ok, Snapshot, Evt, CommitTime});
							    false ->
                                    ok
					        end,
						    SnapshotDict1=descend_insert(Evt, {CommitTime, Snapshot}, SnapshotDict),
						    snapshot_insert_gc(Key,SnapshotDict1, OpsDict, SnapshotCache, OpsCache),
						    {ok, Snapshot};
				        {error, Reason} ->
				            riak_core_vnode:reply(Sender, {error, Reason}),
				            {error, Reason}
					end
            end
	end.


apply_ops_to_snapshot(Type, Snapshot, SnapshotEvt, SnapshotTimestamp, Time, Ops, TxId) ->
    %Ops = [H | T],
    %lager:info("Ops: ~p, SnapshotTime ~w, evt ~w, time is ~w", [Ops, SnapshotTimestamp, SnapshotEvt, Time]),
    case Time of
        latest ->
            %lager:info("Eager Materialize!!"),
            V = eiger_materializer:materialize_eager(Type, Snapshot, SnapshotEvt, SnapshotTimestamp, Ops),
            %lager:info("Value is ~w", [V]),
            V;
        _ ->
            %lager:info("Materialize!!"),
            eiger_materializer:materialize(Type, Snapshot, Time, Ops, TxId, SnapshotEvt, SnapshotTimestamp)
    end.
    
%% @doc Obtains, from an orddict of Snapshots, the latest snapshot that can be included in
%% a snapshot identified by SnapshotTime
-spec get_latest_snapshot([], SnapshotTime::vectorclock:vectorclock())
                         -> {ok, term()} | {ok, no_snapshot}| {error, wrong_format, term()}.
get_latest_snapshot(SnapshotDict, Time) ->
    case first_belongs_to(Time, SnapshotDict) of
        error ->
            {ok, no_snapshot};
        V ->
            {Evt, {CommitTime, Snapshot}} = V,
            {ok, {Evt, CommitTime, Snapshot}}
    end.

%% @doc Check whether a Key's operation or stored snapshot is included
%%		in a snapshot defined by a vector clock
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this Snapshot at DC
%%             SnapshotTime = vector clock
%%      Outptut: true or false
-spec belongs_to_snapshot(Evt::non_neg_integer(),
                          SnapshotTime::non_neg_integer()) -> boolean().
belongs_to_snapshot(Evt, Time) ->
    case Time of
        latest ->
            true;
        _ ->
            Evt =< Time
    end.

%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(Key::term(), SnapshotDict::[],
                         OpsDict::[], atom() , atom() ) -> true.
snapshot_insert_gc(Key, SnapshotDict, OpsDict, SnapshotCache, OpsCache)->
    case (length(SnapshotDict))>=?SNAPSHOT_THRESHOLD of
        true ->
            PrunedSnapshots=lists:sublist(SnapshotDict, 1+?SNAPSHOT_THRESHOLD-?SNAPSHOT_MIN, ?SNAPSHOT_MIN),
            FirstOp=lists:nth(1, PrunedSnapshots),
            {CommitTime, _S} = FirstOp,
            PrunedOps=prune_ops(OpsDict, CommitTime),
            ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
            ets:insert(OpsCache, {Key, PrunedOps});
        false ->
            ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops([], {Dc::term(),CommitTime::non_neg_integer()})-> [].
prune_ops(OpsDict, Threshold)->
    lists:filter(fun(_Key, Value) ->
                           (belongs_to_snapshot(Threshold, Value#clocksi_payload.snapshot_time)) end, OpsDict).


%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(term(), clocksi_payload(),
                   atom() , atom() )-> true.
op_insert_gc(Key, Op, OpsCache, SnapshotCache)->
    OpsDict = case ets:lookup(OpsCache, Key) of
                  []->
                      [];
                  [{_, Dict}]->
                      Dict
              end,
    case (length(OpsDict))>=?OPS_THRESHOLD of
        true ->
            Type=Op#clocksi_payload.type,
            SnapshotTime=Op#clocksi_payload.snapshot_time,
            {_, _} = internal_read(ignore, Key, Type, SnapshotTime, ignore, OpsCache, SnapshotCache),
            %OpsDict1=orddict:append(Op#clocksi_payload.commit_time, Op, OpsDict),
            OpsDict1=descend_insert(Op#clocksi_payload.evt, Op, OpsDict),
            %lager:info("OpDict is ~p, OpDict1 is ~p ~n", [OpsDict, OpsDict1]),
            ets:insert(OpsCache, {Key, OpsDict1});
        false ->
            %OpsDict1=orddict:append(Op#clocksi_payload.commit_time, Op, OpsDict),
            OpsDict1=descend_insert(Op#clocksi_payload.evt, Op, OpsDict),
            %lager:info("OpDict is ~p, OpDict1 is ~p ~n", [OpsDict, OpsDict1]),
            %lager:info("OpsCache ~w: Inserting key ~w, op ~p", [OpsCache, Key, DownstreamOp]),
            ets:insert(OpsCache, {Key, OpsDict1})
    end.

descend_insert(Key, New, [{K,_}|_]=Dict) when Key > K ->
    [{Key,New}|Dict];
descend_insert(Key, New, [{K,_}=E|Dict]) when Key < K ->
    [E|descend_insert(Key, New, Dict)];
descend_insert(Key, New, [{_K,_Old}|Dict]) ->        %Key == K
    [{Key,New}|Dict];
descend_insert(Key, New, []) -> [{Key,New}].

first_belongs_to(_, []) ->
    error;
first_belongs_to(Threshold, [{Key, Value}|_]) when Threshold >= Key ->
    {Key, Value};
first_belongs_to(Threshold, [{Key, _}|R]) when Threshold < Key ->
    first_belongs_to(Threshold, R).


-ifdef(TEST).

%% @doc Testing belongs_to_snapshot returns true when a commit time 
%% is smaller than a snapshot time
belongs_to_snapshot_test()->

	SnapshotVC= 10,
	?assertEqual(true, belongs_to_snapshot(1, SnapshotVC)),
	?assertEqual(true, belongs_to_snapshot(2, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot(11, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot(12, SnapshotVC)).

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_lwwreg,
    DC1 = 1,

    %% Insert one increment
    %{ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {Key, Type, {{assign, 1}, 1}},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     evt=15,
                                     txid = 1
                                    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
    %lager:info("after making the first write: Opscache= ~p~n SnapCache=~p", [OpsCache, SnapshotCache]),
    %internal_read(Sender, Key, Type, Time, TxId, OpsCache, SnapshotCache),
    {ok, Res1} = internal_read(ignore, Key, Type, 16, ignore,  OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    %lager:info("after making the first read: Opscache= ~p~n SnapCache=~p", [OpsCache, SnapshotCache]),
   %% Insert second increment
    %{ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {Key, Type, {{assign,2}, 2}},
                      snapshot_time=vectorclock:from_list([{DC1,16}]),
                      commit_time = {DC1,20},
                      evt=20,
                      txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
    %lager:info("after making the second write: Opscache= ~p~n SnapCache=~p ~n", [OpsCache, SnapshotCache]),
    {ok, Res2} = internal_read(ignore, Key, Type, 21, ignore, OpsCache, SnapshotCache),
    %lager:info("Res2 is ~p ~n", [Res2]),
    ?assertEqual(2, Type:value(Res2)),
    %lager:info("after making the second read: Opscache= ~p~n SnapCache=~p ~n", [OpsCache, SnapshotCache]),

    %% Read old version
    {ok, ReadOld} = internal_read(ignore, Key, Type, 17, ignore, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).
   

%multipledc_write_test() ->
%    OpsCache = ets:new(ops_cache, [set]),
%    SnapshotCache = ets:new(snapshot_cache, [set]),
%    Key = mycount,
%    Type = riak_dt_gcounter,
%    DC1 = 1,
%    DC2 = 2,
%    S1 = Type:new(),

    %% Insert one increment in DC1
%    {ok,Op1} = Type:update(increment, a, S1),
%    DownstreamOp1 = #clocksi_payload{key = Key,
%                                     type = Type,
%                                     op_param = {merge, Op1},
%                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
%                                     commit_time = {DC1, 15},
%                                     txid = 1
%                                    },
%    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache),
%    {ok, Res1} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}]), ignore, OpsCache, SnapshotCache),
%    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
%    {ok,Op2} = Type:update(increment, b, Res1),
%    DownstreamOp2 = DownstreamOp1#clocksi_payload{
%                      op_param = {merge, Op2},
%                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
%                      commit_time = {DC2,20},
%                      txid=2},

%    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache),
%    {ok, Res2} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore, OpsCache, SnapshotCache),
%    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
%    {ok, ReadOld} = internal_read(ignore, Key, Type, vectorclock:from_list([{DC1,16}, {DC2,16}]), ignore, OpsCache, SnapshotCache),
%    ?assertEqual(1, Type:value(ReadOld)).
-endif.
