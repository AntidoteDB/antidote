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
-module(eiger_materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         materialize/7,
         materialize_eager/5]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().

%% @doc Applies the operation of a list to a CRDT. Only the
%%      operations with smaller timestamp than the specified
%%      are considered. Newer operations are discarded.
%%      Input:
%%      Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotTime: Threshold for the operations to be applied.
%%      Ops: The list of operations to apply in causal order
%%      Output: The CRDT after appliying the operations and its commit
%%      time taken from the last operation that was applied to the snapshot.
-spec materialize(type(), 
					  snapshot(),
                      snapshot_time(),
                      [clocksi_payload()], 
                      txid(), non_neg_integer(), 
                      LastOpCommitTime::{dcid(),CommitTime::non_neg_integer()} | ignore) ->
                             {ok,snapshot(), {dcid(),CommitTime::non_neg_integer()} | ignore} | {error, term()}.
materialize(Type, Snapshot, SnapshotTime, Ops, TxId, LastEvt, LastOpCommitTime) ->
    {ok, OpsToApply} = materialize(Type, SnapshotTime, Ops, TxId, LastEvt, []),
    lists:foldl(fun(Op, {ok, Acc1, _, _}) ->
                      {_, _, {Param, Actor}} = Op#clocksi_payload.op_param,
                      {assign, V} = Param,
                      Evt = Op#clocksi_payload.evt,
                      {ok, NewSnapshot} = Type:update({assign, V, Evt}, Actor, Acc1),
                      {ok, NewSnapshot, Op#clocksi_payload.evt, Op#clocksi_payload.commit_time}
                  end, {ok, Snapshot, LastEvt, LastOpCommitTime}, OpsToApply).

materialize(_, _SnapshotTime, [], _TxId, _LastEvt, OpsToApply) ->
    %lager:info("In mat.. No op!!"),
    {ok, OpsToApply};
materialize(Type, SnapshotTime, [{OpEvt, Op}|Rest], TxId, LastEvt, OpsToApply) ->
    case Type == Op#clocksi_payload.type of
        true ->
            %lager:info("Type is ~w", [Type]),
            %OpEvt=Op#clocksi_payload.evt,
            %lager:info("OpCommitTime is ~w, SnapshotTime is ~w", [OpCommitTime, Time]),
            case has_applied_op(OpEvt, LastEvt) of
                true ->
                    {ok, OpsToApply};
                    %{ok, Snapshot, LastEvt, LastOpCommitTime};
                false ->
                    case (op_below_timestamp(OpEvt, SnapshotTime)
                        or (TxId == Op#clocksi_payload.txid)) of
                        true ->
                            materialize(Type, SnapshotTime, Rest, TxId, OpEvt, [Op|OpsToApply]);
                        false ->
                            materialize(Type, SnapshotTime, Rest, TxId, LastEvt, OpsToApply)
                    end
            end;
        false -> %% Op is not for this {Key, Type}
            materialize(Type, SnapshotTime, Rest, TxId, LastEvt, OpsToApply)
    end.

%% @doc Check whether an udpate is included in a snapshot and also
%%		if that update is newer than a snapshot's commit time
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%			   SnapshotCommitTime = commit time of that snapshot.
%%      Outptut: true or false
-spec op_below_timestamp(non_neg_integer(), non_neg_integer()) -> boolean().
op_below_timestamp(OpEvt, Time) ->
    OpEvt =< Time.

-spec has_applied_op(non_neg_integer(), non_neg_integer()) -> boolean().
has_applied_op(OpEvt, LastEvt) ->
    OpEvt =< LastEvt.

%% @doc materialize_eager: apply updates in order without any checks
-spec materialize_eager(type(), snapshot(), non_neg_integer(), {term(), non_neg_integer()}, 
            [clocksi_payload()]) -> snapshot().
materialize_eager(Type, Snapshot, SnapshotEvt, SnapshotTimestamp, Ops) ->
    %lager:info("Materializer eager! Snapshot is ~p, Evt is ~p, Timestamp is ~p, Ops are ~p", [Snapshot, SnapshotEvt, SnapshotTimestamp, Ops]),
    materialize_eager(Type, Snapshot, SnapshotEvt, SnapshotEvt, SnapshotTimestamp, Ops).

-spec materialize_eager(type(), snapshot(), non_neg_integer(),
            non_neg_integer(), {term(), non_neg_integer()}, [clocksi_payload()]) -> snapshot().
materialize_eager(Type, Snapshot, SnapshotEvt, OldEvt, OldTimestamp, Ops) ->
    OpsToApply = lists:foldl(fun({OpEvt, Op}, Acc) -> 
    %                                lager:info("SnapshotEvt is ~w, OpEvt is ~w", [SnapshotEvt, OpEvt]),
                                    case OpEvt > SnapshotEvt of
                                    true -> [Op|Acc]; false -> Acc end end, [], Ops), 
    lager:info("Snapshot is ~p, Ops to apply are ~p", [Snapshot, OpsToApply]),
    lists:foldl(fun(Op, {ok, Acc1, _, _}) ->
                    {_, _, {Param, Actor}} = Op#clocksi_payload.op_param,
                    {assign, V} = Param,
                    Evt = Op#clocksi_payload.evt,
                    {ok, NewSnapshot} = Type:update({assign, V, Evt}, Actor, Acc1),
                    %lager:info("Applying ~p to ~p, new value is ~p", [Param, Acc1, NewSnapshot]),
                    {ok, NewSnapshot, Evt, Op#clocksi_payload.commit_time}
                end, {ok, Snapshot, OldEvt, OldTimestamp}, OpsToApply).



-ifdef(TEST).

materializer_clocksi_test()->
    PNCounter = new(riak_dt_lwwreg),
    ?assertEqual(<<>>,riak_dt_lwwreg:value(PNCounter)),
    Op1 = #clocksi_payload{key = abc, type = riak_dt_lwwreg,
                           op_param = {abc, riak_dt_lwwreg, {{assign,2},1}},
                           evt=1,
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = riak_dt_lwwreg,
                           op_param = {abc, riak_dt_lwwreg, {{assign,1},1}},
                           evt=3,
                           commit_time = {2, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = riak_dt_lwwreg,
                           op_param = {abc, riak_dt_lwwreg, {{assign,3},1}},
                           evt=5,
                           commit_time = {2, 4}, txid = 3},
    Op4 = #clocksi_payload{key = abc, type = riak_dt_lwwreg,
                           op_param = {abc, riak_dt_lwwreg, {{assign,4},1}},
                           evt=6,
                           commit_time = {1, 6}, txid = 4},

    Ops = [{6,Op4},{5,Op3},{3,Op2},{1,Op1}],
    {ok, PNCounter2, Evt2, CommitTime2} = materialize(riak_dt_lwwreg,
                                      PNCounter, 4,
                                      Ops, ignore, 0, ignore),
    ?assertEqual({1, 3, {2,2}}, {riak_dt_lwwreg:value(PNCounter2), Evt2, CommitTime2}),
    {ok, PNcounter3, Evt3, CommitTime3} = materialize(riak_dt_lwwreg, PNCounter,
                                   5, Ops, ignore, 0, ignore),
    ?assertEqual({3, 5, {2,4}}, {riak_dt_lwwreg:value(PNcounter3), Evt3, CommitTime3}),
    {ok, PNcounter4, Evt4, CommitTime4} = materialize(riak_dt_lwwreg, PNCounter, 
                                   7, Ops, ignore, 0, ignore),
    ?assertEqual({4, 6, {1,6}}, {riak_dt_lwwreg:value(PNcounter4), Evt4, CommitTime4}).

%% @doc Testing gcounter with empty update log
materializer_clocksi_noop_test() ->
    PNCounter = new(riak_dt_lwwreg),
    ?assertEqual(<<>>,riak_dt_lwwreg:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, 0, ignore} = materialize(riak_dt_lwwreg, PNCounter, 1, Ops,
                                ignore, 0,  ignore),
    ?assertEqual(<<>>,riak_dt_lwwreg:value(PNCounter2)).
    
    
    
    
%is_op_in_snapshot_test()->
%	OpCT1 = {dc1, 1},
%	ST1 = vectorclock:from_list([{dc1, 2}]),
%	ST2 = vectorclock:from_list([{dc1, 0}]),
%	true = is_op_in_snapshot(OpCT1, ST1, ignore),
%	false = is_op_in_snapshot(OpCT1, ST2, ignore).
    
    
-endif.
