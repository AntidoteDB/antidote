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
-module(ec_materializer).
-include("antidote.hrl").

%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         materialize/6,
         materialize_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().


%% @doc materialize_eager: apply updates in order without any checks
-spec materialize_eager(type(), snapshot(), [ec_payload()]) -> snapshot().
materialize_eager(_, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op|Rest]) ->
   case Op of
        {merge, State} ->
            NewSnapshot = Type:merge(Snapshot, State);
        {update, DownstreamOp} ->
            {ok, NewSnapshot} = Type:update(DownstreamOp, Snapshot)
    end,
    materialize_eager(Type, NewSnapshot, Rest).


-ifdef(TEST).

materializer_ec_test()->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 1}, tx_id = 1},
    Op2 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 2}, tx_id = 2},
    Op3 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,1},1}},
                           commit_time = {1, 3}, tx_id = 3},
    Op4 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update,{{increment,2},1}},
                           commit_time = {1, 4}, tx_id = 4},

    Ops = [Op1,Op2,Op3,Op4],
    {ok, PNCounter2, CommitTime2} = materialize(crdt_pncounter,
                                      PNCounter, ignore, vectorclock:from_list([{1,3}]),
                                      Ops, ignore),
    ?assertEqual({4, {1,3}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    {ok, PNcounter3, CommitTime3} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,4}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    {ok, PNcounter4, CommitTime4} = materialize(crdt_pncounter, PNCounter, ignore,
                                   vectorclock:from_list([{1,7}]),Ops, ignore),
    ?assertEqual({6, {1,4}}, {crdt_pncounter:value(PNcounter4), CommitTime4}).

materializer_ec_concurrent_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Op1 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,2}, actor1}},
                           commit_time = {1, 1}, tx_id = 1},
    Op2 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {1, 2}, tx_id = 2},
    Op3 = #ec_payload{key = abc, type = crdt_pncounter,
                           op_param = {update, {{increment,1}, actor1}},
                           commit_time = {2, 1}, tx_id = 3},

    Ops = [Op1,Op2,Op3],
    {ok, PNCounter2, CommitTime2} = materialize(crdt_pncounter,
                                      PNCounter, ignore,
                                      vectorclock:from_list([{2,2},{1,2}]),
                                      Ops, ignore, ignore),
    ?assertEqual({4, {2,1}}, {crdt_pncounter:value(PNCounter2), CommitTime2}),
    
    
    
    Snapshot=new(crdt_pncounter),
    {ok, PNcounter3, CommitTime3} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,2}]),Ops, ignore),
    ?assertEqual({3, {1,2}}, {crdt_pncounter:value(PNcounter3), CommitTime3}),
    
    {ok, PNcounter4, CommitTime4} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{2,1}]),Ops, ignore),
    ?assertEqual({1, {2,1}}, {crdt_pncounter:value(PNcounter4), CommitTime4}),
    
    {ok, PNcounter5, CommitTime5} = materialize(crdt_pncounter, Snapshot, ignore,
                                   vectorclock:from_list([{1,1}]),Ops, ignore),
    ?assertEqual({2, {1,1}}, {crdt_pncounter:value(PNcounter5), CommitTime5}).

%% @doc Testing gcounter with empty update log
materializer_ec_noop_test() ->
    PNCounter = new(crdt_pncounter),
    ?assertEqual(0,crdt_pncounter:value(PNCounter)),
    Ops = [],
    {ok, PNCounter2, ignore} = materialize(crdt_pncounter, PNCounter, ignore,
                                vectorclock:from_list([{1,1}]),
                                Ops, ignore, ignore),
    ?assertEqual(0,crdt_pncounter:value(PNCounter2)).
    
    
    
    
is_op_in_snapshot_test()->
	OpCT1 = {dc1, 1},
	ST1 = vectorclock:from_list([{dc1, 2}]),
	ST2 = vectorclock:from_list([{dc1, 0}]),
	true = is_op_in_snapshot(OpCT1, ST1, ignore),
	false = is_op_in_snapshot(OpCT1, ST2, ignore).
    
    
-endif.
