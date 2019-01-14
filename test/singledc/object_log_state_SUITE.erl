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

-module(object_log_state_SUITE).

-include("../../include/antidote.hrl").

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([object_log_state_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TBUCKET, test_utils:bucket(object_log_state_bucket)).


init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [object_log_state_test].

object_log_state_test(Config) ->
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_set_aw,
    Key = object_log_state_test,
    Bucket = ?TBUCKET,
    BoundObject = {Key, Type, Bucket},

    CommitTime = add_set(Node, BoundObject, lists:seq(1, 15), vectorclock:new()),

    %% Check the read is 15
    {ok, [Val], _CT} = rpc:call(Node, antidote, read_objects, [CommitTime, [], [BoundObject]]),
    ?assertEqual(lists:seq(1, 15), Val),

    %% Get the object state
    {ok, [ReadResult1], _CT2} = rpc:call(Node,
                      antidote, get_objects, [CommitTime, [], [BoundObject]]),
    ?assertEqual(ok, check_orset_state(lists:seq(1, 15), ReadResult1)),

    CommitTime2 = add_set(Node, BoundObject, lists:seq(16, 30), CommitTime),

    %% Check the read is 30
    {ok, [Val2], _CT3} = rpc:call(Node, antidote, read_objects, [CommitTime2, [], [BoundObject]]),
    ?assertEqual(lists:seq(1, 30), Val2),

    {ok, [LogOps]} = rpc:call(Node,
                  antidote, get_log_operations, [[{BoundObject, CommitTime}]]),

    ?assertEqual(ok, check_orset_ops(lists:seq(16, 30), LogOps, {Key, Bucket})).


check_orset_ops([], [], _KeyBucket) ->
    ok;
check_orset_ops([Val|Rest1],
        [{_Id, #clocksi_payload{key = KeyBucket, type = antidote_crdt_set_aw, op_param = [{Val, _Binary, []}]}}
         | Rest2],
        KeyBucket) ->
    check_orset_ops(Rest1, Rest2, KeyBucket).

check_orset_state([], []) ->
    ok;
check_orset_state([Val|Rest1], [{Val, [Binary]}|Rest2]) when is_binary(Binary) ->
    check_orset_state(Rest1, Rest2).

%% Auxiliary method to add a list of items to a set
add_set(_FirstNode, _BoundObject, [], Commit) ->
    Commit;
add_set(FirstNode, Object, [First|Rest], PrevCommit) ->
    Update = {Object, add, First},
    ReadResult = rpc:call(FirstNode, antidote, read_objects, [ignore, [], [Object]]),
    ?assertMatch({ok, _, _}, ReadResult),
    {ok, Commit} = rpc:call(FirstNode, antidote, update_objects, [ignore, [], [Update]]),
    add_set(FirstNode, Object, Rest, vectorclock:max([PrevCommit, Commit])).
