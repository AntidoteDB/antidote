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
-module(bcountermgr_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([new_bcounter_test/1,
         test_dec_success/1,
         test_dec_fail/1,
         test_dec_multi_success/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(TYPE, antidote_crdt_bcounter).


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),

    [{clusters, Clusters}|Config].


end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [
         new_bcounter_test,
         test_dec_success,
         test_dec_fail,
         test_dec_multi_success
        ].

%% Tests creating a new `bcounter()'.
new_bcounter_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = bcounter1,
    Result = read(Node1, Key),
    {ok, {_, [Obj], _CommitTime}} = Result,
    ?assertEqual(0, ?TYPE:permissions(Obj)).

test_dec_success(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter2,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node1, decrement, Key, 4, Actor),
    check_read(Node2, Key, 6, CommitTime).

test_dec_fail(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter3,
    {ok, CommitTime} = execute_op(Node1, increment, Key, 10, Actor),
    _ForcePropagation = read(Node2, Key, CommitTime),
    Result = execute_op(Node2, decrement, Key, 5, Actor),
    ?assertEqual({error, no_permissions}, Result).

test_dec_multi_success(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter4,
    {ok, CommitTime} = execute_op(Node1, increment, Key, 10, Actor),
    _ForcePropagation = read(Node2, Key, CommitTime),
    _Result = execute_op(Node2, decrement, Key, 5, Actor),
    timer:sleep(1000),
    {ok, _}  = execute_op(Node2, decrement, Key, 5, Actor),
    check_read(Node2, Key, 5, CommitTime).

%%Auxiliary functions.
execute_op(Node, Op, Key, Amount, Actor) ->
    Result = rpc:call(Node, antidote, append,
                      [Key, ?TYPE, {Op, {Amount,Actor}}]),
    case Result of
        {ok, {_,_,CommitTime}} -> {ok, CommitTime};
        Error -> Error
    end.

read(Node, Key) ->
    rpc:call(Node, antidote, clocksi_read, [Key, ?TYPE]).

read(Node, Key, CommitTime) ->
    rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, ?TYPE]).

check_read(Node, Key, Expected, CommitTime) ->
    {ok, {_, [Obj], _}} = read(Node, Key, CommitTime),
    ?assertEqual(Expected, ?TYPE:permissions(Obj)).

