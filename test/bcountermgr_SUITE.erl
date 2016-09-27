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
         test_dec_multi_success0/1,
         test_dec_multi_success1/1
         %test_provisioning/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(TYPE, antidote_crdt_bcounter).
-define(ANTIDOTE_BUCKET, antidote_bucket).
-define(RETRY_COUNT, 5).


init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = lists:flatten(Clusters),

    %Ensure that write operations are certified
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_cert, true]) end, Nodes),

    %Check that indeed transactions certification is turned on
    {ok, true} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_cert]),

    [{nodes, Nodes},
     {clusters, Clusters}|Config].


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
         test_dec_multi_success0,
         test_dec_multi_success1
         %,
         %test_provisioning
        ].

%% Tests creating a new `bcounter()'.
new_bcounter_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Key = bcounter1_mgr,
    {ok, Obj} = read(Node1, Key),
    ?assertEqual(0, ?TYPE:permissions(Obj)).

test_dec_success(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter2_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node1, decrement, Key, 4, Actor),
    check_read(Node2, Key, 6, CommitTime).

test_dec_fail(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter3_mgr,
    {ok, CommitTime} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, {_, [_Obj], _}} = read_si(Node2, Key, CommitTime),
    Result0 = execute_op_success(Node2, decrement, Key, 5, Actor, 0),
    ?assertEqual({error, no_permissions}, Result0).

test_dec_multi_success0(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter4_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node2, decrement, Key, 5, Actor),
    check_read(Node1, Key, 5, CommitTime).

test_dec_multi_success1(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Actor = dc,
    Key = bcounter5_mgr,
    {ok, _} = execute_op(Node1, increment, Key, 10, Actor),
    {ok, _} = execute_op(Node2, decrement, Key, 5, Actor),
    {error, no_permissions} = execute_op(Node1, decrement, Key, 6, Actor),
    {ok, Obj} = read(Node1, Key),
    ?assertEqual(5, ?TYPE:permissions(Obj)).

%test_provisioning(Config) ->
%    Clusters = proplists:get_value(clusters, Config),
%    [Node1, Node2 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
%    Actor = dc,
%    Key = bcounter6_mgr,
%    {ok, CT} = execute_op(Node1, increment, Key, 10, Actor),
%    {ok,{_,_,_}} = read_si(Node2, Key,CT),
%    ok = rpc:call(Node2, bcounter_mgr, request_transfer, [{Key,?ANTIDOTE_BUCKET}, 5]),
%    timer:sleep(1000),
%    {ok,{_,[Obj2],_}} = read_si(Node2, Key,CT),
%    {P,D} = Obj2,
%    Res = orddict:fold(fun({_,SomeId}, _Amount, Check) ->
%                               (?TYPE:localPermissions(SomeId,Obj2) == 5) and Check end, true , P),
%    ?assertEqual(true, Res).

execute_op(Node, Op, Key, Amount, Actor) ->
    execute_op_success(Node, Op, Key, Amount, Actor, ?RETRY_COUNT).

%%Auxiliary functions.
execute_op_success(Node, Op, Key, Amount, Actor, Try) ->
    Bound_object = {Key, ?TYPE, ?ANTIDOTE_BUCKET},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    Result = rpc:call(Node, antidote, update_objects, [[{Bound_object, Op, {Amount, Actor}}], TxId]),
    case Result of
        ok ->
            rpc:call(Node, antidote, commit_transaction, [TxId]);
        Error when Try == 0 ->
            rpc:call(Node, antidote, abort_transaction, [TxId]),
            Error;
        _ ->
            rpc:call(Node, antidote, abort_transaction, [TxId]),
            timer:sleep(1000),
            execute_op_success(Node, Op, Key, Amount, Actor, Try -1)
    end.

read(Node, Key) ->
    {ok, {_, [Obj], _}} = rpc:call(Node, antidote, clocksi_read, [Key, ?TYPE]),
    {ok, Obj}.

read_si(Node, Key, CommitTime) ->
    rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, ?TYPE]).

check_read(Node, Key, Expected, CommitTime) ->
    {ok, {_, [Obj], _}} = read_si(Node, Key, CommitTime),
    ?assertEqual(Expected, ?TYPE:permissions(Obj)).

