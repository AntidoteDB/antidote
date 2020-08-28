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
-module(lock_mgr_SUITE).


%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([simple_transaction_tests_with_locks/1,
    locks_in_sequence_check/1,
    lock_acquisition_test/1,
    get_lock_owned_by_other_dc_1/1,
    get_lock_owned_by_other_dc_2/1,
    multi_value_register_test/1,
    asynchronous_test_1/1,
    asynchronous_test_2/1,
    asynchronous_test_3/1,
    asynchronous_test_4/1,
    asynchronous_test_5/1,
    a_lot_of_locks_per_transaction_1/1,
    a_lot_of_locks_per_transaction_2/1,
    cluster_failure_test_1/1,
    cluster_failure_test_2/1
]).
%% test helper funktions
-export([
    asynchronous_test_helper/8,
    asynchronous_test_helper2/8
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include("../../include/antidote.hrl").
-include("../../include/inter_dc_repl.hrl").





init_per_suite(InitialConfig) ->
    Config = test_utils:init_multi_dc(?MODULE, InitialConfig),
    Nodes = proplists:get_value(nodes, Config),
    Clusters = proplists:get_value(clusters, Config),

    %Ensure that the clocksi protocol is used
    test_utils:pmap(fun(Node) ->
        rpc:call(Node, application, set_env,
        [antidote, txn_prot, clocksi]) end, Nodes),

    %Check that indeed clocksi is running
    {ok, clocksi} = rpc:call(hd(hd(Clusters)), application, get_env, [antidote, txn_prot]),

    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    simple_transaction_tests_with_locks,
    locks_in_sequence_check,
    lock_acquisition_test,
    get_lock_owned_by_other_dc_2,
    multi_value_register_test,
    asynchronous_test_1,
    asynchronous_test_2,
    asynchronous_test_3,
    asynchronous_test_4,
    asynchronous_test_5,
    a_lot_of_locks_per_transaction_1,
    a_lot_of_locks_per_transaction_2,
    cluster_failure_test_1
%%    cluster_failure_test_2 % sometimes fails, probably because read servers are not restarted properly
].

%% Checks if a transaction on the leading(may create new locks) node can aquire never used
%% locks, do some updates and then releases them when the transaction is committed
simple_transaction_tests_with_locks(Config) ->
    [Node | _Nodes] = proplists:get_value(nodes, Config),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [lock1, lock2, lock3, lock4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
        {Key, Type, Bucket}
    end, Keys
    ),
    Updates = lists:map(fun({Object, IncVal}) ->
        {Object, increment, IncVal}
    end, lists:zip(Objects, IncValues)),
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    % read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    % read objects all at once
    {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    % checks if the updates were successful
    ?assertEqual([1, 2, 3, 4], Res).

%% test if after a transaction released some lock another transaction on the same node can aquire them
locks_in_sequence_check(Config) ->
    [Node | _Nodes] = proplists:get_value(nodes, Config),
    Keys = [lock5, lock6, lock7, lock8],
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    % {error,{error,[{_TxId,Missing_Keys}]}} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
        {Key, Type, Bucket}
    end, Keys
    ),
    Updates = lists:map(fun({Object, IncVal}) ->
        {Object, increment, IncVal}
    end, lists:zip(Objects, IncValues)),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    %% read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    %% read objects all at once
    {ok, _Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ok.

%% Tests if lock acquisition in multiple dcs of the same locks can propperly acquire
%% them and the lock_mgr manages the lock data as intendet.
lock_acquisition_test(Config) ->
    [Node | _Nodes] = proplists:get_value(nodes, Config),
    Keys = [lock21, lock22, lock23, lock24],

    {ok, TxId1} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {ok, _Clock1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {ok, _Clock2} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    {ok, TxId3} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {ok, _Clock3} = rpc:call(Node, antidote, commit_transaction, [TxId3]),
    ok.

%% Test if sequential lock acquisition works as intendet
get_lock_owned_by_other_dc_1(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Keys = [lock31, lock32, lock33, lock34],

    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {ok, _Clock1} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    {ok, TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,[lock31]}]]),
    {ok, _Clock2} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
    {ok, TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,[lock32,lock33,lock34]}]]),
    {ok, _Clock3} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
    ok.
%% Test if sequential lock acquisition works as intendet
get_lock_owned_by_other_dc_2(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
     Keys = [lock41, lock42, lock43, lock44],
    Lock_request_order = [Node3, Node1, Node1, Node2, Node3, Node2, Node3, Node1],
    helper_do_lock_requests(Lock_request_order, Keys).
%% Helper function for get_lock_owned_by_other_dc_2
helper_do_lock_requests([],_)-> ok;
helper_do_lock_requests([Current_Node | Remaining_Nodes],Keys)->
    case rpc:call(Current_Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok, TxId1} ->
            {ok, _Clock1} = rpc:call(Current_Node, antidote, commit_transaction, [TxId1]),
            helper_do_lock_requests(Remaining_Nodes, Keys);
        {error,{error,_Missing_Locks}} ->
            helper_do_lock_requests([Current_Node|Remaining_Nodes], Keys)
    end.
%% Tests if a multi value register allways has the correct value when it is updated on multiple dcs using locks
multi_value_register_test(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],

    Key = multi_value_register,
    Bound_object = {Key, antidote_crdt_register_mv, antidote_bucket},
    Updates_List=[{[[]],Node1,<<"n1">>},
        {<<"n1">>,Node2,<<"x2">>},
        {<<"x2">>,Node2,<<"x3">>},
        {<<"x3">>,Node3,<<"y4">>},
        {<<"y4">>,Node1,<<"n5">>},
        {<<"n5">>,Node1,<<"n6">>},
        {<<"n6">>,Node3,<<"y7">>},
        {<<"y7">>,Node1,<<"n8">>},
        {<<"n8">>,Node2,<<"x9">>},
        {<<"x9">>,Node3,<<"y10">>}],
    helper_multi_value_register_test(Updates_List,[Key],Bound_object).
%% helper functin for multi_value_register_test
helper_multi_value_register_test([],_,_)-> ok;
helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes],Keys,Object)->
    case rpc:call(Current_Node, cure, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok, TxId1} ->
            case Value2 of
                <<"n1">> ->
                    ok = rpc:call(Current_Node, cure, update_objects, [[{Object,assign,Value2}],TxId1]),
                    {ok, _Clock1} = rpc:call(Current_Node, cure, commit_transaction, [TxId1]),
                    helper_multi_value_register_test(Remaining_Nodes, Keys, Object);
                _ ->
                    {ok, [[Read_Val]|[]]} = rpc:call(Current_Node, cure, read_objects, [[Object], TxId1]),
                    ok = rpc:call(Current_Node, cure, update_objects, [[{Object,assign,Value2}],TxId1]),
                    {ok, _Clock1} = rpc:call(Current_Node, cure, commit_transaction, [TxId1]),
                    ?assertEqual(Value1,Read_Val),
                    helper_multi_value_register_test(Remaining_Nodes, Keys, Object)
            end;
        {error,{error,_Missing_Locks}} ->
            helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes], Keys,Object)
    end.


%% Let 3 processes asynchronously increment the same counter each 100times while using a lock to restrict the access.
%% 30 ms delay between increments
asynchronous_test_1(Config) ->
    N = 100,
Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Keys = [asynchronous_test_key_1],
    Object = {asynchronous_test_key_1, antidote_crdt_counter_pn, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node1,Keys,Object,N,[],self(),30,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node2,Keys,Object,N,[],self(),30,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node3,Keys,Object,N,[],self(),30,3]),

    receive
        {done,Node1,1,_Clocks1} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 1)")
    end,
    receive
        {done,Node2,2,_Clocks2} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 2)")
    end,
    receive
        {done,Node3,3,_Clocks3} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 3)")
    end,


    {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_11 = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
    {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),

    {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_22 = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
    {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),

    {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_33 = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
    {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
    {ok, [Res1]} = Res_11,
    {ok, [Res2]} = Res_22,
    {ok, [Res3]} = Res_33,
    ?assertEqual(3*N,Res1),
    ?assertEqual(3*N,Res2),
    ?assertEqual(3*N,Res3).



%% Let 3 processes asynchronously increment the same counter 100times while using a lock to restrict the access.
%% 0 ms delay between increments
asynchronous_test_2(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Keys = [asynchronous_test_key_2],
    Object = {asynchronous_test_key_2, antidote_crdt_counter_pn, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node1,Keys,Object,100,[],self(),0,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node2,Keys,Object,100,[],self(),0,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node3,Keys,Object,100,[],self(),0,3]),
    receive
        {done,Node1,1,_Clocks1} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 1)")
    end,
    receive
        {done,Node2,2,_Clocks2} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 2)")
    end,
    receive
        {done,Node3,3,_Clocks3} -> ok
    after 600000 -> error("The test case took too long and was timed out (Node 3)")
    end,


    {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_11 = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
    {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_22 = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
    {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
    {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Res_33 = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
    {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
    {ok, [Res1]} = Res_11,
    {ok, [Res2]} = Res_22,
    {ok, [Res3]} = Res_33,
    ?assertEqual(300,Res1),
    ?assertEqual(300,Res2),
    ?assertEqual(300,Res3).

asynchronous_test_helper(Node,_,_,0,Clocks,Caller,_,Id)->
    Caller ! {done,Node,Id,Clocks};

asynchronous_test_helper(Node,Keys,Object,Increments,Clocks,Caller,Delay,Id)->
    ct:pal("Starting transaction ~p ~p", [Node, Increments]),
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok, TxId1} ->
            ok = rpc:call(Node, antidote, update_objects,[[{Object, increment, 1}],TxId1]),
            {ok, Clock1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
            ct:pal("Committed transaction ~p ~p", [Node, Increments]),
            timer:sleep(Delay),
            asynchronous_test_helper(Node, Keys, Object,Increments-1,[Clock1|Clocks],Caller,Delay,Id);
        Error ->
            throw({failed_test, Error})
    end.


%% Let 3 processes asynchronously increment the same multy value register 100times while using a lock to restrict the access.
%% 30 ms delay between increments
asynchronous_test_3(Config) ->
Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Keys = [asynchronous_test_key_3],
    Object = {asynchronous_test_key_3, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys,Object,100,[],self(),30,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys,Object,100,[],self(),30,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys,Object,100,[],self(),30,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                        {done,Node3,3,_Clocks3} ->

                            {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_11 = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                            {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                            {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_22 = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                            {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_33 = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                            {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
                            {ok, [[Res1]|[]]} = Res_11,
                            {ok, [[Res2]|[]]} = Res_22,
                            {ok, [[Res3]|[]]} = Res_33,
                            ?assertEqual(300,Res1),
                            ?assertEqual(300,Res2),
                            ?assertEqual(300,Res3)
                    after 300000 -> ?assertError("The test case took too long and was timed out",false)
                    end
            after 300000 -> ?assertError("The test case took too long and was timed out",false)
            end
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.



%% Let 3 processes asynchronously increment the same multy value register 100times while using a lock to restrict the access.
%% 0 ms delay between increments
asynchronous_test_4(Config) ->
Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    Keys = [asynchronous_test_key_4],
    Object = {asynchronous_test_key_4, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys,Object,100,[],self(),0,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys,Object,100,[],self(),0,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys,Object,100,[],self(),0,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                        {done,Node3,3,_Clocks3} ->

                            {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_11 = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                            {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                            {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_22 = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                            {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                            Res_33 = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                            {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
                            {ok, [[Res1]|[]]} = Res_11,
                            {ok, [[Res2]|[]]} = Res_22,
                            {ok, [[Res3]|[]]} = Res_33,
                            ?assertEqual(300,Res1),
                            ?assertEqual(300,Res2),
                            ?assertEqual(300,Res3)
                    after 300000 -> ?assertError("The test case took too long and was timed out",false)
                    end
            after 300000 -> ?assertError("The test case took too long and was timed out",false)
            end
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.



asynchronous_test_helper2(Node,_,_,0,Clocks,Caller,_,Id)->
    Caller ! {done,Node,Id,Clocks};

asynchronous_test_helper2(Node,Keys,Object,Increments,Clocks,Caller,Delay,Id)->
    ct:pal("Starting transaction ~p at ~p", [Increments, Node]),
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok, TxId1} ->
            ct:pal("Started transaction ~p at ~p", [Increments, Node]),
            {ok, Res} = rpc:call(Node, antidote, read_objects, [[Object],TxId1]),
            case Res of
                [[]] ->
                    ok = rpc:call(Node, antidote, update_objects,[[{Object, assign, 1}],TxId1]);
                [[Res2]|[]] ->
                    ok = rpc:call(Node, antidote, update_objects,[[{Object, assign, Res2+1}],TxId1])
            end,
            {ok, Clock1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
            ct:pal("Committed transaction ~p at ~p", [Increments, Node]),
            timer:sleep(Delay),
            asynchronous_test_helper2(Node, Keys, Object,Increments-1,[Clock1|Clocks],Caller,Delay,Id);
        {error,{error,_Missing_Locks}} ->
            ct:pal("Error starting transaction ~p at ~p", [Increments, Node]),
            timer:sleep(Delay),
            asynchronous_test_helper2(Node, Keys,Object,Increments,Clocks,Caller,Delay,Id)
    end.

%% Runs asynchronous_test_1-4 in parallel
asynchronous_test_5(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],   % asynchronous_test_4
    Keys1 = [asynchronous_test_key_5],
    Object1 = {asynchronous_test_key_5, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,100,[],self(),0,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,100,[],self(),0,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,100,[],self(),0,3]),
    % asynchronous_test_3
    Keys2 = [asynchronous_test_key_6],
    Object2 = {asynchronous_test_key_6, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys2,Object2,100,[],self(),30,4]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys2,Object2,100,[],self(),30,5]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys2,Object2,100,[],self(),30,6]),
    % asynchronous_test_2
    Keys3 = [asynchronous_test_key_7],
    Object3 = {asynchronous_test_key_7, antidote_crdt_counter_pn, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node1,Keys3,Object3,100,[],self(),0,7]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node2,Keys3,Object3,100,[],self(),0,8]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node3,Keys3,Object3,100,[],self(),0,9]),
    % asynchronous_test_1
    Keys4 = [asynchronous_test_key_8],
    Object4 = {asynchronous_test_key_8, antidote_crdt_counter_pn, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node1,Keys4,Object4,100,[],self(),30,10]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node2,Keys4,Object4,100,[],self(),30,11]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper,[Node3,Keys4,Object4,100,[],self(),30,12]),
    helper_receive_result({Node1,1},{Node2,2},{Node3,3},"ansynchronous_test_5 -- as.test_4 repetition"),
    helper_receive_result({Node1,4},{Node2,5},{Node3,6},"ansynchronous_test_5 -- as.test_3 repetition"),
    helper_receive_result({Node1,7},{Node2,8},{Node3,9},"ansynchronous_test_5 -- as.test_2 repetition"),
    helper_receive_result({Node1,10},{Node2,11},{Node3,12},"ansynchronous_test_5 -- as.test_1 repetition"),
    % asynchronous_test_4
    helper_check_result2(Node1, Keys1, Object1, 300),
    helper_check_result2(Node2, Keys1, Object1, 300),
    helper_check_result2(Node3, Keys1, Object1, 300),
    % asynchronous_test_3
    helper_check_result2(Node1, Keys2, Object2, 300),
    helper_check_result2(Node2, Keys2, Object2, 300),
    helper_check_result2(Node3, Keys2, Object2, 300),
    % asynchronous_test_2
    helper_check_result1(Node1, Keys3, Object3, 300),
    helper_check_result1(Node2, Keys3, Object3, 300),
    helper_check_result1(Node3, Keys3, Object3, 300),
    % asynchronous_test_1
    helper_check_result1(Node1, Keys4, Object4, 300),
    helper_check_result1(Node2, Keys4, Object4, 300),
    helper_check_result1(Node3, Keys4, Object4, 300).


helper_receive_result({Node1,Id1},{Node2,Id2},{Node3,Id3},_Info) ->
    receive
        {done,Node1,Id1,Clocks1} ->
            A={Node1,Clocks1},
            receive
                {done,Node2,Id2,Clocks2} ->
                    B={Node2,Clocks2},
                    receive
                        {done,Node3,Id3,Clocks3} ->
                            C={Node3,Clocks3},
                            {A,B,C}
                    after 300000 -> ?assertError("The test case took too long and was timed out",false)
                    end
            after 300000 -> ?assertError("The test case took too long and was timed out",false)
            end
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.



helper_check_result1(Node,Keys,Object,Value) ->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok,TxId} ->
            {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Object],TxId]),
            {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId]),
            ?assertEqual(Value,Res);
        {error,_} ->
            helper_check_result1(Node, Keys, Object, Value)
    end.

helper_check_result2(Node,Keys,Object,Value) ->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of
        {ok,TxId} ->
            {ok, [[Res]|[]]} = rpc:call(Node, antidote, read_objects, [[Object],TxId]),
            {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId]),
            ?assertEqual(Value,Res);
        {error,_} ->
            helper_check_result2(Node, Keys, Object, Value)
    end.

%% Asynchronously starts transactions on all DCs that require the same 100 locks each.
a_lot_of_locks_per_transaction_1(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    % asynchronous_test_4
    Keys1 = generate_lock_helper(50,"erwwqd"),
    Object1 = {a_lot_of_locks_per_transaction_1_key, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,10,[],self(),0,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,10,[],self(),0,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,10,[],self(),0,3]),
    helper_receive_result({Node1,1},{Node2,2},{Node3,3},"a_lot_of_locks_per_transaction_1"),
    helper_check_result2(Node1, Keys1, Object1, 30),
    helper_check_result2(Node2, Keys1, Object1, 30),
    helper_check_result2(Node3, Keys1, Object1, 30).

%% Asynchronously starts transactions on all DCs that require the same 500 locks each. Remark the higher the value the likelier the gen server call timouts.
a_lot_of_locks_per_transaction_2(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    % asynchronous_test_4
    Keys1 = generate_lock_helper(100,"asdf"),
    Object1 = {a_lot_of_locks_per_transaction_2_key, antidote_crdt_register_mv, antidote_bucket},
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,20,[],self(),0,1]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,20,[],self(),0,2]),
    spawn_link(lock_mgr_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,20,[],self(),0,3]),
    helper_receive_result({Node1,1},{Node2,2},{Node3,3},"a_lot_of_locks_per_transaction_2"),
    helper_check_result2(Node1, Keys1, Object1, 60),
    helper_check_result2(Node2, Keys1, Object1, 60),
    helper_check_result2(Node3, Keys1, Object1, 60).

generate_lock_helper(Amount,String) ->
    generate_lock_helper(Amount,String,[]).
generate_lock_helper(Amount,String,List) ->
    New_List = [list_to_binary(integer_to_list(Amount)++String++"_Lock")|List],
    case Amount of
        A when A > 0 ->
            generate_lock_helper(Amount-1,String, New_List);
        _ -> New_List
    end.


%% Starts a transaction that acquires a lock on one node. Then this node is killed and restarted.
%% Then another transaction is started on another Node using the same lock.
cluster_failure_test_1(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Keys = [cluster_failure_test_1_key],
            {ok,_TxId1} = helper_start_transaction(Keys, Node3),
            %% Kill a node
            ct:print("Killing node ~w", [Node3]),
            [Node3] = test_utils:brutal_kill_nodes([Node3]),
            timer:sleep(10000), % since the brutal kill command is someties delayed ...
            %% Start the node back up and be sure everything works
            ct:print("Restarting node ~w", [Node3]),
            [Node3] = test_utils:restart_nodes([Node3], Config),
            timer:sleep(10000),
            {ok,TxId2} = helper_start_transaction(Keys, Node2),
            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2])
    end.
%% TODO Does not work
%% Starts a transaction that acquires a lock on one node3. Then node3 is killed.
%% Then another transaction is started on Node2 trying to acquire the same lock.
%% Then node3 is restarted and a transaction acquiring the lock on node2 is started again.
cluster_failure_test_2(Config) ->
    Clusters = proplists:get_value(clusters, Config),
    [Node1, Node2, Node3 | _Nodes] =  [ hd(Cluster)|| Cluster <- Clusters ],
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Keys = [cluster_failure_test_2_key],
            {ok,_TxId1} = helper_start_transaction(Keys, Node3),
            %% Kill a node
            ct:pal("Killing node ~w", [Node3]),
            [Node3] = test_utils:brutal_kill_nodes([Node3]),
            timer:sleep(10000), % since the brutal kill command is sometimes delayed ...
            %% Test if the lock can be acquired by another dc
            % TODO This rpc:call will crash the lock_mgr and wont let it recover
            % (Assumption: The inter_dc communication does not handle this case and lock_mgr does
            % not have a build in error handling process for this case.)
            ct:pal("Starting transaction on ~w (expecting to fail)", [Node2]),
            {error,_} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
            %% Start the node back up and be sure everything works
            [Node3] = test_utils:restart_nodes([Node3], Config),
            timer:sleep(10000),
            ct:pal("Starting transaction on ~w", [Node2]),
            {ok,TxId2} = helper_start_transaction(Keys, Node2),
            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2])
    end.

helper_start_transaction(Locks,Node) ->
    rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Locks}]]).


txn_seq_read_check(Node, TxId, Objects, ExpectedValues) ->
    lists:map(fun({Object, Expected}) ->
        {ok, [Val]} = rpc:call(Node, antidote, read_objects, [[Object], TxId]),
        ?assertEqual(Expected, Val)
    end, lists:zip(Objects, ExpectedValues)).

txn_seq_update_check(Node, TxId, Updates) ->
    lists:map(fun(Update) ->
        Res = rpc:call(Node, antidote, update_objects, [[Update], TxId]),
        ?assertMatch(ok, Res)
    end, Updates).



