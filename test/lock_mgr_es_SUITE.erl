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
-module(lock_mgr_es_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([simple_transaction_tests_with_locks/1,
        locks_in_sequence_check/1,
        locks_required_by_another_transaction/1,
         lock_acquisition_test/1,
         some_test/1,
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
-include("../include/antidote.hrl").
-include("../include/inter_dc_repl.hrl").





init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    %Nodes = hd(Clusters),
    [{nodes, Clusters}|Config].

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
         locks_required_by_another_transaction,
         lock_acquisition_test,
         some_test,
         get_lock_owned_by_other_dc_2,
         multi_value_register_test,
         asynchronous_test_1,
         asynchronous_test_2,
         asynchronous_test_3,
         asynchronous_test_4,
         asynchronous_test_5,
         a_lot_of_locks_per_transaction_1,
         a_lot_of_locks_per_transaction_2%,
         %cluster_failure_test_1,
         %cluster_failure_test_2
        ].

%% Checks if a transaction on the leading(may create new locks) node can aquire never used
%% locks, do some updates and then releases them when the transaction is committed
simple_transaction_tests_with_locks(Config) ->
    %
    Node = hd(hd(proplists:get_value(nodes, Config))),
    Type = antidote_crdt_counter_pn,
    Bucket = antidote_bucket,
    Keys = [es_lock1, es_lock2, es_lock3, es_lock4],
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
    % checks if all locks are released
    Lock_Info2 = rpc:call(Node, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    % checks if the updates were successful
    ?assertEqual([1, 2, 3, 4], Res).

%% test if after a transaction released some lock another transaction on the same node can aquire them
locks_in_sequence_check(Config) ->
    %
    Node = hd(hd(proplists:get_value(nodes, Config))),
    Keys = [es_lock5, es_lock6, es_lock7, es_lock8],
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys}} = lists:keyfind(TxId,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    {error,{error,{[{TxId,Missing_Keys}],[]}}} = rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    ?assertEqual(length(Keys),length(Missing_Keys)),
    ?assertEqual([],Keys -- Missing_Keys),
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
    Lock_Info2 = rpc:call(Node, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    ok.

%% starts a transaction on the leading node aquiring some lock.
%% Tests if transactions of other nodes can not acquire a subset of these keys
locks_required_by_another_transaction(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node3 = hd(hd(tl(Nodes))),
    Node4 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_lock14, es_lock13, es_lock12, es_lock11],
    {ok, TxId} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys}} = lists:keyfind(TxId,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys)),
    ?assertEqual([],Keys--Used_Keys),
    % Test if transaction requiring the used keys can not start a transaction
    {error,{error,{[{TxId,Missing_Keys0}],[]}}} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    {error,{error,{[],Missing_Keys2}}} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,[hd(tl(Keys))]}]]),%TODO Error in testcase -- lock_mgr_es says ok instead of an error....
    {error,{error,{[],Missing_Keys3}}} = rpc:call(Node4, antidote, start_transaction, [ignore, [{exclusive_locks,tl(Keys)}]]),
    ?assertEqual(length(Keys),length(Missing_Keys0)),
    ?assertEqual([],Keys -- Missing_Keys0),
    ?assertEqual(length([hd(tl(Keys))]),length(Missing_Keys2)),
    ?assertEqual([],[hd(tl(Keys))] -- Missing_Keys2),
    ?assertEqual(length(tl(Keys)),length(Missing_Keys3)),
    ?assertEqual([],tl(Keys) -- Missing_Keys3),
    {ok, _Clock} = rpc:call(Node1, antidote, commit_transaction, [TxId]),
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId,1,Lock_Info2),
    ok.
%% Tests if lock acquisition in multiple dcs of the same locks can propperly acquire 
%% them and the lock_mgr_es manages the lock data as intendet.
lock_acquisition_test(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Keys = [es_lock21, es_lock22, es_lock23, es_lock24],
    
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys1}} = lists:keyfind(TxId1,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys1)),
    ?assertEqual([],Keys--Used_Keys1),
    {ok, _Clock1} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    
    
    {ok, TxId2} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys2}} = lists:keyfind(TxId2,1,Lock_Info2),
    ?assertEqual(length(Keys),length(Used_Keys2)),
    ?assertEqual([],Keys--Used_Keys2),
    {ok, _Clock2} = rpc:call(Node1, antidote, commit_transaction, [TxId2]),
    
    
    {ok, TxId3} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info3 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys3}} = lists:keyfind(TxId3,1,Lock_Info3),
    ?assertEqual(length(Keys),length(Used_Keys3)),
    ?assertEqual([],Keys--Used_Keys3),
    {ok, _Clock3} = rpc:call(Node1, antidote, commit_transaction, [TxId3]),
    
    Lock_Info4 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info4),
    false = lists:keyfind(TxId2,1,Lock_Info4),
    false = lists:keyfind(TxId3,1,Lock_Info4),
    ok.
%% Test if sequential lock acquisition works as intendet
get_lock_owned_by_other_dc_1(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node3 = hd(hd(tl(Nodes))),
    Node4 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_lock31, es_lock32, es_lock33, es_lock34],
    
    {ok, TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
    Lock_Info1 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys1}} = lists:keyfind(TxId1,1,Lock_Info1),
    ?assertEqual(length(Keys),length(Used_Keys1)),
    ?assertEqual([],Keys--Used_Keys1),
    {ok, _Clock1} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
    Lock_Info1_2 = rpc:call(Node1, lock_mgr_es, local_locks_info, []),
    false = lists:keyfind(TxId1,1,Lock_Info1_2),
    
    {ok, TxId2} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,[es_lock31]}]]),
    Lock_Info2 = rpc:call(Node3, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys2}} = lists:keyfind(TxId2,1,Lock_Info2),
    ?assertEqual(1,length(Used_Keys2)),
    ?assertEqual([],[es_lock31]--Used_Keys2),
    {ok, _Clock2} = rpc:call(Node3, antidote, commit_transaction, [TxId2]),
    
    {ok, TxId3} = rpc:call(Node4, antidote, start_transaction, [ignore, [{exclusive_locks,[es_lock32,es_lock33,es_lock34]}]]),
    Lock_Info3 = rpc:call(Node4, lock_mgr_es, local_locks_info, []),
    {_,{using,[],Used_Keys3}} = lists:keyfind(TxId3,1,Lock_Info3),
    ?assertEqual(3,length(Used_Keys3)),
    ?assertEqual([],[es_lock32,es_lock33,es_lock34]--Used_Keys3),
    {ok, _Clock3} = rpc:call(Node4, antidote, commit_transaction, [TxId3]),
    ok.
%% Test if sequential lock acquisition works as intendet
get_lock_owned_by_other_dc_2(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node3 = hd(hd(tl(Nodes))),
    Node4 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_lock41, es_lock42, es_lock43, es_lock44],
    Lock_request_order = [Node3,Node1,Node1,Node3,Node4,Node3,Node4,Node1],
    helper_do_lock_requests(Lock_request_order, Keys).
%% Helper function for get_lock_owned_by_other_dc_2
helper_do_lock_requests([],_)-> ok;
helper_do_lock_requests([Current_Node | Remaining_Nodes],Keys)->
    case rpc:call(Current_Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of 
        {ok, TxId1} ->
            Lock_Info1 = rpc:call(Current_Node, lock_mgr_es, local_locks_info, []),
            {_,{using,[],Used_Keys1}} = lists:keyfind(TxId1,1,Lock_Info1),
            ?assertEqual(length(Keys),length(Used_Keys1)),
            ?assertEqual([],Keys--Used_Keys1),
            {ok, _Clock1} = rpc:call(Current_Node, antidote, commit_transaction, [TxId1]),
            Lock_Info1_2 = rpc:call(Current_Node, lock_mgr_es, local_locks_info, []),
            ?assertEqual(false, lists:keyfind(TxId1,1,Lock_Info1_2)),
            helper_do_lock_requests(Remaining_Nodes, Keys);
        {error,{error,_Missing_Locks}} ->
            helper_do_lock_requests([Current_Node|Remaining_Nodes], Keys)
    end.
%% Tests if a multi value register allways has the correct value when it is updated on multiple dcs using locks
multi_value_register_test(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node3 = hd(hd(tl(Nodes))),
    Node4 = hd(hd(tl(tl(Nodes)))),
    Key = es_multi_value_register,
    Bound_object = {Key, antidote_crdt_register_mv, antidote_bucket},
    Updates_List=[{[[]],Node1,<<"n1">>},{<<"n1">>,Node3,<<"x2">>},{<<"x2">>,Node3,<<"x3">>},{<<"x3">>,Node4,<<"y4">>},{<<"y4">>,Node1,<<"n5">>}
                    ,{<<"n5">>,Node1,<<"n6">>},{<<"n6">>,Node4,<<"y7">>},{<<"y7">>,Node1,<<"n8">>},{<<"n8">>,Node3,<<"x9">>},{<<"x9">>,Node4,<<"y10">>}],
    helper_multi_value_register_test(Updates_List,[Key],Bound_object).
%% helper functin for multi_value_register_test
helper_multi_value_register_test([],_,_)-> ok;
helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes],Keys,Object)->
    case rpc:call(Current_Node, cure, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of 
        {ok, TxId1} ->
            case Value2 of
                <<"n1">> -> ok;
                _ ->
                    {ok, [[Read_Val]|[]]} = rpc:call(Current_Node, cure, read_objects, [[Object], TxId1]),
                    ?assertEqual(Value1,Read_Val)
            end,
            ok = rpc:call(Current_Node, cure, update_objects, [[{Object,assign,Value2}],TxId1]),
            {ok, _Clock1} = rpc:call(Current_Node, cure, commit_transaction, [TxId1]),
        
            helper_multi_value_register_test(Remaining_Nodes, Keys, Object);
        {error,{error,_Missing_Locks}} ->
            helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes], Keys,Object)
    end.    
%helper_multi_value_register_test([],_,_,_)-> ok;
%helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes],Keys,Object,Snapshot)->
%    case rpc:call(Current_Node, cure, start_transaction, [Snapshot, [{exclusive_locks,Keys}]]) of 
%        {ok, TxId1} ->
%            case Value2 of
%                n1 -> ok;
%                _ ->
%                    {ok, [[Read_Val]|[]]} = rpc:call(Current_Node, cure, read_objects, [[Object], TxId1]),
%                    ?assertEqual(Value1,Read_Val)
%            end,
%            ok = rpc:call(Current_Node, cure, update_objects, [[{Object,assign,Value2}],TxId1]),
%            {ok, Clock1} = rpc:call(Current_Node, cure, commit_transaction, [TxId1]),
%        {error,{error,_Missing_Locks}} ->
%            helper_multi_value_register_test([{Value1,Current_Node,Value2} | Remaining_Nodes], Keys,Object,Snapshot)
%    end.

%% Let 3 processes asynchronously increment the same counter each 100times while using a lock to restrict the access.
%% 30 ms delay between increments
asynchronous_test_1(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_asynchronous_test_key_1],
    Object = {es_asynchronous_test_key_1, antidote_crdt_counter_pn, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node1,Keys,Object,100,[],self(),30,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node2,Keys,Object,100,[],self(),30,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node3,Keys,Object,100,[],self(),30,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                            {done,Node3,3,_Clocks3} ->

                                {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res1]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                                {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                                ?assertEqual(300,Res1),
                                {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res2]} = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                                {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                                ?assertEqual(300,Res2),
                                {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res3]} = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                                {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
                                ?assertEqual(300,Res3)
                        after 300000 -> ?assertError("The test case took too long and was timed out",false)
                        end
            after 300000 -> ?assertError("The test case took too long and was timed out",false)
            end
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.
    
    



%% Let 3 processes asynchronously increment the same counter 100times while using a lock to restrict the access.
%% 0 ms delay between increments
asynchronous_test_2(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_asynchronous_test_key_2],
    Object = {es_asynchronous_test_key_2, antidote_crdt_counter_pn, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node1,Keys,Object,100,[],self(),0,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node2,Keys,Object,100,[],self(),0,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node3,Keys,Object,100,[],self(),0,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                            {done,Node3,3,_Clocks3} ->

                                {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res1]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                                {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                                ?assertEqual(300,Res1),
                                {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res2]} = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                                {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                                ?assertEqual(300,Res2),
                                {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [Res3]} = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                                {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
                                ?assertEqual(300,Res3)
                        after 300000 -> ?assertError("The test case took too long and was timed out",false)
                        end
            after 300000 -> ?assertError("The test case took too long and was timed out",false)
            end
    after 300000 -> ?assertError("The test case took too long and was timed out",false)
    end.

asynchronous_test_helper(Node,_,_,0,Clocks,Caller,_,Id)->
    Caller ! {done,Node,Id,Clocks};
    
asynchronous_test_helper(Node,Keys,Object,Increments,Clocks,Caller,Delay,Id)->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of 
        {ok, TxId1} ->
            ok = rpc:call(Node, antidote, update_objects,[[{Object, increment, 1}],TxId1]),
            {ok, Clock1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
            timer:sleep(Delay),
            asynchronous_test_helper(Node, Keys, Object,Increments-1,[Clock1|Clocks],Caller,Delay,Id);
        {error,{error,_Missing_Locks}} ->
            timer:sleep(Delay),
            asynchronous_test_helper(Node, Keys,Object,Increments,Clocks,Caller,Delay,Id)
    end.


%% Let 3 processes asynchronously increment the same multy value register 100times while using a lock to restrict the access.
%% 30 ms delay between increments
asynchronous_test_3(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_asynchronous_test_key_3],
    Object = {es_asynchronous_test_key_3, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys,Object,100,[],self(),30,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys,Object,100,[],self(),30,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys,Object,100,[],self(),30,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                            {done,Node3,3,_Clocks3} ->

                                {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res1]|[]]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                                {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                                ?assertEqual(300,Res1),
                                {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res2]|[]]} = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                                {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                                ?assertEqual(300,Res2),
                                {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res3]|[]]} = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                                {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
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
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    Keys = [es_asynchronous_test_key_4],
    Object = {es_asynchronous_test_key_4, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys,Object,100,[],self(),0,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys,Object,100,[],self(),0,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys,Object,100,[],self(),0,3]),
    receive
        {done,Node1,1,_Clocks1} ->

            receive
                {done,Node2,2,_Clocks2} ->

                    receive
                            {done,Node3,3,_Clocks3} ->

                                {ok,TxId1} = rpc:call(Node1, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res1]|[]]} = rpc:call(Node1, antidote, read_objects, [[Object],TxId1]),
                                {ok, _} = rpc:call(Node1, antidote, commit_transaction, [TxId1]),
                                ?assertEqual(300,Res1),
                                {ok,TxId2} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res2]|[]]} = rpc:call(Node2, antidote, read_objects, [[Object],TxId2]),
                                {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2]),
                                ?assertEqual(300,Res2),
                                {ok,TxId3} = rpc:call(Node3, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
                                {ok, [[Res3]|[]]} = rpc:call(Node3, antidote, read_objects, [[Object],TxId3]),
                                {ok, _} = rpc:call(Node3, antidote, commit_transaction, [TxId3]),
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
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]) of 
        {ok, TxId1} ->
            {ok, Res} = rpc:call(Node, antidote, read_objects, [[Object],TxId1]),
            case Res of
                [[]] ->
                    ok = rpc:call(Node, antidote, update_objects,[[{Object, assign, 1}],TxId1]);
                [[Res2]|[]] -> 
                    ok = rpc:call(Node, antidote, update_objects,[[{Object, assign, Res2+1}],TxId1])
            end,
            {ok, Clock1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
            timer:sleep(Delay),
            asynchronous_test_helper2(Node, Keys, Object,Increments-1,[Clock1|Clocks],Caller,Delay,Id);
        {error,{error,_Missing_Locks}} ->
            timer:sleep(Delay),
            asynchronous_test_helper2(Node, Keys,Object,Increments,Clocks,Caller,Delay,Id)
    end.

%% Runs asynchronous_test_1-4 in parallel
asynchronous_test_5(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    % asynchronous_test_4
    Keys1 = [es_asynchronous_test_key_5],
    Object1 = {es_asynchronous_test_key_5, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,100,[],self(),0,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,100,[],self(),0,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,100,[],self(),0,3]),
    % asynchronous_test_3
    Keys2 = [es_asynchronous_test_key_6],
    Object2 = {es_asynchronous_test_key_6, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys2,Object2,100,[],self(),30,4]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys2,Object2,100,[],self(),30,5]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys2,Object2,100,[],self(),30,6]),
    % asynchronous_test_2
    Keys3 = [es_asynchronous_test_key_7],
    Object3 = {es_asynchronous_test_key_7, antidote_crdt_counter_pn, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node1,Keys3,Object3,100,[],self(),0,7]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node2,Keys3,Object3,100,[],self(),0,8]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node3,Keys3,Object3,100,[],self(),0,9]),
    % asynchronous_test_1
    Keys4 = [es_asynchronous_test_key_8],
    Object4 = {es_asynchronous_test_key_8, antidote_crdt_counter_pn, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node1,Keys4,Object4,100,[],self(),30,10]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node2,Keys4,Object4,100,[],self(),30,11]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper,[Node3,Keys4,Object4,100,[],self(),30,12]),
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
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    % asynchronous_test_4
    Keys1 = generate_lock_helper(50,"eghjrwwqd"),
    Object1 = {es_a_lot_of_locks_per_transaction_1_key, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,10,[],self(),0,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,10,[],self(),0,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,10,[],self(),0,3]),
    helper_receive_result({Node1,1},{Node2,2},{Node3,3},"a_lot_of_locks_per_transaction_1"),
    helper_check_result2(Node1, Keys1, Object1, 30),
    helper_check_result2(Node2, Keys1, Object1, 30),
    helper_check_result2(Node3, Keys1, Object1, 30).

%% Asynchronously starts transactions on all DCs that require the same 500 locks each.  Remark 1000 timouted the gen_server call "get_locks" to lock_mgr_es an a personal pc
a_lot_of_locks_per_transaction_2(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    % asynchronous_test_4
    Keys1 = generate_lock_helper(100,"asiuiopdf"),
    Object1 = {es_a_lot_of_locks_per_transaction_2_key, antidote_crdt_register_mv, antidote_bucket},
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node1,Keys1,Object1,20,[],self(),0,1]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node2,Keys1,Object1,20,[],self(),0,2]),
    spawn(lock_mgr_es_SUITE,asynchronous_test_helper2,[Node3,Keys1,Object1,20,[],self(),0,3]),
    helper_receive_result({Node1,1},{Node2,2},{Node3,3},"a_lot_of_locks_per_transaction_2"),
    helper_check_result2(Node1, Keys1, Object1, 60),
    helper_check_result2(Node2, Keys1, Object1, 60),
    helper_check_result2(Node3, Keys1, Object1, 60).

generate_lock_helper(Amount,String) ->
    generate_lock_helper(Amount,String,[]).
generate_lock_helper(Amount,String,List) ->
    New_List = [integer_to_list(Amount)++String++"_Lock"|List],
    case Amount of
        A when A > 0 ->
            generate_lock_helper(Amount-1,String, New_List);
        _ -> New_List
    end.


%% Starts a transactin that aquires a lock on one node. Then this node is killed and restarted.
%% Then another transaction is started on another Node using the same lock.
cluster_failure_test_1(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Keys = [<<"es_cluster_failure_test_1_key">>],
            {ok,_TxId1} = helper_start_transaction(Keys, Node3, 10),
            %% Kill a node
            ct:print("Killing node ~w", [Node3]),
            [Node3] = test_utils:brutal_kill_nodes([Node3]),
            timer:sleep(10000), % since the brutal kill command is someties delayed ...
            %% Start the node back up and be sure everything works
            ct:print("Restarting node ~w", [Node3]),
            [Node3] = test_utils:restart_nodes([Node3], Config),
            timer:sleep(10000),
            {ok,TxId2} = helper_start_transaction(Keys, Node2, 100),
            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2])
    end.
%% TODO Does not work
%% Starts a transactin that aquires a lock on one node3. Then node3 is killed.
%% Then another transaction is started on Node2 trying to aquire the same lock.
%% Then node3 is restarted and a transaction aquiring the lock on node2 is started again.
cluster_failure_test_2(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node2 = hd(hd(tl(Nodes))),
    Node3 = hd(hd(tl(tl(Nodes)))),
    case rpc:call(Node1, application, get_env, [antidote, enable_logging]) of
        {ok, false} ->
            pass;
        _ ->
            Keys = [<<"es_cluster_failure_test_2_key">>],
            {ok,_TxId1} = helper_start_transaction(Keys, Node3, 10),
            %% Kill a node
            ct:print("Killing node ~w", [Node3]),
            [Node3] = test_utils:brutal_kill_nodes([Node3]),
            timer:sleep(10000), % since the brutal kill command is someties delayed ...
            %% Test if the lock can be aquired by another dc
            % TODO This rpc:call will crash the lock_mgr_es and wont let it recover
            % (Assumption: The inter_dc communication does not handle this case and lock_mgr_es does
            % not have a build in error handling process for this case.)
            {error,_} = rpc:call(Node2, antidote, start_transaction, [ignore, [{exclusive_locks,Keys}]]),
            %% Start the node back up and be sure everything works
            ct:print("Restarting node ~w", [Node3]),
            [Node3] = test_utils:restart_nodes([Node3], Config),
            timer:sleep(10000),
            {ok,TxId2} = helper_start_transaction(Keys, Node2, 100),
            {ok, _} = rpc:call(Node2, antidote, commit_transaction, [TxId2])
    end.
helper_start_transaction(Locks,_Node,0) ->
    {error,"Could not aquire Locks"};
helper_start_transaction(Locks,Node,Tries) ->
    case rpc:call(Node, antidote, start_transaction, [ignore, [{exclusive_locks,Locks}]]) of
        {ok,TxId}->
            {ok,TxId};
        {error,_}->
            helper_start_transaction(Locks,Node,Tries-1);
        {badrpc,_}->
            helper_start_transaction(Locks,Node,Tries-1)
    end.
to_string_helper([])->
    "[]";
to_string_helper([HD|TL])->
    to_string_helper(TL,"["++atom_to_list(HD)).
to_string_helper([],String)->
    String++"]";
to_string_helper([HD|TL],String)->
    New_String = String++", "++atom_to_list(HD),
    to_string_helper(TL, New_String).


%% Tests some internal functions of lock_mgr_es
some_test(Config) ->
    %
    Nodes = proplists:get_value(nodes, Config),
    Node1 = hd(hd(Nodes)),
    Node3 = hd(hd(tl(Nodes))),
    Node4 = hd(hd(tl(tl(Nodes)))),
    A = rpc:call(Node1, lock_mgr_es, am_i_leader, []),
    B = rpc:call(Node3, lock_mgr_es, am_i_leader, []),
    C = rpc:call(Node4, lock_mgr_es, am_i_leader, []),
    [true,false,false] = [A,B,C],
    ok.

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



