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
-module(json_client_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ADDRESS, "localhost").

-define(PORT, 10017).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),
    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(Node,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    pass = start_stop_test(),

    [Nodes1] = common:clean_and_rebuild_clusters([Nodes]),
    rt:wait_for_service(Node, antidote),
    simple_transaction_test(hd(Nodes)),

    [Nodes2] = common:clean_and_rebuild_clusters([Nodes1]),
    rt:wait_for_service(Node, antidote),
    read_write_test(hd(Nodes)),

    [Nodes3] = common:clean_and_rebuild_clusters([Nodes2]),
    rt:wait_for_service(Node, antidote),
    get_empty_crdt_test(),

    [Nodes4] = common:clean_and_rebuild_clusters([Nodes3]),
    rt:wait_for_service(Node, antidote),
    pb_test_counter_read_write(hd(Nodes)),

    [Nodes5] = common:clean_and_rebuild_clusters([Nodes4]),
    rt:wait_for_service(Node, antidote),
    pb_test_set_read_write(hd(Nodes)),

    [Nodes6] = common:clean_and_rebuild_clusters([Nodes5]),
    rt:wait_for_service(Node, antidote),
    pb_empty_txn_clock_test(),

    [Nodes7] = common:clean_and_rebuild_clusters([Nodes6]),
    rt:wait_for_service(Node, antidote),
    pass = update_counter_crdt_test(<<"key1">>,<<"bucket">>, 10),

    [Nodes8] = common:clean_and_rebuild_clusters([Nodes7]),
    rt:wait_for_service(Node, antidote),
    pass = update_counter_crdt_and_read_test(<<"key2">>, 15),
    [Nodes9] = common:clean_and_rebuild_clusters([Nodes8]),
    rt:wait_for_service(Node, antidote),
    update_set_read_test(),
    [Nodes10] = common:clean_and_rebuild_clusters([Nodes9]),
    rt:wait_for_service(Node, antidote),
    static_transaction_test(),

    [Nodes11] = common:clean_and_rebuild_clusters([Nodes10]),
    rt:wait_for_service(Node, antidote),
    pb_get_objects_test(),

    [Nodes12] = common:clean_and_rebuild_clusters([Nodes11]),
    rt:wait_for_service(Node, antidote),
    pb_get_log_operations_test(),
    
    [Nodes13] = common:clean_and_rebuild_clusters([Nodes12]),
    rt:wait_for_service(Node, antidote),
    update_set_fixed_snapshot_test(hd(Nodes13)),

    [_Nodes14] = common:clean_and_rebuild_clusters([Nodes13]),
    rt:wait_for_service(Node, antidote),
    dont_certify_test(),
    pass.

start_stop_test() ->
    lager:info("Verifying pb connection..."),
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.


%% starts and transaction and read a key
simple_transaction_test(Node) ->
    Bound_object = {key, crdt_pncounter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    rpc:call(Node, antidote, finish_transaction, [TxId]).


read_write_test(Node) ->
    Bound_object = {key, crdt_pncounter, bucket},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [0]} = rpc:call(Node, antidote, read_objects, [[Bound_object], TxId]),
    ok = rpc:call(Node, antidote, update_objects, [[{Bound_object, increment, 1}], TxId]),
    rpc:call(Node, antidote, finish_transaction, [TxId]).


%% Single object rea
get_empty_crdt_test() ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {<<"key">>, crdt_pncounter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    ?assertMatch(true, antidotec_counter:is_type(Val)).

pb_get_objects_test() ->
    Key1 = <<"key_get_objects1">>,
    Key2 = <<"key_get_objects2">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object1 = {Key1, crdt_orset, <<"bucket">>},
    Bound_object2 = {Key2, crdt_orset, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object1, add, <<"a">>},{Bound_object2, add, <<"b">>}], TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    
    {ok, Val} = antidotec_pb:get_objects(Pid, [Bound_object1,Bound_object2], json),
    lager:info("The get objects result ~p", [Val]),
    [[Object1,CommitTime1],[Object2,CommitTime2]]=Val,
    lager:info("The base objects ~p", [[Object1,Object2]]),
    lager:info("The commit times ~p", [[dict:to_list(CommitTime1),dict:to_list(CommitTime2)]]),
    _Disconnected = antidotec_pb_socket:stop(Pid).


pb_get_log_operations_test() ->
    Key1 = <<"key_get_objects1">>,
    Key2 = <<"key_get_objects2">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object1 = {Key1, crdt_orset, <<"bucket">>},
    Bound_object2 = {Key2, crdt_orset, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object1, add, <<"a">>},{Bound_object2, add, <<"b">>}], TxId, json),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pid, TxId, json),
    lager:info("committime ~p",[CommitTime]),
    %% Read committed updated
    %% Set the commit time to 0, so you can get all updates
    CommitTime2 = dict:map(fun(_DCID,_Time) ->
				   0
			   end, CommitTime),

    lists:foreach(fun(ReplyType) ->
			  {ok, Val} = antidotec_pb:get_log_operations(Pid,[{Bound_object1,CommitTime2},{Bound_object2,CommitTime2}], ReplyType),
			  lager:info("The get objects result using ~p are ~p", [ReplyType,Val])
		  end, [json]),
    _Disconnected = antidotec_pb_socket:stop(Pid).
    

pb_test_counter_read_write(_Node) ->
    Key = <<"key_read_write">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, crdt_pncounter, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, increment, 1}], TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    ?assertEqual(1, antidotec_counter:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_test_set_read_write(_Node) ->
    Key = <<"key_read_write_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, crdt_orset, <<"bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, add, "a"}], TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    ?assertEqual(["a"],antidotec_set:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

pb_empty_txn_clock_test() ->
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, CommitTime} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, CommitTime, [], json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    _Disconnected = antidotec_pb_socket:stop(Pid).


update_counter_crdt_test(Key, Bucket,  Amount) ->
    lager:info("Verifying retrieval of updated counter CRDT..."),
    BObj = {Key, crdt_pncounter, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Obj = antidotec_counter:new(),
    Obj2 = antidotec_counter:increment(Amount, Obj),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(BObj, Obj2),
                                     TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    pass.

update_counter_crdt_and_read_test(Key, Amount) ->
    pass = update_counter_crdt_test(Key, <<"bucket">>, Amount),
    pass = get_crdt_check_value(Key, crdt_pncounter, <<"bucket">>, Amount).

get_crdt_check_value(Key, Type, Bucket, Expected) ->
    lager:info("Verifying value of updated CRDT..."),
    BoundObject = {Key, Type, Bucket},
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [BoundObject], Tx2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    _Disconnected = antidotec_pb_socket:stop(Pid),
    Mod = antidotec_datatype:module_for_term(Val),
    ?assertEqual(Expected,Mod:value(Val)),
    pass.

update_set_read_test() ->
    Key = <<"key_update_read_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, crdt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add("a", Set),
    Set2 = antidotec_set:add("b", Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, [], json),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains("a", Val)),
    ?assertMatch(true, antidotec_set:contains("b", Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).

static_transaction_test() ->
    Key = <<"key_static_update_read_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, crdt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add("a", Set),
    Set2 = antidotec_set:add("b", Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, [{static, true}], json),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId, json),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2, json),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains("a", Val)),
    ?assertMatch(true, antidotec_set:contains("b", Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid).
    
update_set_fixed_snapshot_test(Node) ->

    %% Disable certification
    ok = rpc:call(Node, clocksi_vnode, set_txn_cert_internal, [false]),

    Key = <<"key_fixed_snapshot_set">>,
    {ok, Pid} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, crdt_orset, <<"bucket">>},
    Set = antidotec_set:new(),
    Set1 = antidotec_set:add("a", Set),
    Set2 = antidotec_set:add("b", Set1),

    {ok, TxId} = antidotec_pb:start_transaction(Pid,
                                                ignore, [], json),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set2),
                                     TxId, json),
    {ok, CT} = antidotec_pb:commit_transaction(Pid, TxId, json),
    CT2 = dict:map(fun(_DCID,Time) ->
			   Time + 1
		   end, CT),
        
    %% Add b again
    Set3 = antidotec_set:add("b",Set),
    {ok, TxId2} = antidotec_pb:start_transaction(Pid,
                                                ignore, [], json),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set3),
                                     TxId2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId2, json),

    %% Remove b using the old commit time
    Set4 = antidotec_set:remove("b",Set),
    {ok, TxId3} = antidotec_pb:start_transaction(Pid, CT2, [{update_clock, false}], json),
    lager:info("The remove ops ~p", [antidotec_set:to_ops(Bound_object, Set4)]),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(Bound_object, Set4),
                                     TxId3, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId3, json),

    %% Read the set again to be sure b is still there
    {ok, TxId4} = antidotec_pb:start_transaction(Pid, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], TxId4, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId4, json),
    ?assertEqual(2,length(antidotec_set:value(Val))),
    ?assertMatch(true, antidotec_set:contains("a", Val)),
    ?assertMatch(true, antidotec_set:contains("b", Val)),

    %% Reenable certification
    ok = rpc:call(Node, clocksi_vnode, set_txn_cert_internal, [true]),

    _Disconnected = antidotec_pb_socket:stop(Pid).

dont_certify_test() ->
    Key = <<"dont_certify_test">>,
    {ok, Pid1} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Pid2} = antidotec_pb_socket:start(?ADDRESS, ?PORT),
    Bound_object = {Key, riak_dt_pncounter, <<"bucket">>},
    %% First Tx
    {ok, TxId1} = antidotec_pb:start_transaction(Pid1, ignore, [], json),
    %% Second Tx, that doesn't certify
    {ok, TxId2} = antidotec_pb:start_transaction(Pid2, ignore, [{certify, dont_certify}], json),
    %% Update and commit first tx
    ok = antidotec_pb:update_objects(Pid1, [{Bound_object, increment, 1}], TxId1, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, TxId1, json),
    %% Update and commit second tx
    ok = antidotec_pb:update_objects(Pid2, [{Bound_object, increment, 1}], TxId2, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid2, TxId2, json),

    %% Read committed updated
    {ok, TxId3} = antidotec_pb:start_transaction(Pid1, ignore, [], json),
    {ok, [Val]} = antidotec_pb:read_objects(Pid1, [Bound_object], TxId3, json),
    {ok, _} = antidotec_pb:commit_transaction(Pid1, TxId3, json),
    ?assertEqual(2, antidotec_counter:value(Val)),
    _Disconnected = antidotec_pb_socket:stop(Pid1),
    _Disconnected = antidotec_pb_socket:stop(Pid2).
