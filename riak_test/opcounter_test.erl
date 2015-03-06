%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(opcounter_test).

-export([confirm/0, clocksi_test1/1, clocksi_test2/1]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clocksi_test1(Nodes),
    clocksi_test2 (Nodes),
    clocksi_tx_noclock_test(Nodes),
    clocksi_single_key_update_read_test(Nodes),
    clocksi_multiple_read_update_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocksi_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    Type = crdt_pncounter,
    %% Empty transaction works,
    Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[]]),
    ?assertMatch({ok, _}, Result0),
    Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [[]]),
    ?assertMatch({ok, _}, Result1),

    % A simple read returns empty
    Result11=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, key1, Type}]]),
    ?assertMatch({ok, _}, Result11),
    {ok, {_, ReadSet11, _}}=Result11, 
    ?assertMatch([0], ReadSet11),

    %% Read what you wrote
    Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                      [{read, key1, Type},
                      {update, key1, Type, {increment, a}},
                      {update, key2, Type, {increment, a}},
                      {read, key1, Type}]]),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, ReadSet2, _}}=Result2, 
    ?assertMatch([0,1], ReadSet2),

    %% Update is persisted && update to multiple keys are atomic
    Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, key1, Type},
                      {read, key2, Type}]]),
    ?assertMatch({ok, _}, Result3),
    {ok, {_, ReadSet3, _}}=Result3,
    ?assertEqual([1,1], ReadSet3),

    %% Multiple updates to a key in a transaction works
    Result5=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{update, key1, Type, {increment, a}},
                      {update, key1, Type, {increment, a}}]]),
    ?assertMatch({ok,_}, Result5),

    Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
                    [
                     [{read, key1, Type}]]),
    {ok, {_, ReadSet6, _}}=Result6,
    ?assertEqual(3, hd(ReadSet6)),
    pass.

%% @doc The following function tests that ClockSI can run an interactive tx.
%%      that updates multiple partitions.
clocksi_test2(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test2 started"),
    Type = crdt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, abc, crdt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, abc, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult),
    ReadResult=rpc:call(FirstNode, antidote, clocksi_iread,
                        [TxId, abc, crdt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult),
    WriteResult1=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, bcd, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult1),
    ReadResult1=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, bcd, crdt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult1),
    WriteResult2=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, cde, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult2),
    ReadResult2=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, cde, crdt_pncounter]),
    ?assertEqual({ok, 1}, ReadResult2),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End),
    {ok,{_Txid, CausalSnapshot}} = End,
    ReadResult3 = rpc:call(FirstNode, antidote, clocksi_read,
                           [CausalSnapshot, abc, Type]),
    {ok, {_,[ReadVal],_}} = ReadResult3,
    ?assertEqual(ReadVal, 1),
    lager:info("Test2 passed"),
    pass.

%% @doc Test to execute transaction with out explicit clock time
clocksi_tx_noclock_test(Nodes) ->
    FirstNode = hd(Nodes),
    Key = itx,
    Type = crdt_pncounter,
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult0=rpc:call(FirstNode, antidote, clocksi_iread,
                         [TxId, Key, crdt_pncounter]),
    ?assertEqual({ok, 0}, ReadResult0),
    WriteResult0=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, Key, Type, {increment, 4}]),
    ?assertEqual(ok, WriteResult0),
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),
    End=rpc:call(FirstNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End),
    ReadResult1 = rpc:call(FirstNode, antidote, clocksi_read,
                           [Key, crdt_pncounter]),
    {ok, {_, ReadSet1, _}}= ReadResult1,
    ?assertMatch([1], ReadSet1),

    FirstNode = hd(Nodes),
    WriteResult1 = rpc:call(FirstNode, antidote, clocksi_bulk_update,
                            [[{update, Key, Type, {increment, a}}]]),
    ?assertMatch({ok, _}, WriteResult1),
    ReadResult2= rpc:call(FirstNode, antidote, clocksi_read,
                          [Key, crdt_pncounter]),
    {ok, {_, ReadSet2, _}}=ReadResult2,
    ?assertMatch([2], ReadSet2),
    lager:info("Test3 passed"),
    pass.

%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocksi_single_key_update_read_test(Nodes) ->
    lager:info("Test3 started"),
    FirstNode = hd(Nodes),
    Key = k3,
    Type = crdt_pncounter,
    Result= rpc:call(FirstNode, antidote, clocksi_bulk_update,
                     [
                      [{update, Key, Type, {increment, a}}]]),
    ?assertMatch({ok, _}, Result),
    {ok,{_,_,CommitTime}} = Result,
    Result2= rpc:call(FirstNode, antidote, clocksi_read,
                      [CommitTime, Key, crdt_pncounter]),
    {ok, {_, ReadSet, _}}=Result2,
    ?assertMatch([1], ReadSet),
    lager:info("Test3 passed"),
    pass.


%% @doc Read an update a key multiple times.
clocksi_multiple_read_update_test(Nodes) ->
    Node = hd(Nodes),
    Key = get_random_key(),
    NTimes = 100,
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key, crdt_pncounter]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1,NTimes)),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key, crdt_pncounter]),
    ?assertEqual(Result1+NTimes, Result2),
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).
%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    
    Type = crdt_pncounter,
    {ok,Result1} = rpc:call(Node, antidote, read,
                            [Key, Type]),
    {ok,_} = rpc:call(Node, antidote, clocksi_bulk_update,
                      [[{update, Key, Type, {increment,a}}]]),
    {ok,Result2} = rpc:call(Node, antidote, read,
                            [Key, Type]),
    ?assertEqual(Result1+1,Result2),
    pass.
