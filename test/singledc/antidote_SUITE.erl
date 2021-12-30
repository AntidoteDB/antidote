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

%% @doc antidote_SUITE:
%%    Test the basic api of antidote
%%    static and interactive transactions with single and multiple Objects
%%    interactive transaction with abort
-module(antidote_SUITE).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

%% tests
-export([
         static_txn_single_object/1,
         static_txn_single_object_clock/1,
         static_txn_multi_objects/1,
         static_txn_multi_objects_clock/1,
         interactive_txn/1,
         interactive_txn_abort/1,
         interactive_txn_validate_or_read/1,
         interactive_txn_validate_or_read_concurrent/1,
         interactive_txn_validate_or_read_multiple/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(antidote_bucket)).

init_per_suite(Config) ->
    test_utils:init_single_dc(?MODULE, Config).


end_per_suite(Config) ->
    Config.


init_per_testcase(_Name, Config) ->
    Config.


end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.


all() ->
    [
     static_txn_single_object,
     static_txn_single_object_clock,
     static_txn_multi_objects,
     static_txn_multi_objects_clock,
     interactive_txn,
     interactive_txn_abort,
     interactive_txn_validate_or_read,
     interactive_txn_validate_or_read_concurrent,
     interactive_txn_validate_or_read_multiple
    ].


static_txn_single_object(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = antidote_key_static1,
    Type = antidote_crdt_counter_pn,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},

    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], [Update]]),
    {ok, [Val], _} = rpc:call(Node, antidote, read_objects, [ignore, [], [Object]]),
    ?assertEqual(1, Val).


static_txn_single_object_clock(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Key = antidote_key_static2,
    Type = antidote_crdt_counter_pn,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},

    {ok, Clock1} = rpc:call(Node, antidote, update_objects, [ignore, [], [Update]]),
    {ok, [Val1], Clock2} = rpc:call(Node, antidote, read_objects, [Clock1, [], [Object]]),
    ?assertEqual(1, Val1),
    {ok, Clock3} = rpc:call(Node, antidote, update_objects, [Clock2, [], [Update]]),
    {ok, [Val2], _Clock4} = rpc:call(Node, antidote, read_objects, [Clock3, [], [Object]]),
    ?assertEqual(2, Val2).


static_txn_multi_objects(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,
    Keys = [antidote_static_m1, antidote_static_m2, antidote_static_m3, antidote_static_m4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),

    {ok, _} = rpc:call(Node, antidote, update_objects, [ignore, [], Updates]),
    {ok, Res, _} = rpc:call(Node, antidote, read_objects, [ignore, [], Objects]),
    ?assertEqual([1, 2, 3, 4], Res).


static_txn_multi_objects_clock(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,
    Keys = [antidote_static_mc1, antidote_static_mc2, antidote_static_mc3, antidote_static_mc4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),

    {ok, Clock1} = rpc:call(Node, antidote, update_objects, [ignore, [], Updates]),
    {ok, Res1, Clock2} = rpc:call(Node, antidote, read_objects, [Clock1, [], Objects]),
    ?assertEqual([1, 2, 3, 4], Res1),

    {ok, Clock3} = rpc:call(Node, antidote, update_objects, [Clock2, [], Updates]),
    {ok, Res2, _} = rpc:call(Node, antidote, read_objects, [Clock3, [], Objects]),
    ?assertEqual([2, 4, 6, 8], Res2).


interactive_txn(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,
    Keys = [antidote_int_m1, antidote_int_m2, antidote_int_m3, antidote_int_m4],
    IncValues = [1, 2, 3, 4],
    Objects = lists:map(fun(Key) ->
                                {Key, Type, Bucket}
                        end, Keys
                       ),
    Updates = lists:map(fun({Object, IncVal}) ->
                                {Object, increment, IncVal}
                        end, lists:zip(Objects, IncValues)),
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    %% update objects one by one.
    txn_seq_update_check(Node, TxId, Updates),
    %% read objects one by one
    txn_seq_read_check(Node, TxId, Objects, [1, 2, 3, 4]),
    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId]),

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
    %% read objects all at once
    {ok, Res} = rpc:call(Node, antidote, read_objects, [Objects, TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertEqual([1, 2, 3, 4], Res).


interactive_txn_abort(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,
    Key = antidote_int_abort_m1,
    Object = {Key, Type, Bucket},
    Update = {Object, increment, 1},
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    ok = rpc:call(Node, antidote, update_objects, [[Update], TxId]),
    ok = rpc:call(Node, antidote, abort_transaction, [TxId]), % must abort successfully

    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    %% read object
    {ok, Res} = rpc:call(Node, antidote, read_objects, [[Object], TxId2]),
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]),
    ?assertEqual([0], Res). % prev txn is aborted so read returns 0


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

interactive_txn_validate_or_read(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,

    Object1 = {vor_key1, Type, Bucket},

    {ok, TxId1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    {ok, Res1} = rpc:call(Node, antidote, read_objects, [[Object1], TxId1]),
    ?assertEqual([0], Res1),

    % An empty token is invalid and returns the correct next token
    {ok, Res2} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [<<>>], TxId1]),
    [{invalid, 0, Token1}] = Res2,

    % With a correct token, no new value is returned.
    {ok, Res3} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [Token1], TxId1]),
    ?assertEqual([valid], Res3),

    {ok, Clock} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [Clock, []]),

    % Tokens works across transaction boundaries.
    {ok, Res4} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [Token1], TxId2]),
    ?assertEqual([valid], Res4),

    Update = {Object1, increment, 1},
    ok = rpc:call(Node, antidote, update_objects, [[Update], TxId2]),

    % Local updates discard the validate_or_read optim, token will always
    % be invalid with this transaction.
    {ok, Res5} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [Token1], TxId2]),
    ?assertEqual([{invalid, 1, <<>>}], Res5),

    {ok, Res6} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [<<>>], TxId2]),
    ?assertEqual([{invalid, 1, <<>>}], Res6),

    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxId2]).

interactive_txn_validate_or_read_concurrent(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,

    Object1 = {vor_key1, Type, Bucket},

    {ok, TxIdA1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, TxIdB1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    % On the first read of an empty object without a min transaction time, there is no
    % other choice but to return an invalid token.
    {ok, ResA1} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [<<>>], TxIdA1]),
    ?assertEqual([{invalid, 0, <<>>}], ResA1),

    {ok, ResB1} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [<<>>], TxIdB1]),
    [{invalid, 0, TokenB1}] = ResB1,

    % It should work on both transaction.
    {ok, ResA2} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [TokenB1], TxIdA1]),
    ?assertEqual([valid], ResA2),

    {ok, ResB2} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [TokenB1], TxIdB1]),
    ?assertEqual(ResA2, ResB2),

    % Local updates doesn't affect the other transaction.
    Update = {Object1, increment, 1},
    ok = rpc:call(Node, antidote, update_objects, [[Update], TxIdA1]),

    {ok, ResB3} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [TokenB1], TxIdB1]),
    ?assertEqual([valid], ResB3),

    % Commit updates from A, check B.
    {ok, ClockA1} = rpc:call(Node, antidote, commit_transaction, [TxIdA1]),

    {ok, ResB3} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [TokenB1], TxIdB1]),
    ?assertEqual([valid], ResB3),

    % However restarting a transaction which causally depends on A1
    % correctly mismatches the previous token.
    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxIdB1]),
    {ok, TxIdB2} = rpc:call(Node, antidote, start_transaction, [ClockA1, []]),

    {ok, ResB4} = rpc:call(Node, antidote, validate_or_read_objects, [[Object1], [TokenB1], TxIdB2]),
    ?assertMatch([{invalid, 1, _}], ResB4),

    {ok, _} = rpc:call(Node, antidote, commit_transaction, [TxIdB2]).

interactive_txn_validate_or_read_multiple(Config) ->
    Bucket = ?BUCKET,
    Node = proplists:get_value(node, Config),
    Type = antidote_crdt_counter_pn,

    ObjectA = {vor_key1, Type, Bucket},
    ObjectB = {vor_key2, Type, Bucket},
    Objects = [ObjectA, ObjectB],

    {ok, TxId1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    {ok, _Res1} = rpc:call(Node, antidote, read_objects, [Objects, TxId1]),

    {ok, Res2} = rpc:call(Node, antidote, validate_or_read_objects,
                          [Objects, [<<>>, <<>>], TxId1]),
    ?assertMatch([{invalid, 0, _}, {invalid, 0, _}], Res2),
    [{_, _, TokenObjectA1}, {_, _, TokenObjectB1}] = Res2,

    {ok, Res3} = rpc:call(Node, antidote, validate_or_read_objects,
                          [Objects, [TokenObjectA1, TokenObjectB1], TxId1]),
    ?assertEqual([valid, valid], Res3),

    % Update one object and checks that it doesn't affect the other.
    Update = {ObjectA, increment, 1},
    ok = rpc:call(Node, antidote, update_objects, [[Update], TxId1]),

    % Both objects lies on the same partition, the local update discard both
    % token.
    {ok, Res4} = rpc:call(Node, antidote, validate_or_read_objects,
                          [Objects, [TokenObjectA1, TokenObjectB1], TxId1]),
    ?assertEqual(
        [{invalid, 1, <<>>},
         {invalid, 0, <<>>}], Res4),

    % On a new transaction token for B is still valid.
    {ok, _ClockTxId1} = rpc:call(Node, antidote, commit_transaction, [TxId1]),
    {ok, TxId2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),

    {ok, Res5} = rpc:call(Node, antidote, validate_or_read_objects,
                          [Objects, [TokenObjectA1, TokenObjectB1], TxId2]),
    ?assertMatch([{invalid, 1, _}, valid], Res5),
    [{_, _, TokenObjectA2}, valid] = Res5,

    {ok, Res6} = rpc:call(Node, antidote, validate_or_read_objects,
                          [Objects, [TokenObjectA2, TokenObjectB1], TxId2]),
    ?assertEqual([valid, valid], Res6).
