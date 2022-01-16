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

-module(antidote_utils).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, test_utils:bucket(append_bucket)).
-define(TYPE_PNC, antidote_crdt_counter_pn).
-define(TYPE_B, antidote_crdt_counter_b).

%% API
-export([
    increment_pn_counter/2,

    read_pn_counter/2,
    read_pn_counter/3,
    read_b_counter/2,
    read_b_counter_commit/3,

    %% clocksi
    check_read/5,
    check_read/6,
    check_read_key/7,
    check_read_key/6,
    check_read_keys/6,
    update_counters/6,
    update_counters/5,
    update_sets/4,
    spawn_com/2,
    spawn_read/5,
    get_random_key/0,
    find_key_same_node/3,
    atomic_write_txn/5,
    atomic_read_txn/5
    , update_sets_clock/4]).


increment_pn_counter(Node, Key) ->
    Obj = {Key, ?TYPE_PNC, ?BUCKET},
    WriteResult = rpc:call(Node, antidote, update_objects, [ignore, [], [{Obj, increment, 1}]]),
    {ok, Vectorclock} = WriteResult,
    Vectorclock.


read_pn_counter(Node, Key) ->
    Obj = {Key, ?TYPE_PNC, ?BUCKET},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [ignore, [], [Obj]]),
    {Value, CommitTime}.

read_pn_counter(Node, Key, Clock) ->
    Obj = {Key, ?TYPE_PNC, ?BUCKET},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [Clock, [], [Obj]]),
    {Value, CommitTime}.


read_b_counter(Node, Key) ->
    read_b_counter_commit(Node, Key, ignore).

read_b_counter_commit(Node, Key, CommitTime) ->
    Obj = {Key, ?TYPE_B, ?BUCKET},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [CommitTime, [], [Obj]]),
    {?TYPE_B:permissions(Value), CommitTime}.









%% ------------------
%% From clocksi_SUITE
%% ------------------

check_read_key(Node, Key, Type, Expected, Clock, TxId) ->
    check_read(Node, [{Key, Type}], [Expected], Clock, TxId).

check_read_key(Node, Key, Type, Expected, Clock, TxId, ProtocolModule) ->
    check_read(Node, [{Key, Type}], [Expected], Clock, TxId, ProtocolModule).

check_read_keys(Node, Keys, Type, Expected, Clock, TxId) ->
    Objects = lists:map(fun(Key) ->
        {Key, Type}
                        end,
        Keys
    ),
    check_read(Node, Objects, Expected, Clock, TxId).

check_read(Node, Objects, Expected, Clock, TxId) ->
    check_read(Node, Objects, Expected, Clock, TxId, cure).

check_read(Node, Objects, Expected, Clock, TxId, ProtocolModule) ->
    case TxId of
        static ->
            {ok, Res, CT} = rpc:call(Node, ProtocolModule, read_objects, [Clock, [], Objects]),
            ?assertEqual(Expected, Res),
            {ok, Res, CT};
        _ ->
            {ok, Res} = rpc:call(Node, ProtocolModule, read_objects, [Objects, TxId]),
            ?assertEqual(Expected, Res),
            {ok, Res}
    end.

update_counters(Node, Keys, IncValues, Clock, TxId) ->
    update_counters(Node, Keys, IncValues, Clock, TxId, cure).

update_counters(Node, Keys, IncValues, Clock, TxId, ProtocolModule) ->
    Updates = lists:map(fun({Key, Inc}) ->
        {{Key, antidote_crdt_counter_pn}, increment, Inc}
                        end,
        lists:zip(Keys, IncValues)
    ),

    case TxId of
        static ->
            {ok, CT} = rpc:call(Node, ProtocolModule, update_objects, [Clock, [], Updates]),
            {ok, CT};
        _->
            ok = rpc:call(Node, ProtocolModule, update_objects, [Updates, TxId]),
            ok
    end.


update_sets(Node, Keys, Ops, TxId) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
        {{Key, antidote_crdt_set_aw}, Op, Param}
                        end,
        lists:zip(Keys, Ops)
    ),
    ok = rpc:call(Node, antidote, update_objects, [Updates, TxId]),
    ok.


update_sets_clock(Node, Keys, Ops, Clock) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
        {{Key, antidote_crdt_set_aw}, Op, Param}
                        end,
        lists:zip(Keys, Ops)
    ),
    {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates]),
    {ok, CT}.


spawn_com(FirstNode, TxId) ->
    timer:sleep(3000),
    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_, _}}, End1).


spawn_read(Node, TxId, Return, Key, Type) ->
    {ok, [Res]} = check_read_key(Node, Key, Type, 1, ignore, TxId),
    Return ! {self(), {ok, Res}}.


get_random_key() ->
    rand:seed(exsplus, {erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()}),
    rand:uniform(1000).  % TODO use deterministic keys in testcase


find_key_same_node(FirstNode, IndexNode, Num) ->
    NewKey = list_to_atom(atom_to_list(aaa) ++ integer_to_list(Num)),
    Preflist = rpc:call(FirstNode, log_utilities, get_preflist_from_key, [aaa]),
    case hd(Preflist) == IndexNode of
        true ->
            NewKey;
        false ->
            find_key_same_node(FirstNode, IndexNode, Num+1)
    end.



%% inter dc utils

atomic_write_txn(Node, Key1, Key2, Key3, _Type) ->
    antidote_utils:update_counters(Node, [Key1, Key2, Key3], [1, 1, 1], ignore, static, antidote).


atomic_read_txn(Node, Key1, Key2, Key3, Type) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [R1]} = rpc:call(Node, antidote, read_objects,
        [[{Key1, Type}], TxId]),
    {ok, [R2]} = rpc:call(Node, antidote, read_objects,
        [[{Key2, Type}], TxId]),
    {ok, [R3]} = rpc:call(Node, antidote, read_objects,
        [[{Key3, Type}], TxId]),
    rpc:call(Node, antidote, commit_transaction, [TxId]),
    ?assertEqual(R1, R2),
    ?assertEqual(R2, R3),
    R1.
