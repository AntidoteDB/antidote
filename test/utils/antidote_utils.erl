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
-include("include/antidote.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TYPE_PNC, antidote_crdt_counter_pn).

%% API
-export([
    increment_pn_counter/3,

    read_pn_counter/3,
    bcounter_read_single/3,
    bcounter_read_single/4,
    bcounter_check_permissions/2,
    bcounter_check_read_value/4,
    bcounter_check_read_value/5,
    bcounter_get_decrement_op/2,
    bcounter_get_increment_op/2,
    bcounter_get_transfer_op/3,
    bcounter_update_single/4,
    bcounter_update_single/5,
    bcounter_update_single_retry/5,
    bcounter_update_single_retry/6,
    get_dcid/1,

    %% clocksi
    check_read/5,
    check_read/6,
    check_read_key/7,
    check_read_key/8,
    check_read_keys/7,
    update_counters/6,
    update_counters/7,
    update_sets/5,
    spawn_com/2,
    spawn_read/6,
    get_random_key/0,
    find_key_same_node/3,
    atomic_write_txn/6,
    atomic_read_txn/6
    , update_sets_clock/5]).


increment_pn_counter(Node, Key, Bucket) ->
    Obj = {Key, ?TYPE_PNC, Bucket},
    WriteResult = rpc:call(Node, antidote, update_objects, [ignore, [], [{Obj, increment, 1}]]),
    ?assertMatch({ok, _}, WriteResult),
    ok.


read_pn_counter(Node, Key, Bucket) ->
    Obj = {Key, ?TYPE_PNC, Bucket},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [ignore, [], [Obj]]),
    {Value, CommitTime}.

-spec bcounter_read_single(node(), key(), bucket()) -> {antidote_crdt_counter_b:antidote_crdt_counter_b(), snapshot_time()}.
bcounter_read_single(Node, Key, Bucket) ->
    bcounter_read_single(Node, Key, Bucket, ignore).

-spec bcounter_read_single(node(), key(), bucket(), snapshot_time()) -> {antidote_crdt_counter_b:antidote_crdt_counter_b(), snapshot_time()}.
bcounter_read_single(Node, Key, Bucket, SnapshotTime) ->
    {ok, [BCounter], CommitTime} = rpc:call(Node, antidote, read_objects, [SnapshotTime, [], [{Key, antidote_crdt_counter_b, Bucket}]]),
    {BCounter, CommitTime}.

-spec bcounter_check_permissions(antidote_crdt_counter_b:antidote_crdt_counter_b(), non_neg_integer()) -> ok | no_return().
bcounter_check_permissions(BCounter, Expected) ->
    ?assertEqual(Expected, antidote_crdt_counter_b:permissions(BCounter)).

-spec bcounter_check_read_value(node(), key(), bucket(), non_neg_integer()) -> {antidote_crdt_counter_b:antidote_crdt_counter_b(), snapshot_time()} | no_return().
bcounter_check_read_value(Node, Key, Bucket, Expected) ->
    bcounter_check_read_value(Node, Key, Bucket, ignore, Expected).

-spec bcounter_check_read_value(node(), key(), bucket(), snapshot_time(), non_neg_integer()) -> {antidote_crdt_counter_b:antidote_crdt_counter_b(), snapshot_time()} | no_return().
bcounter_check_read_value(Node, Key, Bucket, SnapshotTime, Expected) ->
    Result = {BCounter, _} = bcounter_read_single(Node, Key, Bucket, SnapshotTime),
    bcounter_check_permissions(BCounter, Expected),
    Result.

-spec bcounter_update_single(node(), key(), bucket(), {increment | decrement, {pos_integer(), dcid()}} | {transfer, {pos_integer(), dcid(), dcid()}}) -> {ok, snapshot_time()} | {error, no_permissions}.
bcounter_update_single(Node, Key, Bucket, Update) ->
    bcounter_update_single(Node, Key, Bucket, ignore, Update).

-spec bcounter_update_single(node(), key(), bucket(), snapshot_time(), {increment | decrement, {pos_integer(), dcid()}} | {transfer, {pos_integer(), dcid(), dcid()}}) -> {ok, snapshot_time()} | {error, no_permissions}.
bcounter_update_single(Node, Key, Bucket, SnapshotTime, {UpdateOp, UpdateParam}) ->
    rpc:call(Node, antidote, update_objects, [SnapshotTime, [], [{{Key, antidote_crdt_counter_b, Bucket}, UpdateOp, UpdateParam}]]).

-spec bcounter_update_single_retry(node(), key(), bucket(), {increment | decrement, {pos_integer(), dcid()}} | {transfer, {pos_integer(), dcid(), dcid()}}, non_neg_integer()) -> {ok, snapshot_time()} | {error, no_permissions}.
bcounter_update_single_retry(Node, Key, Bucket, Update, Retries) ->
    bcounter_update_single_retry(Node, Key, Bucket, ignore, Update, Retries).

-spec bcounter_update_single_retry(node(), key(), bucket(), snapshot_time(), {increment | decrement, {pos_integer(), dcid()}} | {transfer, {pos_integer(), dcid(), dcid()}}, non_neg_integer()) -> {ok, snapshot_time()} | {error, no_permissions}.
bcounter_update_single_retry(Node, Key, Bucket, SnapshotTime, {UpdateOp, UpdateParam}, Retries) ->
    Result = rpc:call(Node, antidote, update_objects, [SnapshotTime, [], [{{Key, antidote_crdt_counter_b, Bucket}, UpdateOp, UpdateParam}]]),
    case Result of
        {ok, CommitTime} -> {ok, CommitTime};
        Error when Retries == 0 -> Error;
        _ ->
            timer:sleep(1000),
            bcounter_update_single_retry(Node, Key, Bucket, SnapshotTime, {UpdateOp, UpdateParam}, Retries - 1)
    end.

-spec bcounter_get_increment_op(node(), pos_integer()) -> {increment, {pos_integer(), dcid()}}.
bcounter_get_increment_op(Node, Amount) -> {increment, {Amount, get_dcid(Node)}}.

-spec bcounter_get_decrement_op(node(), pos_integer()) -> {decrement, {pos_integer(), dcid()}}.
bcounter_get_decrement_op(Node, Amount) -> {decrement, {Amount, get_dcid(Node)}}.

-spec bcounter_get_transfer_op(node(), pos_integer(), dcid()) -> {transfer, {pos_integer(), dcid(), dcid()}}.
bcounter_get_transfer_op(Node, Amount, ToDCID) ->
    {transfer, {Amount, ToDCID, get_dcid(Node)}}.

-spec get_dcid(node()) -> dcid().
get_dcid(Node) ->
    rpc:call(Node, dc_utilities, get_my_dc_id, []).

%% ------------------
%% From clocksi_SUITE
%% ------------------

check_read_key(Node, Key, Type, Expected, Clock, TxId, Bucket) ->
    check_read(Node, [{Key, Type, Bucket}], [Expected], Clock, TxId).

check_read_key(Node, Key, Type, Expected, Clock, TxId, Bucket, ProtocolModule) ->
    check_read(Node, [{Key, Type, Bucket}], [Expected], Clock, TxId, ProtocolModule).

check_read_keys(Node, Keys, Type, Expected, Clock, TxId, Bucket) ->
    Objects = lists:map(fun(Key) ->
        {Key, Type, Bucket}
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

update_counters(Node, Keys, IncValues, Clock, TxId, Bucket) ->
    update_counters(Node, Keys, IncValues, Clock, TxId, Bucket, cure).

update_counters(Node, Keys, IncValues, Clock, TxId, Bucket, ProtocolModule) ->
    Updates = lists:map(fun({Key, Inc}) ->
        {{Key, antidote_crdt_counter_pn, Bucket}, increment, Inc}
                        end,
        lists:zip(Keys, IncValues)
    ),

    case TxId of
        static ->
            {ok, CT} = rpc:call(Node, ProtocolModule, update_objects, [Clock, [], Updates]),
            {ok, CT};
        _ ->
            ok = rpc:call(Node, ProtocolModule, update_objects, [Updates, TxId]),
            ok
    end.


update_sets(Node, Keys, Ops, TxId, Bucket) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
        {{Key, antidote_crdt_set_aw, Bucket}, Op, Param}
                        end,
        lists:zip(Keys, Ops)
    ),
    ok = rpc:call(Node, antidote, update_objects, [Updates, TxId]),
    ok.


update_sets_clock(Node, Keys, Ops, Clock, Bucket) ->
    Updates = lists:map(fun({Key, {Op, Param}}) ->
        {{Key, antidote_crdt_set_aw, Bucket}, Op, Param}
                        end,
        lists:zip(Keys, Ops)
    ),
    {ok, CT} = rpc:call(Node, antidote, update_objects, [Clock, [], Updates]),
    {ok, CT}.


spawn_com(FirstNode, TxId) ->
    timer:sleep(3000),
    End1 = rpc:call(FirstNode, cure, clocksi_icommit, [TxId]),
    ?assertMatch({ok, {_Txid, _CausalSnapshot}}, End1).


spawn_read(Node, TxId, Return, Key, Type, Bucket) ->
    {ok, [Res]} = check_read_key(Node, Key, Type, 1, ignore, TxId, Bucket),
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
            find_key_same_node(FirstNode, IndexNode, Num + 1)
    end.


%% inter dc utils

atomic_write_txn(Node, Key1, Key2, Key3, _Type, Bucket) ->
    antidote_utils:update_counters(Node, [Key1, Key2, Key3], [1, 1, 1], ignore, static, Bucket, antidote).


atomic_read_txn(Node, Key1, Key2, Key3, Type, Bucket) ->
    {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    {ok, [R1]} = rpc:call(Node, antidote, read_objects,
        [[{Key1, Type, Bucket}], TxId]),
    {ok, [R2]} = rpc:call(Node, antidote, read_objects,
        [[{Key2, Type, Bucket}], TxId]),
    {ok, [R3]} = rpc:call(Node, antidote, read_objects,
        [[{Key3, Type, Bucket}], TxId]),
    rpc:call(Node, antidote, commit_transaction, [TxId]),
    ?assertEqual(R1, R2),
    ?assertEqual(R2, R3),
    R1.
