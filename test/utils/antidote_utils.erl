%%%-------------------------------------------------------------------
%%% Created : 04. September 2018
%%%-------------------------------------------------------------------
-module(antidote_utils).

-include_lib("eunit/include/eunit.hrl").

-define(TYPE_PNC, antidote_crdt_counter_pn).
-define(TYPE_B, antidote_crdt_counter_b).

%% API
-export([
    increment_pn_counter/3,

    read_pn_counter/3,
    read_b_counter/3,
    read_b_counter_commit/4,

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


read_b_counter(Node, Key, Bucket) ->
    read_b_counter_commit(Node, Key, Bucket, ignore).

read_b_counter_commit(Node, Key, Bucket, CommitTime) ->
    Obj = {Key, ?TYPE_B, Bucket},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [CommitTime, [], [Obj]]),
    {?TYPE_B:permissions(Value), CommitTime}.









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
        _->
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
    rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    rand_compat:uniform(1000).  % TODO use deterministic keys in testcase


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

