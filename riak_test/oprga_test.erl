-module(oprga_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    empty_test(Nodes),
    add_test(Nodes),
    remove_test(Nodes),
    insert_after_remove_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

empty_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Empty test started"),
    Type = crdt_rga,
    Key = key_empty,
    Result0 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, []}, Result0),
    lager:info("Empty test OK").

add_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Add test started"),
    Type = crdt_rga,
    Key = key_add,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, a, 1}, ucl}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}]}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, b, 1}, ucl}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}]}, Result3),
    Result4 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, c, 2}, ucl}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {ok, c, _}]}, Result5),
    lager:info("Add test OK").


remove_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Remove test started"),
    Type = crdt_rga,
    Key = key_remove,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, a, 1}, ucl}},
            {update, Key, Type, {{addRight, b, 1}, ucl}},
            {update, Key, Type, {{addRight, c, 2}, ucl}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {ok, c, _}]}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{remove, 2}, ucl}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {deleted, b, _}, {ok, c, _}]}, Result3),
    Result4 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{remove, 1}, ucl}}, {update, Key, Type, {{remove, 3}, ucl}}]]),
    ?assertMatch({ok, _}, Result4),
    Result5 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{deleted, a, _}, {deleted, b, _}, {deleted, c, _}]}, Result5),
    lager:info("Remove test OK").

insert_after_remove_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Remove-Insert test started"),
    Type = crdt_rga,
    Key = key_remove_insert,
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, a, 1}, ucl}},
            {update, Key, Type, {{addRight, b, 1}, ucl}},
            {update, Key, Type, {{addRight, c, 2}, ucl}}]]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{remove, 3}, ucl}}]]),
    ?assertMatch({ok, _}, Result1),
    Result2 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, Key, Type, {{addRight, d, 3}, ucl}}]]),
    ?assertMatch({ok, _}, Result2),
    Result3 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, [{ok, a, _}, {ok, b, _}, {deleted, c, _}, {ok, d, _}]}, Result3),
    lager:info("Remove-Insert test OK").