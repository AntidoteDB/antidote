-module(oprga_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    empty_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

empty_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("empty_test started"),
    Type = crdt_rga,
    Key = key_empty,
    Result0 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    ?assertMatch({ok, []}, Result0).
