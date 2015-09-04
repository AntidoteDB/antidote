-module(oprga_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    simple_test(),
    rt:clean_cluster(Nodes),
    pass.

simple_test() ->
    ?assert(true).