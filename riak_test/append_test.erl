-module(append_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    Node = hd(Nodes),

    WriteResult = rpc:call(Node,
                           floppy, append,
                           [key, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    pass.
