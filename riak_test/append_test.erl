-module(append_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    Node = hd(Nodes),

    rt:log_to_nodes(Nodes, "Starting write operation 1"),

    WriteResult = rpc:call(Node,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    rt:log_to_nodes(Nodes, "Starting write operation 2"),

    WriteResult2 = rpc:call(Node,
                           floppy, append,
                           [key2, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),

    rt:log_to_nodes(Nodes, "Starting read operation 1"),

    ReadResult1 = rpc:call(Node,
                           floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 1}, ReadResult1),

    rt:log_to_nodes(Nodes, "Starting read operation 2"),

    ReadResult2 = rpc:call(Node,
                           floppy, read,
                           [key2, riak_dt_gcounter]),
    ?assertEqual({ok, 1}, ReadResult2),

    pass.
