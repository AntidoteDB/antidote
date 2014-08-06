-module(repfsm_error_handle_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([6]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    handle_error_test(Nodes).

handle_error_test(Nodes) ->
    [N1, N2, N3, N4, N5, N6] = Nodes,

    %% Test when network partition happens, if error handling is happening.
    PartInfo = rt:partition([N1, N2, N4, N5, N6], [N3]),

    WriteResult1 = rpc:call(N2, floppy, append, [key1, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),

    ReadResult2 = rpc:call(N3, floppy, read, [key1, riak_dt_gcounter]),
    ?assertMatch({error, quorum_unreachable}, ReadResult2),

    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    %% Test that values are actually written after network partition
    ReadResult3 = rpc:call(N3, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[ReadResult3]),
    ?assertEqual(1, ReadResult3),

    ReadResult4 = rpc:call(N3, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[ReadResult3]),
    ?assertEqual(1, ReadResult4),

    _PartInfo2 = rt:partition([N1, N2, N3, N4, N5], [N6]),

    %% Make sure that read repair is working
    ReadResult5 = rpc:call(N6, floppy, read, [key1, riak_dt_gcounter]),
    ?assertMatch({error, can_not_reach_vnode}, ReadResult5),

    pass.
