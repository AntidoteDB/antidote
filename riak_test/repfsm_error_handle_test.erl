-module(repfsm_error_handle_test).

-export([confirm/0, send_multiple_updates/3]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    N=3,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    handle_error_test(Nodes).

handle_error_test(Nodes) ->
    [N1, N2, N3] = Nodes,

    WriteResult = rpc:call(N3, floppy, append, [key1, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    PartInfo = rt:partition([N1, N2], [N3]),
    
    %Total = send_multiple_updates(N1, 10, 0),
    WriteResult1 = rpc:call(N1, floppy, append, [key1, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    %lager:info("Total: ~w",[Total]),

    WriteResult2 = rpc:call(N3, floppy, append, [key2, {increment, ucl}]),
    ?assertMatch({error, _}, WriteResult2),
    {error, Reason} = WriteResult1,
    lager:info("Reason: ~w",[Reason]),
    
    ReadResult1 = rpc:call(N1, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[ReadResult1]),

    ReadResult2 = rpc:call(N3, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[ReadResult2]),

    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    ReadResult3 = rpc:call(N3, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[ReadResult3]),

    pass.
    
send_multiple_updates(Node, Total, Acc) ->
    case Total of
        0 ->
            Acc;
        _ ->
            WriteResult = rpc:call(Node, floppy, append, [key1, {increment, ucl}]),
            ?assertMatch({ok, _}, WriteResult),
            send_multiple_updates(Node, Total - 1, Acc + 1)
    end.
