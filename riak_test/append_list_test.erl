-module(append_list_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    N=6,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    append_list_test(Nodes).

append_list_test(Nodes) ->
    [N1, N2, N3, N4, N5, N6] = Nodes,

    WriteResult = rpc:call(N3, floppy, append, [key1, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    PartInfo = rt:partition([N1, N2, N3, N5, N6], [N4]),
    
    Total = send_multiple_updates(N1, 10, 0),
    lager:info("Total: ~w",[Total]),
    
    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    Result = rpc:call(N1, floppy, read, [key1, riak_dt_gcounter]),
    lager:info("Value read: ~w",[Result]),
    ?assertEqual(Result, Total + 1),
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
            
