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

%% @doc Tests the append_list operation of the logging vnode
%%      by triggering read repair.
append_list_test(Nodes) ->
    [N1, _N2, N3, _N4, _N5, _N6] = Nodes,
    Key = key1,

    DocIdx = rpc:call(N1, riak_core_util, chash_key, [{<<"floppy">>, Key}]),
    Preflist = rpc:call(N1, riak_core_apl, get_primary_apl, [DocIdx, 3, replication]),
    NodesRep = [Node || {{_Index, Node}, primary} <- Preflist],
    Excluded = hd(lists:reverse(NodesRep)),
    lager:info("Nodes that replicate ~w: ~w",[Key, NodesRep]),

    F = fun(Node, Acc) ->
            case Node of
                Excluded ->
                    Acc;
                _ ->
                    [Node | Acc]
            end
        end,

    Partition1 = lists:foldl(F, [], lists:reverse(Nodes)),

    WriteResult = rpc:call(N3, floppy, append, [Key, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),

    PartInfo = rt:partition(Partition1, [Excluded]),

    Total = send_multiple_updates(N1, 10, 0, Key),
    lager:info("Total: ~w",[Total]),

    ok = rt:heal(PartInfo),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    Result = rpc:call(N1, floppy, read, [Key, riak_dt_gcounter]),
    lager:info("Value read: ~w",[Result]),
    ?assertEqual(Result, Total + 1),
    pass.

send_multiple_updates(Node, Total, Acc, Key) ->
    case Total of
        0 ->
            Acc;
        _ ->
            WriteResult = rpc:call(Node, floppy, append, [Key, {increment, ucl}]),
            ?assertMatch({ok, _}, WriteResult),
            send_multiple_updates(Node, Total - 1, Acc + 1, Key)
    end.
