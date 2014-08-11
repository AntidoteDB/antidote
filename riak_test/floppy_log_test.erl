%% @doc mult_writes_one_read: Test that perform NumWrites increments to the key:abc.
%%      Each increment is sent to a random node of the cluster.
%%      Test norml behaviour of the logging layer
%%      Perflorms a read to the first node of the cluster to check whether all the
%%      increment operations where successfully applied.
%%  Input:  N:  Number of nodes
%%          Nodes: List of the nodes that belong to the built cluster.
%%

-module(floppy_log_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    N = 6,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    NumWrites = 120,
    ListIds = [random:uniform(N) || _ <- lists:seq(1, NumWrites)],

    F = fun(Elem, Acc) ->
            Node = lists:nth(Elem, Nodes),
            lager:info("Sending append to Node ~w~n",[Node]),
            WriteResult = rpc:call(Node,
                                   floppy, append, [abc, riak_dt_gcounter, {increment, 4}]),
            ?assertMatch({ok, _}, WriteResult),
            Acc + 1
    end,

    Total = lists:foldl(F, 0, ListIds),
    FirstNode = hd(Nodes),
    ReadResult = rpc:call(FirstNode,
                          floppy, read, [abc, riak_dt_gcounter]),
    lager:info("Read value: ~w~n",[ReadResult]),
    ?assertEqual({ok, Total}, ReadResult),

    pass.
