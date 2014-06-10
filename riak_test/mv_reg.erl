-module(mv_reg).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    assign_propagate().

assign_propagate() ->
    N = 3,
    ListIds = [random:uniform(N) || _ <- lists:seq(1, 10)],
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    F = fun(Elem) ->
            Node = lists:nth(Elem, Nodes),
            lager:info("Sending asign to Node ~w~n",[Node]),
            AssignResult = rpc:call(Node, floppy, append, [abc, {{assign, Elem}, actor1}]),
            ?assertMatch({ok, _}, AssignResult)
    end,

    lists:map(F, ListIds),
    FirstNode = hd(Nodes),
    Result = hd(lists:reverse(ListIds)),
    lager:info("Sending read to Node ~w~n",[FirstNode]),
    ReadResult = rpc:call(FirstNode, floppy, read, [abc, riak_dt_mvreg]),
    ?assertEqual([Result], ReadResult),

    PropagateResult1 = rpc:call(FirstNode, floppy, append, [abc, {{propagate, value2, [{actor2, 5}]}, actor1}]),
    ?assertMatch({ok, _}, PropagateResult1),
    ReadResult1 = rpc:call(FirstNode, floppy, read, [abc, riak_dt_mvreg]),
    Result1 = [value1, value2],
    ?assertEqual(lists:sort(Result1), lists:sort(ReadResult1)),

    PropagateResult2 = rpc:call(FirstNode, floppy, append, [abc, {{propagate, value3, [{actor2, 6}, {actor1, 11}]}, actor1}]),
    ?assertMatch({ok, _}, PropagateResult2),
    ReadResult2 = rpc:call(FirstNode, floppy, read, [abc, riak_dt_mvreg]),
    ?assertEqual([value3], ReadResult2),
    pass.
    
