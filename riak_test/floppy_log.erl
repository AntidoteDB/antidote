-module(floppy_log).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    mult_writes_one_read().

mult_writes_one_read() ->
    N = 6,
    ListIds = [random:uniform(N) || _ <- lists:seq(1, 120)],
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    F = fun(Elem, Acc) ->
            Node = lists:nth(Elem, Nodes),
            lager:info("Sending append to Node ~w~n",[Node]),
            WriteResult = rpc:call(Node, floppy, append, [abc, {increment, 4}]),
            ?assertMatch({ok, _}, WriteResult),
            Acc + 1
    end,

    Total = lists:foldl(F, 0, ListIds),
    FirstNode = hd(Nodes),
    ReadResult = rpc:call(FirstNode, floppy, read, [abc, riak_dt_gcounter]),
    lager:info("Read value: ~w~n",[ReadResult]),
    ?assertEqual(Total, ReadResult),
    pass.
