-module(append_failures_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([6]),
    N = hd(Nodes),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    %% Identify preference list for a given key.
    Preflist = rpc:call(N, log_utilities, get_preflist_from_key, [key1]),
    lager:info("Preference list: ~p", [Preflist]),

    NodeList = [Node || {_Index, Node} <- Preflist],
    lager:info("Responsible nodes for key: ~p", [NodeList]),

    {A, _} = lists:split(1, NodeList),
    lager:info("About to partition: ~p from: ~p", [A, Nodes -- A]),

    %% Partition the network.
    _PartInfo = rt:partition(A, Nodes -- A),
    First = hd(A),

    %% Write to the minority partition.
    WriteResult = rpc:call(First, floppy, append, [key1, {increment, ucl}]),
    ?assertMatch({error, quorum_unreachable}, WriteResult),

    %% Read from the minority partition.
    ReadResult = rpc:call(First, floppy, read, [key1, riak_dt_gcounter]),
    ?assertMatch({error, quorum_unreachable}, ReadResult),

    pass.
