-module(inter_dc_repl_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    % Must be a power of 2, minimum 8 and maximum 1024.
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Cluster1, Cluster2] = rt:build_clusters([1,1]),

    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),

    rt:wait_until_registered(Node1, inter_dc_manager),
    rt:wait_until_registered(Node2, inter_dc_manager),

    {ok, DC1} = rpc:call(Node1, inter_dc_manager, start_receiver,[8091]),
    {ok, DC2} = rpc:call(Node2, inter_dc_manager, start_receiver,[8092]),

    lager:info("DCs: ~p and ~p", [DC1, DC2]),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    ok = rpc:call(Node1, inter_dc_manager, add_dc,[DC2]),
    ok = rpc:call(Node2, inter_dc_manager, add_dc,[DC1]),

    simple_replication_test(Cluster1, Cluster2),
    pass.

simple_replication_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    Result = rpc:call(Node1, antidote, read,
                      [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, Result),

    waituntil(fun() -> 
                      ReadResult = rpc:call(Node2,
                                            antidote, read,
                                            [key1, riak_dt_gcounter]),
                      {ok, Result1}= ReadResult,
                      3 == Result1
              end),


    {ok,_}= rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    {ok,_} = rpc:call(Node1,
                            antidote, append,
                            [key1, riak_dt_gcounter, {increment, ucl}]),
    waituntil(fun() -> 
                      {ok, Result2} = rpc:call(Node2,
                                            antidote, read,
                                            [key1, riak_dt_gcounter]),
                      5==Result2
              end),

    lager:info("Simple replication test passed!"),
    pass.


waituntil(Fun) ->
    case Fun() of
        true ->
            ok;
        false ->
            Fun()
    end.
