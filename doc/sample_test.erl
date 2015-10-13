-module(sample_test).

%% function confirm/0 is executed by the riak_test environment
-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->    
    N = 2,
    %% Create a cluster
    [Nodes] = rt:build_clusters([N]),
    rt:wait_until_ring_converged(Nodes),
    Node = hd(Nodes),
    Key = abc,
    Type = riak_dt_gcounter,
    Op = {increment, a},
    %% Increment a counter
    WriteResult = rpc:call(Node, antidote, append, 
                           [Key, Type, Op]),
    ?assertMatch({ok, _}, WriteResult),
    %% Read the counter and check the value returned
    ReadResult = rpc:call(Node,
                              antidote, read, [Key, Type]),
    lager:info("Read value: ~p", [ReadResult]),
    ?assertEqual({ok, 1}, ReadResult),
    pass.
