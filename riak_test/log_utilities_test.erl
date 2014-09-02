-module(log_utilities_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Cluster] = rt:build_clusters([1]),
    Node = hd(Cluster),
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster), 
    Key = log,
    Preflist_key = rpc:call(Node, log_utilities, get_preflist_from_key, [Key]),
    LogId = rpc:call(Node, log_utilities, get_logid_from_key, [Key]),
    Preflist_logid = rpc:call(Node, log_utilities, get_preflist_from_logid, [LogId]),   
    ?assertMatch(Preflist_key, Preflist_logid),
    {Partition, _Node} = hd(Preflist_key),
    Logid_p =  rpc:call(Node, log_utilities, get_logid_from_partition, [Partition]),  
    ?assertMatch(LogId, Logid_p),
    pass.

