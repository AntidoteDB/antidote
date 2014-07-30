-module(interdc).
-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Dc1,Dc2] = rt:build_clusters([3,3]),
    lager:info("Nodes: ~p -p", [Dc2,Dc2]),
    simplereadwrite_test([Dc1,Dc2]),
    ok.
    

simplereadwrite_test([Dc1,Dc2]) ->
    FirstNode = hd(Dc1),
    Key = abc,
    Result=rpc:call(FirstNode, floppy, clocksi_execute_tx, [now(), 
    [{update, 11, {increment, a}}]]), 
    {ok, {_, ReadSet, _}}=Result,

    Node2 = hd(Dc2),    
    Result=rpc:call(Node2, floppy, clocksi_read, [now(), Key, riak_dt_pncounter]), 
    {ok, {_, ReadSet, _}}=Result,             
    
    ?assertMatch([1], ReadSet),    
    ok.
