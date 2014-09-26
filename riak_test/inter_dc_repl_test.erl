-module(inter_dc_repl_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Cluster1, Cluster2] = rt:build_clusters([1,1]),
    Node2 = hd(Cluster2),
    lager:info("NODE 2 = ~p",[Node2]),
    Result = rpc:call(Node2, floppy_sup, start_rep,[8091]),
    lager:info("Sup start result ~p", [Result]),
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    simple_replication_test(Cluster1, Cluster2),
    multiple_keys_test(Cluster1, Cluster2),
    causality_test(Cluster1,Cluster2),
    pass.

simple_replication_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    WriteResult1 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    Result = rpc:call(Node1, floppy, read,
                           [key1, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, Result),

    ReadResult = rpc:call(Node2,
                           floppy, clocksi_read,
                           [CommitTime, key1, riak_dt_gcounter]),
    {ok, {_,[ReadSet],_} }= ReadResult,
    ?assertEqual(3, ReadSet),
    lager:info("Simple replication test passed!"),
    pass.

multiple_keys_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    lists:foreach( fun(_) ->
                            multiple_writes(Node1, 1, 10, rpl)
                   end,
                   lists:seq(1,10)),
    WriteResult3 = rpc:call(Node1,
                           floppy, append,
                           [key1, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,

    Result1 = multiple_reads(Node1, 1, 10, 10,CommitTime),
    ?assertEqual(length(Result1), 0),
    Result2 = multiple_reads(Node2, 1, 10, 10, CommitTime),
    ?assertEqual(length(Result2), 0),
    lager:info("Multiple key read-write test passed!"),
    pass.

multiple_writes(Node, Start, End, Actor)->
    F = fun(N, Acc) ->
            case rpc:call(Node, floppy, append,
                          [N, riak_dt_gcounter,
                           {{increment, 1}, Actor}]) of
                {ok, _} ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).

multiple_reads(Node, Start, End, Total, CommitTime) ->
    F = fun(N, Acc) ->
            case rpc:call(Node, floppy, clocksi_read, [CommitTime, N, riak_dt_gcounter]) of
                {error, _} ->
                    [{N, error} | Acc];
                {ok, {_,[Value],_}} ->
                    ?assertEqual(Value, Total),
                    Acc
            end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).

causality_test(Cluster1, Cluster2) ->
    %% add element e to orset in one DC
    %% remove element e from other DC
    %% result set should not contain e
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key = myset,
    %% Add two elements in DC1
    AddResult1 = rpc:call(Node1,
                           floppy, append,
                           [Key, riak_dt_orset, {{add, first}, act1}]),
    ?assertMatch({ok, _}, AddResult1),
    AddResult2 = rpc:call(Node1,
                           floppy, append,
                           [Key, riak_dt_orset, {{add, second}, act1}]),
    ?assertMatch({ok, _}, AddResult2),
    {ok,{_,_,CommitTime}}=AddResult2,

    %% Remove one element from D2C
    RemoveResult = rpc:call(Node2,
                           floppy, clocksi_bulk_update,
                            [CommitTime,
                             [{update, Key, riak_dt_orset, {{remove, first}, act1}}]]),
    ?assertMatch({ok, _}, RemoveResult),
    %% Read result
    Result = rpc:call(Node2, floppy, read,
                           [Key, riak_dt_orset]),
    ?assertMatch({ok, [second]}, Result),
    lager:info("Causality test passed!"),
    pass.
