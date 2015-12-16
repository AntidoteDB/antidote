-module(inter_dc_repl_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),

    Clean = rt_config:get(clean_cluster, true),

    [Cluster1, Cluster2] = rt:build_clusters([1,1]),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    {ok, Prot} = rpc:call(hd(Cluster1), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),

    ok = common:setup_dc_manager([Cluster1, Cluster2], first_run),
    simple_replication_test(Cluster1, Cluster2),

    [Cluster3, Cluster4] = common:clean_clusters([Cluster1, Cluster2]),
    ok = common:setup_dc_manager([Cluster3, Cluster4], Clean),
    multiple_keys_test(Cluster3, Cluster4),

    [Cluster5, Cluster6] = common:clean_clusters([Cluster3, Cluster4]),
    ok = common:setup_dc_manager([Cluster5, Cluster6], Clean),
    causality_test(Cluster5, Cluster6),

    [Cluster7, Cluster8] = common:clean_clusters([Cluster5, Cluster6]),
    ok = common:setup_dc_manager([Cluster7, Cluster8], Clean),
    atomicity_test(Cluster7, Cluster8),

    pass.

simple_replication_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key = simple_replication_test,
    WriteResult1 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl1}]),
    ?assertMatch({ok, _}, WriteResult1),
    WriteResult2 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl2}]),
    ?assertMatch({ok, _}, WriteResult2),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl3}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,
    Result = rpc:call(Node1, antidote, read,
                      [Key, riak_dt_gcounter]),
    ?assertEqual({ok, 3}, Result),

    ReadResult = rpc:call(Node2,
                          antidote, clocksi_read,
                          [CommitTime, Key, riak_dt_gcounter]),
    {ok, {_,[ReadSet],_} }= ReadResult,
    ?assertEqual(3, ReadSet),
    lager:info("Simple replication test passed!"),
    pass.

multiple_keys_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key = multiple_keys_test,
    lists:foreach( fun(_) ->
                           multiple_writes(Node1, Key, 1, 10, rpl)
                   end,
                   lists:seq(1,10)),
    WriteResult3 = rpc:call(Node1,
                            antidote, append,
                            [Key, riak_dt_gcounter, {increment, ucl1}]),
    ?assertMatch({ok, _}, WriteResult3),
    {ok,{_,_,CommitTime}}=WriteResult3,

    Result1 = multiple_reads(Node1, Key, 1, 10, 10,CommitTime),
    ?assertEqual(length(Result1), 0),
    Result2 = multiple_reads(Node2, Key, 1, 10, 10, CommitTime),
    ?assertEqual(length(Result2), 0),
    lager:info("Multiple key read-write test passed!"),
    pass.

multiple_writes(Node, PreKey, Start, End, Actor)->
    F = fun(N, Acc) ->
                Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
                case rpc:call(Node, antidote, append,
                              [Key, riak_dt_gcounter,
                               {{increment, 1}, Actor}]) of
                    {ok, _} ->
                        Acc;
                    Other ->
                        [{Key, Other} | Acc]
                end
        end,
    lists:foldl(F, [], lists:seq(Start, End)).

multiple_reads(Node, PreKey, Start, End, Total, CommitTime) ->
    F = fun(N, Acc) ->
                Key = list_to_atom(atom_to_list(PreKey) ++ [N]),
                case rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, riak_dt_gcounter]) of
                    {error, _} ->
                        [{Key, error} | Acc];
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
    Key = causality_test,
    %% Add two elements in DC1
    AddResult1 = rpc:call(Node1,
                          antidote, append,
                          [Key, riak_dt_orset, {{add, first}, act1}]),
    ?assertMatch({ok, _}, AddResult1),
    AddResult2 = rpc:call(Node1,
                          antidote, append,
                          [Key, riak_dt_orset, {{add, second}, act2}]),
    ?assertMatch({ok, _}, AddResult2),
    {ok,{_,_,CommitTime}}=AddResult2,

    %% Remove one element from D2C
    RemoveResult = rpc:call(Node2,
                            antidote, clocksi_bulk_update,
                            [CommitTime,
                             [{update, {Key, riak_dt_orset, {{remove, first}, act3}}}]]),
    ?assertMatch({ok, _}, RemoveResult),
    %% Read result
    Result = rpc:call(Node2, antidote, read,
                      [Key, riak_dt_orset]),
    ?assertMatch({ok, [second]}, Result),
    lager:info("Causality test passed!"),
    pass.

%% This tests checks reads are atomic when replicated to other DCs
%% TODO: need more deterministic test
atomicity_test(Cluster1, Cluster2) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),
    Key1 = atomicity_test1,
    Key2 = atomicity_test2,
    Key3 = atomicity_test3,
    Caller = self(),
    ContWrite = fun() ->
                        lists:foreach(
                          fun(_) ->
                                  atomic_write_txn(Node1, Key1, Key2, Key3)
                          end, lists:seq(1,10)),
                        Caller ! writedone,
                        lager:info("Atomic writes done")
                end,
    ContRead = fun() ->
                       lists:foreach(
                         fun(_) ->
                                 atomic_read_txn(Node2, Key1, Key2, Key3)
                         end, lists:seq(1,20)),
                       Caller ! readdone,
                       lager:info("Atomic reads done")
               end,
    spawn(ContWrite),
    spawn(ContRead),
    receive
        writedone ->
            receive
                readdone ->
                    pass
            end,
            pass
    end.

atomic_write_txn(Node, Key1, Key2, Key3) ->
    Type = riak_dt_gcounter,
    Result= rpc:call(Node, antidote, clocksi_bulk_update,
                     [
                      [{update, {Key1, Type, {increment, a}}},
                       {update, {Key2, Type, {increment, a}}},
                       {update, {Key3, Type, {increment, a}}}
                      ]]),
    ?assertMatch({ok, _}, Result).

atomic_read_txn(Node, Key1, Key2, Key3) ->
    Type = riak_dt_gcounter,
    {ok,TxId} = rpc:call(Node, antidote, clocksi_istart_tx, []),
    {ok, R1} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key1, Type]),
    {ok, R2} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key2, Type]),
    {ok, R3} = rpc:call(Node, antidote, clocksi_iread,
                        [TxId, Key3, Type]),
    ?assertEqual(R1,R2),
    ?assertEqual(R2,R3).
