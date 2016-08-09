-module(multiple_dcs_bug_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up... new version!! "),
    rt:setup_harness(dummy, dummy),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all, [
			       {riak_core, [{ring_creation_size, NumVNodes}]}
			      ]),
    
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1, 1, 1]),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),
    
    {ok, Prot} = rpc:call(hd(Cluster1), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),
    
    lager:info("STARTING TESTS"),
    ok = common:setup_dc_manager([Cluster1, Cluster2, Cluster3], first_run),
    replicated_set_test(Cluster1, Cluster2, Cluster3),
    
    pass.

replicated_set_test(Cluster1, Cluster2, _Cluster3) ->
    Node1 = hd(Cluster1),
    Node2 = hd(Cluster2),

    Key1 = replicated_set_test,

    lager:info("Writing 100 elements to set!!!"),

    %% add 100 elements to the set on Node 1 while simultaneously reading on Node2
    CommitTimes = lists:map(fun(N) ->
				    lager:info("Writing ~p to set", [N]),
				    WriteResult1 = rpc:call(Node1, antidote, clocksi_execute_tx,
							    [[{update, {Key1, crdt_orset, {{add, N}, ucl}}}]]),
				    ?assertMatch({ok, _}, WriteResult1),
				    {ok, {_, _, CommitTime}} = WriteResult1,
				    
				    {ok, {_, [SetValue], _}} = rpc:call(Node2,
									antidote, clocksi_read,
									[ignore, Key1, crdt_orset]),
				    lager:info("Read value ~p", [SetValue]),
				    case length(SetValue) > 0 of
					true ->
					    ?assertEqual(lists:seq(1,lists:max(SetValue)), SetValue);
					false ->
					    ok
				    end,
				    timer:sleep(200),
				    CommitTime
			    end, lists:seq(1, 100)),
    
    LastCommitTime = lists:last(CommitTimes),
    lager:info("last commit time was ~p.", [LastCommitTime]),
    
    %% now read on Node2
    ReadResult = rpc:call(Node2,
			  antidote, clocksi_read,
			  [LastCommitTime, Key1, crdt_orset]),
    lager:info("Read value ~p.", [ReadResult]),
    {ok, {_, [SetValue], _}} = ReadResult,
    %% expecting to read values 1-100
    ?assertEqual(lists:seq(1, 100), SetValue),
    
    pass.
