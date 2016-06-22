-module(log_recovery_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all, [
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    [Nodes] = rt:build_clusters([3]),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes), fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Nodes: ~p", [Nodes]),
    
    read_pncounter_log_recovery_test(Nodes),

    pass.

%% First we remember the initial time of the counter (with value 0).
%% After 15 updates, we kill the nodes
%% We then restart the nodes, aand read the value
%% being sure that all 15 updates we loaded from the log
read_pncounter_log_recovery_test(Nodes) ->
    lager:info("read_snapshot_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = crdt_pncounter,
    Key = log_value_test,
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, Key, 15),
    %% old read value is 0
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual(0, ReadResult1),
    %% most recent read value is 15
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode,
        antidote, clocksi_read, [Key, Type]),
    ?assertEqual(15, ReadResult2),
    lager:info("Killing and restarting the nodes"),
    %% Shut down the nodes
    %% Sleep a few seconds to let the log be written to disk
    timer:sleep(5000),
    lists:foreach(fun(ANode) ->
			  rt:brutal_kill(ANode)
		  end, Nodes),
    lists:foreach(fun(ANode) ->
			  rt:start_and_wait(ANode)
		  end, Nodes),
    rt:wait_until_ring_converged(Nodes),
    lager:info("Waiting until vnodes are restarted"),
    rt:wait_until(hd(Nodes), fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),
    lager:info("Nodes: ~p", [Nodes]),

    %% Read the value again
    {ok, {_, [ReadResult3], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
					   [[{read, {Key, Type}}]]),
    ?assertEqual(15, ReadResult3),
    lager:info("read_pncounter_log_recovery_test").

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_pncounter, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).

