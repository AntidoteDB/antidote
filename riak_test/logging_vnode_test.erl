-module(logging_vnode_test).

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
    read_initial_value_not_in_ets_test(Nodes),
    read_old_value_not_in_ets_test(Nodes),
    pass.

%% First we remember the initial time of the counter (with value 0).
%% After 15 updates, the smallest snapshot in ets table is 10,
%% but if we read the initial value of the counter,
%% we get 0, because no operations have a smaller commit time in the
%% reconstruction of time 0 in the log. (this is a corner case)
read_pncounter_initial_value_not_in_ets_test(Nodes) ->
    lager:info("read_snapshot_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = crdt_pncounter,
    Key = initial_value_test,
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
    lager:info("read_pncounter_initial_value_not_in_ets_test OK").

read_pncounter_old_value_not_in_ets_test(Nodes) ->
    lager:info("read_old_value_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = crdt_pncounter,
    Key = read_old_val,
    increment_counter(FirstNode, Key, 5),
    {ok, {_, [ReadResult], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual(5, ReadResult),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, Key, 15),
    %% old read value is 5
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual(5, ReadResult1),
    %% most recent read value is 20
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual(20, ReadResult2),
    lager:info("read_pncounter_old_value_not_in_ets_test OK").

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_pncounter, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).