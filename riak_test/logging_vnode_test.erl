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
    
    read_pncounter_initial_value_not_in_ets_test(Nodes),

    [Nodes1] = common:clean_and_rebuild_clusters([Nodes]),
    read_pncounter_old_value_not_in_ets_test(Nodes1),

    [Nodes2] = common:clean_and_rebuild_clusters([Nodes1]),
    read_orset_old_value_not_in_ets_test(Nodes2),
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

%% First increment a counter 5 times and remember the snapshot.
%% Then we increment the counter 15 times more so the old value is no
%% in the ets. Reading the old value returns 5, and the new value is
%% 20, as expected.
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

%% Add number 1 to 5 to an orset and remember the snapshot.
%% Then we add numbers 6 to 20 and read the old value not in the ets.
read_orset_old_value_not_in_ets_test(Nodes) ->
    lager:info("read_pncounter_old_value_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = crdt_orset,
    Key = read_orset_old_val,
    add_elements_starting_from(FirstNode, Key, 5, 0),
    {ok, {_, [ReadResult], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual([1, 2, 3, 4, 5], ReadResult),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    add_elements_starting_from(FirstNode, Key, 15, 5),
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    %% old read value contains elements 1 to 5
    ?assertEqual([1, 2, 3, 4, 5], ReadResult1),
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    %% most recent read vale contains elements 1 to 20
    ?assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 15, 16, 17, 18, 19, 20], ReadResult2),
    lager:info("read_pncounter_old_value_not_in_ets_test OK").

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_pncounter, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).

%% Auxiliary mehtod that adds numbers in the range
%% (Start, Start + N] to an orset.
add_elements_starting_from(_FirstNode, _key, 0, _start) ->
    ok;
add_elements_starting_from(FirstNode, Key, N, Start) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_orset, {{add, Start + N}, ucl}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    add_elements_starting_from(FirstNode, Key, N - 1, Start).
