-module(logging_vnode_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
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
    pass.

%% First we remember the initial time of the counter (value 0).
%% After 10 updates, the smallest snapshot in ets table is 5,
%% but a read of the previous snapshot returns 0, fetching the value from the log.
read_initial_value_not_in_ets_test(Nodes) ->
    lager:info("read_snapshot_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = crdt_pncounter,
    Key = empty_test,
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    ReadResult = rpc:call(FirstNode, antidote, clocksi_iread,
        [TxId, Key, Type]),
    ?assertEqual({ok, 0}, ReadResult),
    increment_counter(FirstNode, Key, 11),
    ReadResult1 = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual({ok, 0}, ReadResult1),
    lager:info("read_snapshot_not_in_ets_test OK").

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, crdt_pncounter, {increment, a}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).