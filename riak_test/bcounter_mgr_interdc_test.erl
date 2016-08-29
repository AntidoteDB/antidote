-module(bcounter_mgr_interdc_test).

-export([confirm/0,
         execute_op/5,
         read_empty/2,
         test_dec_success/4,
         test_dec_fail/4,
         test_dec_multi_success/4
        ]).


-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).
-define(TYPE, crdt_bcounter).

confirm() ->

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),

    _Clean = rt_config:get(clean_cluster, true),

    [[NodeDC1]=Cluster1, [NodeDC2]=Cluster2, Cluster3] = rt:build_clusters([1,1,1]),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),


    {ok, Prot} = rpc:call(hd(Cluster1), application, get_env, [antidote, txn_prot]),
    ?assertMatch(clocksi, Prot),

    ok = common:setup_dc_manager([Cluster1, Cluster2], first_run),

    read_empty(NodeDC1, empty_key),
    test_dec_success(NodeDC1, NodeDC2, key1, actor1),
    test_dec_fail(NodeDC1, NodeDC2, key2, actor1),
    test_dec_multi_success(NodeDC1, NodeDC2, key3, actor1),
    pass.

execute_op(Node, Op, Key, Amount, Actor) ->
    %Result = rpc:call(Node, antidote, clocksi_bulk_update,
    %                       [[{update, {Key, ?TYPE, {{Op, Amount}, Actor}}}]]),
    Result = rpc:call(Node, antidote, append,
                      [Key, ?TYPE, {{Op, Amount}, Actor}]),
    case Result of
        {ok, {_,_,CommitTime}} -> {ok, CommitTime};
        Error -> Error
    end.

read(Node, Key) ->
    rpc:call(Node, antidote, clocksi_read, [Key, ?TYPE]).

read(Node, Key, CommitTime) ->
    rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, ?TYPE]).

%check_read(Node, Key, Expected) ->
%    {ok, {_, [Obj], _}} = read(Node, Key),
%    ?assertEqual(Expected, crdt_bcounter:permissions(Obj)).

check_read(Node, Key, Expected, CommitTime) ->
    {ok, {_, [Obj], _}} = read(Node, Key, CommitTime),
    ?assertEqual(Expected, crdt_bcounter:permissions(Obj)).

read_empty(Node, Key) ->
    Result = read(Node, Key),
    {ok, {_, [Obj], _CommitTime}} = Result,
    ?assertEqual(0, crdt_bcounter:permissions(Obj)).

test_dec_success(DC1, DC2, Key, Actor) ->
    {ok, _} = execute_op(DC1, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(DC1, decrement, Key, 4, Actor),
    check_read(DC2, Key, 6, CommitTime).

test_dec_fail(DC1, DC2, Key, Actor) ->
    {ok, _} = execute_op(DC1, increment, Key, 10, Actor),
    Result = execute_op(DC2, decrement, Key, 5, Actor),
    ?assertEqual({error, no_permissions}, Result).

%Assume that requests timeout is less than 1 second.
test_dec_multi_success(DC1, DC2, Key, Actor) ->
    {ok, _} = execute_op(DC1, increment, Key, 10, Actor),
    timer:sleep(1000),
    _Result = execute_op(DC2, decrement, Key, 5, Actor),
    timer:sleep(1000),
    {ok, CommitTime}  = execute_op(DC2, decrement, Key, 5, Actor),
    check_read(DC2, Key, 5, CommitTime).







