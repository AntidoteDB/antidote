-module(bcounter_mgr_test).

-export([confirm/0,
         execute_op/5,
         read_empty/2,
         test_dec_success/3,
         test_dec_fail/3
        ]).


-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).
-define(TYPE, crdt_bcounter).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),
    [Nodes] = rt:build_clusters([3]),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Nodes: ~p", [Nodes]),

    [Node1,_Node2,_Node3] = Nodes,
    read_empty(Node1, empty_key),
    test_dec_success(Node1, key1, actor1),
    test_dec_fail(Node1, key2, actor1),
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

test_dec_success(Node, Key, Actor) ->
    %{ok, _} = read(Node, Key),
    {ok, _} = execute_op(Node, increment, Key, 10, Actor),
    {ok, CommitTime} = execute_op(Node, decrement, Key, 4, Actor),
    check_read(Node, Key, 6, CommitTime).

test_dec_fail(Node, Key, Actor) ->
    %{ok, _} = read(Node, Key),
    {ok, _} = execute_op(Node, increment, Key, 10, Actor),
    Result = execute_op(Node, decrement, Key, 12, Actor),
    ?assertEqual({error, no_permissions}, Result).

