-module(pb_client_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ADDRESS, "localhost").

-define(PORT, 10017).

confirm() ->
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    pass = start_stop_test(),
    pass = get_empty_crdt_test(<<"key0">>),
    pass = update_counter_crdt_test(<<"key1">>, 10),
    pass = update_counter_crdt_and_read_test(<<"key2">>, 15),

    pass.

start_stop_test() ->
    lager:info("Verifying pb connection..."),
    {ok, Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(ok, Disconnected),
    pass.

get_empty_crdt_test(Key) ->
    {ok, Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid),
    _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(true, floppyc_counter:is_type(Obj)),
    pass.

update_counter_crdt_test(Key, Amount) ->
    {ok, Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = floppyc_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid),
    Obj2 = floppyc_counter:increment(Amount, Obj),
    Result = floppyc_pb_socket:store_crdt(Obj2, Pid),
     _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(ok, Result),
    pass.

update_counter_crdt_and_read_test(Key, Amount) ->
    pass = update_counter_crdt_test(Key, Amount),
    pass = get_crdt_check_value(Key, riak_dt_pncounter, Amount).

get_crdt_check_value(Key, Type, Expected) ->
    {ok, Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok, Obj} = floppyc_pb_socket:get_crdt(Key, Type, Pid),
    Mod = floppyc_datatype:module_for_type(Type),
    _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(Expected, Mod:value(Obj)),
    pass.
