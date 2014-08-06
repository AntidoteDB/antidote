-module(pb_client).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

-define(ADDRESS, "localhost").

-define(PORT, 10017).


confirm() ->
    start_stop_test(),
    get_empty_crdt_test(<<"key0">>),
    update_counter_crdt_test(<<"key1">>,10),
    update_counter_crdt_and_read_test(<<"key2">>,15).

start_stop_test() ->
    [_Nodes] = rt:build_clusters([1]),
    {ok,Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(ok,Disconnected),
    ok.

get_empty_crdt_test(Key) ->
    {ok,Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok,Obj} = floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid),
    _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(true,floppyc_counter:is_type(Obj)),
    ok.

update_counter_crdt_test(Key,Amount) ->
    {ok,Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok,Obj} = floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid),
    Obj2 = floppyc_counter:increment(Amount,Obj),
    Result = floppyc_pb_socket:store_crdt(Obj2,Pid),
     _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(ok,Result).

update_counter_crdt_and_read_test(Key,Amount) ->
    update_counter_crdt_test(Key,Amount),
    get_crdt_check_value(Key,riak_dt_pncounter,Amount).

get_crdt_check_value(Key,Type,Expected) ->
    {ok,Pid} = floppyc_pb_socket:start(?ADDRESS, ?PORT),
    {ok,Obj} = floppyc_pb_socket:get_crdt(Key,Type,Pid),
    Mod = floppyc_datatype:module_for_type(Type),
    _Disconnected = floppyc_pb_socket:stop(Pid),
    ?assertMatch(Expected, Mod:value(Obj)).


%%TODO: Add incorrect type cast test

