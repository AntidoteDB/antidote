-module(dc_utilities).

-export([get_my_dc_id/0]).

get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).
