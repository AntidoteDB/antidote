-module(vectorclock).

-export([get_clock/1, update_clock/3, get_clock_by_key/1]).

get_clock_by_key(Key) ->
    DocIdx = riak_core_util:chash_key({<<"floppy">>, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, vectorclock),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_command(IndexNode, {get_clock}, vectorclock_vnode_master).

get_clock(Node) ->
    riak_core_vnode_master:sync_command(Node, {get_clock}, vectorclock_vnode_master).
    
%% update_clock_by_key(Key, Dc_id, Timestamp) ->
%%     DocIdx = riak_core_util:chash_key({<<"floppy">>, term_to_binary(Key)}),
%%     PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, vectorclock),
%%     [{IndexNode, _Type}] = PrefList,
%%     riak_core_vnode_master:sync_command(IndexNode, {update_clock, Dc_id, Timestamp}, vectorclock_vnode_master).

update_clock(Node, Dc_id, Timestamp) -> 
    riak_core_vnode_master:sync_command(Node, {update_clock, Dc_id, Timestamp}, vectorclock_vnode_master).
