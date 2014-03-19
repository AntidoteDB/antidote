-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 create/2,
	 update/2,
	 get/1,
	 types/0
        ]).

-ignore_xref([
              ping/0
             ]).

-define(BUCKET, <<"floppy">>).

%% Public API

types()->
   io:fwrite("riak_dt_disable_flag~n",[]),
   io:format("riak_dt_enable_flag~n"),
   io:format("riak_dt_gcounter~n"),
   io:format("riak_dt_gset~n"),
   io:format("riak_dt_lwwreg~n"),
   io:format("riak_dt_map~n"),
   io:format("riak_dt_od_flag~n"),
   io:format("riak_dt_oe_flag~n"),
   io:format("riak_dt_orset~n"),
   io:format("riak_dt_orswot~n"),
   io:format("riak_dt_pncounter~n").

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, floppy),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, floppy_vnode_master).

create(Key, Type) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
    [{IndexNode, _Type}] = PrefList,
    logging_vnode:create(IndexNode, Key, Type).

update(Key, Op) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
    [{IndexNode, _Type}] = PrefList,
    logging_vnode:update(IndexNode, Key, Op).

get(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
    [{IndexNode, _Type}] = PrefList,
    logging_vnode:get(IndexNode, Key).


