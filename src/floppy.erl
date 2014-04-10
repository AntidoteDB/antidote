-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 %create/2,
	 append/2,
	 read/1,
	 get/2,
	 update/2,
	 dupdate/2,
	 dread/2,
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

%dcreate(Key, Type) ->
%   floppy_coord_sup:start_fsm([100, self(), create, Key, Type]).

dupdate(Key, Op) ->
    proxy:update(Key, Op,self()),
    inter_dc_repl_vnode:propogate({Key,Op}).
   %{ok, {_ObjKey, _ObjVal}} = floppy_coord_sup:start_fsm([self(), update, li, {increment, thin}]),
   %io:format("Floppy: finished~n"),
   %{ok}.

dread(Key, Type) ->
    {_, Ops} = proxy:read(Key, self()),
    Init=materializer:create_snapshot(Type),
    Snapshot=materializer:update_snapshot(Type, Init, Ops),
    Value=Type:value(Snapshot),
    {ok, Value}.
   %floppy_coord_sup:start_fsm([self(), read, Key, something]).

%create(Key, Type) ->
%    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
%    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
%    [{IndexNode, _Type}] = PrefList,
%    logging_vnode:create(IndexNode, Key, Type).

append(Key, Op) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
    [{IndexNode, _Type}] = PrefList,
    logging_vnode:append(IndexNode, Key, Op).

read(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, logging),
    [{IndexNode, _Type}] = PrefList,
    logging_vnode:read(IndexNode, Key).

get(Key, Type) ->
    {ok, Ops} = read(Key),
    Init=materializer:create_snapshot(Type),
    Snapshot=materializer:update_snapshot(Type, Init, Ops),
    Value=Type:value(Snapshot),
    {ok, Value}.

update(Key, Op)->
    append(Key, Op).
