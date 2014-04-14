-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 %create/2,
	 update/2,
	 read/2,
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

update(Key, Op) ->
    io:format("Update ~w ~w ~n", [Key, Op]),
    floppy_coord_sup:start_fsm([self(), update, Key, Op]),    
    receive 
	{_, Result} ->
	io:format("Update completed!~w~n",[Result]),
        inter_dc_repl_vnode:propogate({Key,Op})
	after 5000 ->
	io:format("Update failed!~n")
    end.
   %{ok, {_ObjKey, _ObjVal}} = floppy_coord_sup:start_fsm([self(), update, li, {increment, thin}]),
   %io:format("Floppy: finished~n"),
   %{ok}.

read(Key, Type) ->
    io:format("Read ~w ~w ~n", [Key, Type]),
    floppy_coord_sup:start_fsm([self(), read, Key, noop]),
    receive 
	{_, Ops} ->
	io:format("Read completed!~n"),
    	Init=materializer:create_snapshot(Type),
    	Snapshot=materializer:update_snapshot(Type, Init, Ops),
    	Value=Type:value(Snapshot),
    	{ok, Value}
	after 5000 ->
	io:format("Read failed!~n"),
	{error, nothing}
    end.
   %floppy_coord_sup:start_fsm([self(), read, Key, something]).


