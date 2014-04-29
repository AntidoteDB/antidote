-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 %create/2,
	 update/2,
	 read/2,
	 %startTX/2,
	 types/0
        ]).

-ignore_xref([
              ping/0
             ]).

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
    case floppy_rep_vnode:append(Key, Op) of
	{ok, Result} ->
	    %inter_dc_repl_vnode:propogate(Key, Op),
	    {ok, Result};
	{error, Reason} ->
	    {error, Reason}
    end.
	

read(Key, Type) ->
    floppy_rep_vnode:read(Key, Type).

%% Clock SI API
startTX(ClientClock, Operations) ->
		clockSI_tx_coord_sup:start_fsm([self(), ClientClock, Operations]).	
