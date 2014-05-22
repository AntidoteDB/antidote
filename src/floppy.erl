-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 %create/2,
	 append/2,
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

%dcreate(Key, Type) ->
%   floppy_coord_sup:start_fsm([100, self(), create, Key, Type]).

append(Key, Op) ->
    case floppy_rep_vnode:append(Key, Op) of
	{ok, Result} ->
	    %inter_dc_repl_vnode:propogate(Key, Op),
	    {ok, Result};
	{error, Reason} ->
	    {error, Reason}
    end.
	

read(Key, Type) ->
    case floppy_rep_vnode:read(Key, Type) of
	{ok, Ops} ->
    	    Init=materializer:create_snapshot(Type),
	    Snapshot=materializer:update_snapshot(Type, Init, Ops),
	    Value=Type:value(Snapshot),
	    Value;
        {error, _} ->
	    io:format("Read failed!~n"),
	    error
    end.

%% Clock SI API
%startTX(ClientClock, Operations) ->
%		clockSI_tx_coord_sup:start_fsm([self(), ClientClock, Operations]).	
