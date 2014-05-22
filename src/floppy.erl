-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 %create/2,
	 append/2,
	 read/2,
	 clockSI_execute_TX/2,
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

%% @doc Starts a new ClockSI transaction.
%% Input:
%%	ClientClock: the last clock the client has seen from a successful transaction.
%%	Operations: the list of the operations the transaction involves.
%% Returns:
%%	an ok message along with the result of the read operations involved in the transaction, 
%%	in case the tx ends successfully. 
%%	error message in case of a failure.
clockSI_execute_TX(ClientClock, Operations) ->
    io:format("Received order to execute transaction with clock: ~w for
    			the list of operations ~w ~n", [ClientClock, Operations]),
    clockSI_tx_coord_sup:start_fsm([self(), ClientClock, Operations]),	
    receive
        EndOfTx ->
	    io:format("TX completed!~n"),
	    EndOfTx
    after 10000 ->
	    io:format("Tx failed!~n"),
	    {error}
    end.
