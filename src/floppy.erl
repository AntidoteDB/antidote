-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 append/2,
	 read/2,
	 clockSI_execute_TX/2,
        ]).

%% Public API

append(Key, Op) ->
    lager:info("Append called!"),
    case floppy_rep_vnode:append(Key, Op) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

read(Key, Type) ->
    case floppy_rep_vnode:read(Key, Type) of
        {ok, Ops} ->
            Init=materializer:create_snapshot(Type),
            Snapshot=materializer:update_snapshot(Type, Init, Ops),
            Type:value(Snapshot);
        {error, _} ->
            lager:info("Read failed!~n"),
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
