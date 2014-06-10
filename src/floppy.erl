-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
	 append/2,
	 read/2,
	 clockSI_execute_TX/2,
	 clockSI_read/3,
	 clockSI_bulk_update/2,
	 clockSI_istart_tx/1,
	 clockSI_iread/3,
	 clockSI_iupdate/3,	 
	 clockSI_iprepare/1,	 
	 clockSI_icommit/1]).

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
    lager:info("FLOPPY: Received order to execute transaction with clock: ~w for the list of operations ~w ~n",
    [ClientClock, Operations]),
    clockSI_tx_coord_sup:start_fsm([self(), ClientClock, Operations]),
    receive
        EndOfTx ->
	    lager:info("FLOPPY: TX completed!~n"),
	    EndOfTx
    after 10000 ->
	    lager:info("FLOPPY: Tx failed!~n"),
	    {error}
    end.
    

%% @doc Starts a new ClockSI interactive transaction.
%% Input:
%%	ClientClock: the last clock the client has seen from a successful transaction.
%% Returns:
%%	an ok message along with the new TxId. 
clockSI_istart_tx(Clock) ->
	lager:info("FLOPPY: Starting FSM for interactive transaction.~n"),
	case Clock of
		{Mega,Sec,Micro} ->
			ClientClock= clockSI_vnode:now_milisec({Mega,Sec,Micro})
		end,
	clockSI_interactive_tx_coord_sup:start_fsm([self(), ClientClock]),
    %lager:info("FLOPPY: Worker started: ~w!~n", [Worker]),	
    receive
        TxId ->
	        lager:info("FLOPPY: TX started with TxId= ~w~n", [TxId]),
	        TxId
    after 10000 ->
	    lager:info("FLOPPY: Tx was not started!~n"),
	    {error, timeout}
    end.
    
    
clockSI_bulk_update(ClientClock, Operations) ->
    clockSI_execute_TX(ClientClock, Operations).

clockSI_read(ClientClock, Key, Type) ->
	Operation={read, Key, Type},
    clockSI_execute_TX(ClientClock, [Operation]).
    
clockSI_iread(TxId=#tx_id{}, Key, Type) ->
	{_, _, CoordFsmPid}=TxId,
	gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).
    
clockSI_iupdate(TxId=#tx_id{}, Key, OpParams) ->
	{_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, OpParams}}).

clockSI_iprepare(TxId=#tx_id{})->
	{_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).

clockSI_icommit(TxId=#tx_id{})->
	{_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, commit).
        
    
    
    
