-module(floppy).

-export([append/3, read/2,
         clocksi_execute_tx/2,
         clocksi_execute_tx/1,
         clocksi_read/3,
         clocksi_bulk_update/2,
         clocksi_bulk_update/1,
         clocksi_istart_tx/1,
         clocksi_istart_tx/0,
         clocksi_iread/3,
         clocksi_iupdate/4,
         clocksi_iprepare/1,
         clocksi_icommit/1]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
append(Key, Type, {OpParam, Actor}) ->
    Operations = [update, Key, Type, {OpParam, Actor}],
    case clocksi_execute_tx(Operations) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
read(Key, Type) ->
    case clocksi_read(now(), Key, Type) of
        {ok,{_,[Val],_ }} ->
            {ok, Val};
        {error, Reason} ->
            {error, Reason}
    end.

%% Clock SI API

%% @doc Starts a new ClockSI transaction.
%% Input:
%% ClientClock: last clock the client has seen from a successful transaction.
%% Operations: the list of the operations the transaction involves.
%% Returns:
%% an ok message along with the result of the read operations involved in the
%% the transaction, in case the tx ends successfully.
%% error message in case of a failure.
clocksi_execute_tx(Clock, Operations) ->
    lager:info(
      "FLOPPY: Received transaction with clock: ~w for operations ~w ~n",
      [Clock, Operations]),
    case Clock of
        {Mega,Sec,Micro} ->
            ClientClock= clocksi_vnode:now_milisec({Mega,Sec,Micro});
        _ ->
            ClientClock = Clock
    end,
    {ok, _PID} = clocksi_tx_coord_sup:start_fsm(
                   [self(), ClientClock, Operations]),
    receive
        EndOfTx ->
            lager:info("FLOPPY: TX completed!~n"),
            EndOfTx
    after 10000 ->
            lager:info("FLOPPY: Tx failed!~n"),
            {error}
    end.

clocksi_execute_tx(Operations) ->
    lager:info(
      "FLOPPY: Received transaction for operations ~w ~n",
      [Operations]),
    {ok, _PID} = clocksi_tx_coord_sup:start_fsm(
                   [self(), Operations]),
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
%% ClientClock: last clock the client has seen from a successful transaction.
%% Returns:
%%	an ok message along with the new TxId.
clocksi_istart_tx(Clock) ->
    lager:info("FLOPPY: Starting FSM for interactive transaction.~n"),
    case Clock of
        {Mega,Sec,Micro} ->
            ClientClock= clocksi_vnode:now_milisec({Mega,Sec,Micro});
        _ ->
            ClientClock = Clock
    end,
    {ok, _PID} = clocksi_interactive_tx_coord_sup:start_fsm(
                   [self(), ClientClock]),
    receive
        TxId ->
            lager:info("FLOPPY: TX started with TxId= ~w~n", [TxId]),
            TxId
    after 10000 ->
            lager:info("FLOPPY: Tx was not started!~n"),
            {error, timeout}
    end.

clocksi_istart_tx() ->
    lager:info("FLOPPY: Starting FSM for interactive transaction.~n"),
    {ok, _PID} = clocksi_interactive_tx_coord_sup:start_fsm(
                   [self()]),
    receive
        TxId ->
            lager:info("FLOPPY: TX started with TxId= ~w~n", [TxId]),
            TxId
    after 10000 ->
            lager:info("FLOPPY: Tx was not started!~n"),
            {error, timeout}
    end.

clocksi_bulk_update(ClientClock, Operations) ->
    clocksi_execute_tx(ClientClock, Operations).

clocksi_bulk_update(Operations) ->
    clocksi_execute_tx(Operations).

clocksi_read(ClientClock, Key, Type) ->
    Operation={read, Key, Type},
    clocksi_execute_tx(ClientClock, [Operation]).

clocksi_iread(TxId, Key, Type) ->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

clocksi_iupdate(TxId, Key, Type, OpParams) ->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, Type, OpParams}}).

clocksi_iprepare(TxId)->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).

clocksi_icommit(TxId)->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, commit).
