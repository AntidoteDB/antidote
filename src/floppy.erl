-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         append/2,
         read/2,
         clocksi_execute_tx/2,
         clocksi_read/3,
         clocksi_bulk_update/2,
         clocksi_istart_tx/1,
         clocksi_iread/3,
         clocksi_iupdate/3,
         clocksi_iprepare/1,
         clocksi_icommit/1]).

-type key() :: term().
-type op()  :: term().
-type crdt() :: term().
-type val() :: term().
-type reason() :: term().

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT object
%% stored at some key.
%% TODO What is returned in case of success?!
-spec append(key(), op()) -> {ok, term()} | {error, timeout}.
append(Key, Op) ->
    lager:info("Append called!"),
    case clocksi_bulk_update(now(), [{update, Key, Op}]) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc The read/2 function returns the current value for the CRDT object stored
%% at some key.
%% TODO Which state is exactly returned? Related to some snapshot? What is current?
-spec read(key(), crdt()) -> val() | {error,reason()}.
read(Key, Type) ->
    case clocksi_read(now(), Key, Type) of
        {ok, {_, [Val], _}} ->
            Val;
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
clocksi_execute_tx(ClientClock, Operations) ->
    lager:info(
      "FLOPPY: Received transaction with clock: ~w for operations ~w ~n",
      [ClientClock, Operations]),
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


clocksi_bulk_update(ClientClock, Operations) ->
    clocksi_execute_tx(ClientClock, Operations).

clocksi_read(ClientClock, Key, Type) ->
    Operation={read, Key, Type},
    clocksi_execute_tx(ClientClock, [Operation]).

clocksi_iread(TxId=#tx_id{}, Key, Type) ->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {read, {Key, Type}}).

clocksi_iupdate(TxId=#tx_id{}, Key, OpParams) ->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {update, {Key, OpParams}}).

clocksi_iprepare(TxId=#tx_id{})->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, {prepare, empty}).

clocksi_icommit(TxId=#tx_id{})->
    {_, _, CoordFsmPid}=TxId,
    gen_fsm:sync_send_event(CoordFsmPid, commit).
