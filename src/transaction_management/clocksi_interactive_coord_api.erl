%%%-------------------------------------------------------------------
%%% @author ayush
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2021 3:55 PM
%%%-------------------------------------------------------------------
-module(clocksi_interactive_coord_api).
-author("ayush").
-include("antidote.hrl").
%% API
-export([
  start_transaction/2,
  commit_transaction/1,
    abort_transaction/1
]).


%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec start_transaction(snapshot_time() | ignore, txn_properties()) ->
  {ok, txid()} | {error, reason()}.
start_transaction(Clock, Properties) ->
  {ok, Pid} = clocksi_interactive_coord_sup:start_fsm(),
  gen_statem:call(Pid, {start_tx, Clock, Properties}).


-spec commit_transaction(txid()) ->
  {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
  case clocksi_full_icommit(TxId) of
    {ok, {_TxId, CommitTime}} ->
      {ok, CommitTime};
    {error, Reason} ->
      {error, Reason};
    Other ->
      {error, Other}
  end.


-spec clocksi_full_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}} | {error, reason()}.
clocksi_full_icommit(TxId)->
  case gen_statem:call(TxId#tx_id.server_pid, {prepare, two_phase}, ?OP_TIMEOUT) of
    {ok, PrepareTime} ->
      gen_statem:call(TxId#tx_id.server_pid, {commit, PrepareTime}, ?OP_TIMEOUT);
    Msg ->
      Msg
  end.


-spec abort_transaction(txid()) -> ok | {error, reason()}.
abort_transaction(TxId) ->
    case gen_statem:call(TxId#tx_id.server_pid, {abort, []}) of
        {error, aborted} -> ok;
        {error, Reason} -> {error, Reason}
    end.
