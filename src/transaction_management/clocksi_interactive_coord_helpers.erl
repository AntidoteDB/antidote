%%%-------------------------------------------------------------------
%%% @author ayush
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2021 4:11 PM
%%%-------------------------------------------------------------------
-module(clocksi_interactive_coord_helpers).
-author("ayush").
-include("antidote.hrl").
%% API
-export([create_transaction_record/3]).



%% @doc Create a map containing the entries of a new transaction.
-spec create_transaction_record(snapshot_time() | ignore, boolean(), txn_properties()) -> tx().
%%noinspection ErlangUnresolvedFunction
create_transaction_record(ClientClock, _IsStatic, Properties) ->
  %% Seed the random because you pick a random read server, this is stored in the process state
  _Res = rand:seed(exsplus, {erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()}),
  {ok, SnapshotTime} = case ClientClock of
                         ignore ->
                           get_snapshot_time();
                         _ ->
                           case antidote:get_txn_property(update_clock, Properties) of
                             update_clock ->
                               get_snapshot_time(ClientClock);
                             no_update_clock ->
                               {ok, ClientClock}
                           end
                       end,
  DcId = dc_utilities:get_my_dc_id(),
  LocalClock = vectorclock:get(DcId, SnapshotTime),
  TransactionId = #tx_id{local_start_time = LocalClock, server_pid = self()},
  #transaction{snapshot_time_local = LocalClock,
    vec_snapshot_time = SnapshotTime,
    txn_id = TransactionId,
    properties = Properties}.



%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time()) -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
  wait_for_clock(ClientClock).


-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
  Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
  {ok, VecSnapshotTime} = dc_utilities:get_stable_snapshot(),
  DcId = dc_utilities:get_my_dc_id(),
  SnapshotTime = vectorclock:set(DcId, Now, VecSnapshotTime),
  {ok, SnapshotTime}.



-spec wait_for_clock(snapshot_time()) -> {ok, snapshot_time()}.
wait_for_clock(Clock) ->
  {ok, VecSnapshotTime} = get_snapshot_time(),
  case vectorclock:ge(VecSnapshotTime, Clock) of
    true ->
      %% No need to wait
      {ok, VecSnapshotTime};
    false ->
      %% wait for snapshot time to catch up with Client Clock
      timer:sleep(?SPIN_WAIT),
      wait_for_clock(Clock)
  end.