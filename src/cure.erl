%%%-------------------------------------------------------------------
%%% @author ayush
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2021 3:45 PM
%%%-------------------------------------------------------------------
-module(cure).
-author("ayush").
-include("antidote.hrl").
%% API
-export([
  read_objects/2,
  read_objects/3,
  update_objects/2,
  update_objects/3
]).



-spec read_objects([bound_object()], txid()) -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
  obtain_objects(Objects, TxId, object_value).

-spec read_objects(snapshot_time() | ignore, txn_properties(), [bound_object()]) ->
  {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, Properties, Objects) ->
  obtain_objects(Clock, Properties, Objects, object_value).


-spec update_objects([{bound_object(), op_name(), op_param()}], txid())
      -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
  %Call the tranasction coordinator to update the objects.
  case gen_statem:call(TxId#tx_id.server_pid, {update_objects, Updates}, ?OP_TIMEOUT) of
    ok ->
      ok;
    {aborted, TxId} ->
      {error, aborted};
    {error, Reason} ->
      {error, Reason}
  end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore , list(), [{bound_object(), op_name(), op_param()}]) ->
  {ok, snapshot_time()} | {error, reason()}.
update_objects(_Clock, _Properties, []) ->
  {ok, vectorclock:new()};
update_objects(ClientCausalVC, Properties, Updates) ->
  {ok, TxId} = clocksi_interactive_coord_api:start_transaction(ClientCausalVC, Properties),
  case update_objects(Updates, TxId) of
    ok -> Result = clocksi_interactive_coord_api:commit_transaction(TxId),
      Result;
    {error, Reason} -> {error, Reason}
  end.



-spec obtain_objects([bound_object()], txid(), object_value|object_state) -> {ok, [term()]} | {error, reason()}.
obtain_objects(Objects, TxId, StateOrValue) ->
  case gen_statem:call(TxId#tx_id.server_pid, {read_objects, Objects}, ?OP_TIMEOUT) of
    {ok, Res} ->
      {ok, transform_reads(Res, StateOrValue, Objects)};
    {error, Reason} -> {error, Reason}
  end.

-spec obtain_objects(snapshot_time() | ignore, txn_properties(), [bound_object()], object_value|object_state) ->
  {ok, list(), vectorclock()} | {error, reason()}.
obtain_objects(Clock, Properties, Objects, StateOrValue) ->
  SingleKey = case Objects of
                [_O] -> %% Single key update
                  case Clock of
                    ignore -> true;
                    _ -> false
                  end;
                [_H|_T] -> false
              end,
  case SingleKey of
    true ->
      [{Key, Type}] = Objects,
      {ok, {Key, Type, Val}, SnapshotTimestamp} = clocksi_interactive_coord:perform_static_operation(Clock, Key, Type, Properties),
      {ok, transform_reads([Val], StateOrValue, Objects), SnapshotTimestamp};
    false ->
      case application:get_env(antidote, txn_prot) of
        {ok, clocksi} ->

          {ok, TxId} = clocksi_interactive_coord_api:start_transaction(Clock, Properties),
          case obtain_objects(Objects, TxId, StateOrValue) of
            {ok, Res} ->
              {ok, CommitTime} = clocksi_interactive_coord_api:commit_transaction(TxId),
              {ok, Res, CommitTime};
            {error, Reason} -> {error, Reason}
          end
      end
  end.

transform_reads(Snapshot, StateOrValue, Objects) ->
  case StateOrValue of
    object_state -> Snapshot;
    object_value -> lists:map(fun({State, {_Key, Type}}) ->
      antidote_crdt:value(Type, State) end,
      lists:zip(Snapshot, Objects))
  end.
