%%%-------------------------------------------------------------------
%%% @author ayush
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Sep 2021 3:30 PM
%%%-------------------------------------------------------------------
-module(antidote).
-author("ayush").
-include("antidote.hrl").
%% API
-export([
    start_transaction/2,
    commit_transaction/1,
    abort_transaction/1,
    read_objects/2,
    read_objects/3,
    update_objects/2,
    update_objects/3,
    get_txn_property/2

]).



-spec read_objects(Objects::[bound_object()], TxId::txid())
      -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
  cure:read_objects(Objects, TxId).

-spec read_objects(snapshot_time() | ignore, txn_properties(), [bound_object()])
      -> {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, Properties, Objects) ->
  cure:read_objects(Clock, Properties, Objects).


-spec update_objects([{bound_object(), op_name(), op_param()}], txid())
      -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
  case type_check(Updates) of
    ok ->
      cure:update_objects(Updates, TxId);
    {error, Reason} ->
      {error, Reason}
  end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore , txn_properties(), [{bound_object(), op_name(), op_param()}])
      -> {ok, snapshot_time()} | {error, reason()}.
update_objects(Clock, Properties, Updates) ->
  case type_check(Updates) of
    ok ->
      cure:update_objects(Clock, Properties, Updates);
    {error, Reason} ->
      {error, Reason}
  end.

%% Transaction API %%
%% ==============  %%

-spec start_transaction(Clock::snapshot_time(), Properties::txn_properties())
        -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, Properties) ->
    clocksi_interactive_coord_api:start_transaction(Clock, Properties).

-spec abort_transaction(TxId::txid()) -> ok | {error, reason()}.
abort_transaction(TxId) ->
    clocksi_interactive_coord_api:abort_transaction(TxId).

-spec commit_transaction(TxId::txid()) ->
    {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    clocksi_interactive_coord_api:commit_transaction(TxId).
%% TODO: Execute post_commit hooks here?


-spec get_txn_property(atom(), txn_properties()) -> atom().
get_txn_property(update_clock, Properties) ->
  case lists:keyfind(update_clock, 1, Properties) of
    false ->
      update_clock;
    {update_clock, ShouldUpdate} ->
      case ShouldUpdate of
        true ->
          update_clock;
        false ->
          no_update_clock
      end
  end;
get_txn_property(certify, Properties) ->
  case lists:keyfind(certify, 1, Properties) of
    false ->
      application:get_env(antidote, txn_cert, true);
    {certify, Certify} ->
      case Certify of
        use_default ->
          application:get_env(antidote, txn_cert, true);
        certify ->
          %% Note that certify will only work correctly when
          %% application:get_env(antidote, txn_cert, true); returns true
          %% the reason is is that in clocksi_vnode:commit, the timestamps
          %% for committed transactions will only be saved if application:get_env(antidote, txn_cert, true)
          %% is true
          %% we might want to change this in the future
          true;
        dont_certify ->
          false
      end
  end.



%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec type_check_update({bound_object(), op_name(), op_param()}) -> boolean().
type_check_update({{_K, Type}, Op, Param}) ->
  antidote_crdt:is_type(Type)
    andalso antidote_crdt:is_operation(Type, {Op, Param}).

-spec type_check([{bound_object(), op_name(), op_param()}]) -> ok | {error, Reason :: any()}.
type_check([]) -> ok;
type_check([Upd|Rest]) ->
  case type_check_update(Upd) of
    true -> type_check(Rest);
    false -> {error, {badtype, Upd}}
  end.

