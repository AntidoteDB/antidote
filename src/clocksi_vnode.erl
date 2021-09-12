%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("kernel/include/logger.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-ignore_xref([start_vnode/1]).

-export([prepare/2]).

-export([start_vnode/1,
  init/1,
  terminate/2,
  handle_command/3,
  is_empty/1,
  delete/1,
  check_tables_ready/0,
  handle_handoff_command/3,
  handoff_starting/2,
  handoff_cancelled/1,
  handoff_finished/2,
  handle_handoff_data/2,
  encode_handoff_item/2,
  handle_coverage/4,
  handle_exit/3,
  handle_overload_command/3,
  handle_overload_info/2]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_tx: the prepared txn for each key. Note that for
%%              each key, there can be at most one prepared txn in any
%%              time.
%%          committed_tx: the transaction id of the last committed
%%              transaction for each key.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: partition_id(),
  prepared_tx :: cache_id(),
  committed_tx :: cache_id(),
  read_servers :: non_neg_integer(),
  prepared_dict :: list()}).


%%%===================================================================
%%% External API
%%%===================================================================
%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
  lists:foreach(fun({Node, WriteSet}) ->
    riak_core_vnode_master:command(Node,
      {prepare, TxId, WriteSet},
      {fsm, undefined, self()},
      ?CLOCKSI_MASTER)
                end, ListofNodes).




%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
  riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
  PreparedTx = antidote_ets_txn_caches:create_prepared_txns_cache(Partition),
  CommittedTx = create_committed_txns_cache(),
  {ok, #state{partition = Partition,
    prepared_tx = PreparedTx,
    committed_tx = CommittedTx,
    read_servers = ?READ_CONCURRENCY,
    prepared_dict = orddict:new()}}.

%% @doc The table holding the prepared transactions is shared with concurrent
%%      readers, so they can safely check if a key they are reading is being updated.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
  PartitionList = dc_utilities:get_all_partitions_nodes(),
  check_table_ready(PartitionList).

check_table_ready([]) ->
  true;
check_table_ready([{Partition, Node} | Rest]) ->
  Result =
    try
      riak_core_vnode_master:sync_command({Partition, Node},
        {check_tables_ready},
        ?CLOCKSI_MASTER,
        infinity)
    catch
      _:_Reason ->
        false
    end,
  case Result of
    true ->
      check_table_ready(Rest);
    false ->
      false
  end.


handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({prepare, Transaction, WriteSet}, _Sender,
    State = #state{partition = _Partition,
      committed_tx = CommittedTx,
      prepared_tx = PreparedTx,
      prepared_dict = PreparedDict
    }) ->
  PrepareTime = dc_utilities:now_microsec(),
  {Result, NewPrepare, NewPreparedDict} = prepare(Transaction, WriteSet, CommittedTx, PreparedTx, PrepareTime, PreparedDict),
  case Result of
    ok ->
      {reply, {prepared, NewPrepare}, State#state{prepared_dict = NewPreparedDict}};
    {error, timeout} ->
      {reply, {error, timeout}, State#state{prepared_dict = NewPreparedDict}};
    {error, no_updates} ->
      {reply, {error, no_tx_record}, State#state{prepared_dict = NewPreparedDict}};
    {error, write_conflict} ->
      {reply, abort, State#state{prepared_dict = NewPreparedDict}}
  end;

handle_command(_Message, _Sender, State) ->
  {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
  {noreply, State}.

handoff_starting(_TargetNode, State) ->
  {true, State}.

handoff_cancelled(State) ->
  {ok, State}.

handoff_finished(_TargetNode, State) ->
  {ok, State}.

handle_handoff_data(_Data, State) ->
  {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
  term_to_binary({StatName, Val}).

is_empty(State) ->
  {true, State}.

delete(State) ->
  {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
  {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
  {noreply, State}.

terminate(_Reason, #state{partition = Partition} = _State) ->
  antidote_ets_txn_caches:delete_prepared_txns_cache(Partition),
  ok.

handle_overload_command(_, _, _) ->
  ok.
handle_overload_info(_, _) ->
  ok.



%%%===================================================================
%%% Internal Functions
%%%===================================================================

prepare(Transaction, TxWriteSet, CommittedTx, PreparedTx, PrepareTime, PreparedDict) ->
  case certification_check(Transaction, TxWriteSet, CommittedTx, PreparedTx) of
    true ->
      case TxWriteSet of
        [{Key, _Type, _Update} | _] ->
          TxId = Transaction#transaction.txn_id,
          Dict = set_prepared(PreparedTx, TxWriteSet, TxId, PrepareTime, dict:new()),
          NewPrepareTimestamp = dc_utilities:now_microsec(),
          ok = reset_prepared(PreparedTx, TxWriteSet, TxId, NewPrepareTimestamp, Dict),
          NewPreparedDict = orddict:store(NewPrepareTimestamp, TxId, PreparedDict),
          Result = gingko_vnode:prepare(Key, TxId, NewPrepareTimestamp),
          %Result = logging_vnode:append(Node, LogId, LogRecord),
          {Result, NewPrepareTimestamp, NewPreparedDict};
      _ ->
      {{error, no_updates}, 0, PreparedDict}
      end;
    false ->
      {{error, write_conflict}, 0, PreparedDict}
end.


certification_check(Transaction, Updates, CommittedTx, PreparedTx) ->
  TxId = Transaction#transaction.txn_id,
  Certify = antidote:get_txn_property(certify, Transaction#transaction.properties),
  case Certify of
    true ->
      certification_with_check(TxId, Updates, CommittedTx, PreparedTx);
    false -> true
  end.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_with_check(_, [], _, _) ->
  true;
certification_with_check(TxId, [H | T], CommittedTx, PreparedTx) ->
  TxLocalStartTime = TxId#tx_id.local_start_time,
  {Key, _, _} = H,
  case get_committed_txn(CommittedTx, Key) of
    {ok, CommitTime} ->
      case CommitTime > TxLocalStartTime of
        true ->
          false;
        false ->
          case check_prepared(TxId, PreparedTx, Key) of
            true ->
              certification_with_check(TxId, T, CommittedTx, PreparedTx);
            false ->
              false
          end
      end;
    not_found ->
      case check_prepared(TxId, PreparedTx, Key) of
        true ->
          certification_with_check(TxId, T, CommittedTx, PreparedTx);
        false ->
          false
      end
  end.




%%%===================================================================
%%%  Ets tables
%%%
%%%  committed_tx_cache: the transaction commit time of the last committed
%%%                      transaction for each key.
%%%===================================================================

set_prepared(_PreparedTx, [], _TxId, _Time, Acc) ->
  Acc;
set_prepared(PreparedTx, [{Key, _Type, _Update} | Rest], TxId, Time, Acc) ->
  ActiveTxs = antidote_ets_txn_caches:get_prepared_txn_by_key_and_table(PreparedTx, Key),
  case lists:keymember(TxId, 1, ActiveTxs) of
    true ->
      set_prepared(PreparedTx, Rest, TxId, Time, Acc);
    false ->
      true = antidote_ets_txn_caches:insert_prepared_txn_by_table(PreparedTx, Key, [{TxId, Time} | ActiveTxs]),
      set_prepared(PreparedTx, Rest, TxId, Time, dict:append_list(Key, ActiveTxs, Acc))
  end.


reset_prepared(_PreparedTx, [], _TxId, _Time, _ActiveTxs) ->
  ok;
reset_prepared(PreparedTx, [{Key, _Type, _Update} | Rest], TxId, Time, ActiveTxs) ->
  %% Could do this more efficiently in case of multiple updates to the same key
  true = antidote_ets_txn_caches:insert_prepared_txn_by_table(PreparedTx, Key, [{TxId, Time} | dict:fetch(Key, ActiveTxs)]),
  reset_prepared(PreparedTx, Rest, TxId, Time, ActiveTxs).


-spec create_committed_txns_cache() -> cache_id().
create_committed_txns_cache() ->
  ets:new(committed_tx, [set]).


-spec get_committed_txn(cache_id(), key()) -> not_found | {ok, clock_time()}.
get_committed_txn(CommittedTxCache, Key) ->
  case ets:lookup(CommittedTxCache, Key) of
    [{_, CommitTime}] ->
      {ok, CommitTime};
    [] ->
      not_found
  end.


check_prepared(_TxId, PreparedTx, Key) ->
  antidote_ets_txn_caches:is_prepared_txn_by_table(PreparedTx, Key).