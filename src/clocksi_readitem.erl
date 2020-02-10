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

-module(clocksi_readitem).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([read_data_item/5,
    async_read_data_item/6]).

%% Internal
-export([perform_read_internal/5]).

%% Spawn
-type read_property_list() :: [].
-export_type([read_property_list/0]).
%%%===================================================================
%%% API
%%%===================================================================

-spec read_data_item(index_node(), key(), type(), tx(), read_property_list()) -> {error, term()} | {ok, snapshot()}.
read_data_item({Partition, Node}, Key, Type, Transaction, PropertyList) ->
    rpc:call(Node, ?MODULE, perform_read_internal, [Key, Type, Transaction, PropertyList, Partition]).

-spec async_read_data_item(index_node(), key(), type(), tx(), read_property_list(), term()) -> ok.
async_read_data_item({Partition, Node}, Key, Type, Transaction, PropertyList, {fsm, Sender}) ->
    spawn_link(Node, fun() -> {
        case perform_read_internal(Key, Type, Transaction, PropertyList, Partition) of
            {ok, Snapshot} ->
                gen_statem:cast(Sender, {ok, {Key, Type, Snapshot}});
            {error, Reason} ->
                gen_statem:cast(Sender, {error, Reason})
        end
    } end),
    ok.


%%%===================================================================
%%% Internal
%%%===================================================================

-spec perform_read_internal(key(), type(), tx(), read_property_list(), partition_id()) ->
    {error, term()} | {ok, snapshot()}.
perform_read_internal(Key, Type, Transaction, PropertyList, Partition) ->
    TxId = Transaction#transaction.txn_id,
    TxLocalStartTime = TxId#tx_id.local_start_time,
    case check_clock(Key, TxLocalStartTime, Partition) of
        {not_ready, Time} ->
            timer:sleep(Time),
            perform_read_internal(Key, Type, Transaction, PropertyList, Partition);
        ready ->
            return(Key, Type, Transaction, PropertyList, Partition)
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
-spec check_clock(key(), clock_time(), partition_id()) ->
                         {not_ready, clock_time()} | ready.
check_clock(Key, TxLocalStartTime, Partition) ->
    Time = dc_utilities:now_microsec(),
    case TxLocalStartTime > Time of
        true ->
            {not_ready, (TxLocalStartTime - Time) div 1000 +1};
        false ->
            check_prepared(Key, TxLocalStartTime, Partition)
    end.

%% @doc check_prepared: Check if there are any transactions
%%      being prepared on the transaction being read, and
%%      if they could violate the correctness of the read
-spec check_prepared(key(), clock_time(), partition_id()) ->
                            ready | {not_ready, ?SPIN_WAIT}.
check_prepared(Key, TxLocalStartTime, Partition) ->
    {ok, ActiveTxs} = clocksi_vnode:get_active_txns_key(Key, Partition),
    check_prepared_list(Key, TxLocalStartTime, ActiveTxs).

-spec check_prepared_list(key(), clock_time(), [{txid(), clock_time()}]) ->
                                 ready | {not_ready, ?SPIN_WAIT}.
check_prepared_list(_Key, _TxLocalStartTime, []) ->
    ready;
check_prepared_list(Key, TxLocalStartTime, [{_TxId, Time}|Rest]) ->
    case Time =< TxLocalStartTime of
        true ->
            {not_ready, ?SPIN_WAIT};
        false ->
            check_prepared_list(Key, TxLocalStartTime, Rest)
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
-spec return(key(), type(), tx(), read_property_list(), partition_id()) -> {error, term()} | {ok, snapshot()}.
return(Key, Type, Transaction, PropertyList, Partition) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    TxId = Transaction#transaction.txn_id,
    materializer_vnode:read(Key, Type, VecSnapshotTime, TxId, PropertyList, Partition).



-ifdef(TEST).

check_prepared_list_test() ->
    ?assertEqual({not_ready, ?SPIN_WAIT}, check_prepared_list(key, 100, [{tx1, 200}, {tx2, 50}])),
    ?assertEqual(ready, check_prepared_list(key, 100, [{tx1, 200}, {tx2, 101}])).



-endif.
