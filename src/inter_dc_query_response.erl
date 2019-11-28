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

-module(inter_dc_query_response).
-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([start_link/1,
         get_entries/2,
         request_permissions/2,
         generate_server_name/1]).
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
      id :: non_neg_integer()}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link(non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Num) ->
    gen_server:start_link({local, generate_server_name(Num)}, ?MODULE, [Num], []).

-spec get_entries(binary(), inter_dc_query_state()) -> ok.
get_entries(BinaryQuery, QueryState) ->
    ok = gen_server:cast(generate_server_name(rand:uniform(?INTER_DC_QUERY_CONCURRENCY)), {get_entries, BinaryQuery, QueryState}).

-spec request_permissions(binary(), inter_dc_query_state()) -> ok.
request_permissions(BinaryRequest, QueryState) ->
    ok = gen_server:cast(generate_server_name(rand:uniform(?INTER_DC_QUERY_CONCURRENCY)), {request_permissions, BinaryRequest, QueryState}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Num]) ->
    {ok, #state{id=Num}}.

handle_cast({get_entries, BinaryQuery, QueryState}, State) ->
    {read_log, Partition, From, To} = binary_to_term(BinaryQuery),
    Entries = get_entries_internal(Partition, From, To),
    BinaryResp = term_to_binary({{dc_utilities:get_my_dc_id(), Partition}, Entries}),
    BinaryPartition = inter_dc_txn:partition_to_bin(Partition),
    FullResponse = <<BinaryPartition/binary, BinaryResp/binary>>,
    ok = inter_dc_query_receive_socket:send_response(FullResponse, QueryState),
    {noreply, State};

handle_cast({request_permissions, BinaryRequest, QueryState}, State) ->
    {request_permissions, Operation, _Partition, _From, _To} = binary_to_term(BinaryRequest),
    BinaryResp = BinaryRequest,
    ok = bcounter_mgr:process_transfer(Operation),
    ok = inter_dc_query_receive_socket:send_response(BinaryResp, QueryState),
    {noreply, State};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

-spec get_entries_internal(partition_id(), log_opid(), log_opid()) -> [interdc_txn()].
get_entries_internal(Partition, From, To) ->
  Node = case lists:member(Partition, dc_utilities:get_my_partitions()) of
             true -> node();
             false ->
                 log_utilities:get_my_node(Partition)
         end,
  Logs = log_read_range(Partition, Node, From, To),
  Asm = log_txn_assembler:new_state(),
  {OpLists, _} = log_txn_assembler:process_all(Logs, Asm),
  Txns = lists:map(fun(TxnOps) -> inter_dc_txn:from_ops(TxnOps, Partition, none) end, OpLists),
  %% This is done in order to ensure that we only send the transactions we committed.
  %% We can remove this once the read_log_range is reimplemented.
  lists:filter(fun inter_dc_txn:is_local/1, Txns).

%% TODO: re-implement this method efficiently once the log provides efficient access by partition and DC (Santiago, here!)
%% TODO: also fix the method to provide complete snapshots if the log was trimmed
-spec log_read_range(partition_id(), node(), log_opid(), log_opid()) -> [log_record()].
log_read_range(Partition, Node, From, To) ->
  {ok, RawOpList} = logging_vnode:read({Partition, Node}, [Partition]),
  OpList = lists:map(fun({_Partition, Op}) -> Op end, RawOpList),
  filter_operations(OpList, From, To).

-spec filter_operations([log_record()], log_opid(), log_opid()) -> [log_record()].
filter_operations(Ops, Min, Max) ->
  F = fun(Op) ->
    Num = Op#log_record.op_number#op_number.local,
    (Num >= Min) and (Max >= Num)
  end,
  lists:filter(F, Ops).

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_server_name(Id) ->
    list_to_atom("inter_dc_query_response" ++ integer_to_list(Id)).
