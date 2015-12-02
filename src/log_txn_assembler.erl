%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% Transaction assembler reads a stream of log operations and produces complete transactions.

-module(log_txn_assembler).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% If you can fix the dialyzer warns for process_all/3, be my guest.
-dialyzer({nowarn_function, process_all/3}).

%% API
-export([
  new_state/0,
  process/2,
  process_all/2]).

%% State
-record(state, {
  op_buffer :: dict()
}).

%%%% API --------------------------------------------------------------------+

-spec new_state() -> #state{}.
new_state() -> #state{op_buffer = dict:new()}.

-spec process(operation(), #state{}) -> {{ok, [operation()]} | none, #state{}}.
process(Operation, State) ->
  Payload = Operation#operation.payload,
  TxId = Payload#log_record.tx_id,
  NewTxnBuf = find_or_default(TxId, [], State#state.op_buffer) ++ [Operation],
  case Payload#log_record.op_type of
    commit -> {{ok, NewTxnBuf}, State#state{op_buffer = dict:erase(TxId, State#state.op_buffer)}};
    abort -> {none, State#state{op_buffer = dict:erase(TxId, State#state.op_buffer)}};
    _ -> {none, State#state{op_buffer = dict:store(TxId, NewTxnBuf, State#state.op_buffer)}}
  end.

%%-spec process_all([operation()], #state{}) -> {[#interdc_txn{}], #state{}}. %% TODO: fix this spec
-spec process_all([#operation{}],#state{op_buffer::'undefined' | dict()}) -> {[],#state{op_buffer::'undefined' | dict()}}.
process_all(LogRecords, State) -> process_all(LogRecords, [], State).

-spec process_all([#operation{}], [#interdc_txn{}], #state{}) -> {[#interdc_txn{}], #state{}}.
process_all([], Acc, State) -> {Acc, State};
process_all([H|T], Acc, State) ->
  {Result, NewState} = process(H, State),
  NewAcc = case Result of
    {ok, Txn} -> Acc ++ [Txn];
    none -> Acc
  end,
  process_all(T, NewAcc, NewState).

%%%% Methods ----------------------------------------------------------------+

-spec find_or_default(#tx_id{}, any(), dict()) -> any().
find_or_default(Key, Default, Dict) ->
  case dict:find(Key, Dict) of
    {ok, Val} -> Val;
    _ -> Default
  end.
