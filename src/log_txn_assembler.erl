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

-export([process/2, process_all/2, new_state/0]).

-record(state, {
  op_buffer :: dict()
}).

new_state() -> #state{op_buffer = dict:new()}.

-spec process(operation(), any()) -> {{ok, [operation()]}, any()} | {none, any()}.
process(Operation, State) ->
  Payload = Operation#operation.payload,
  TxId = Payload#log_record.tx_id,
  NewTxnBuf = find_or_default(TxId, [], State#state.op_buffer) ++ [Operation],
  case Payload#log_record.op_type of
    commit -> {{ok, NewTxnBuf}, State#state{op_buffer = dict:erase(TxId, State#state.op_buffer)}};
    abort -> {none, State#state{op_buffer = dict:erase(TxId, State#state.op_buffer)}};
    _ -> {none, State#state{op_buffer = dict:store(TxId, NewTxnBuf, State#state.op_buffer)}}
  end.

process_all(LogRecords, State) -> process_all(LogRecords, [], State).
process_all([], Accu, State) -> {Accu, State};
process_all([H|T], Accu, State) ->
  {Result, NewState} = process(H, State),
  case Result of
    {ok, Txn} -> process_all(T, Accu ++ [Txn], NewState);
    none -> process_all(T, Accu, NewState)
  end.

find_or_default(Key, Default, Dict) ->
  case dict:find(Key, Dict) of
    {ok, Val} -> Val;
    _ -> Default
  end.
