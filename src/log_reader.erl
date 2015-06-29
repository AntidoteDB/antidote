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

%% Log reader reads all transactions in the log that happened between the defined
-module(log_reader).
-include("antidote.hrl").

-export([get_entries/3]).

get_entries(Partition, From, To) ->
  Logs = log_read_range(node(), Partition, From, To),
  Asm = log_txn_assembler:new_state(),
  {Txns, _} = log_txn_assembler:process_all(Logs, Asm),
  Txns.

%% TODO: reimplement this method efficiently once the log provides true, efficient sequential access
%% TODO: also fix the method to provide complete snapshots if the log was trimmed
log_read_range(Node, Partition, From, To) ->
  {ok, RawOpList} = logging_vnode:read({Partition, Node}, [Partition]),
  OpList = lists:map(fun({_Partition, Op}) -> Op end, RawOpList),
  filter_operations(OpList, From, To).

filter_operations(Ops, Min, Max) ->
  F = fun(Op) ->
    {Num, _Node} = Op#operation.op_number,
    (Num >= Min) and (Max >= Num)
  end,
  lists:filter(F, Ops).


