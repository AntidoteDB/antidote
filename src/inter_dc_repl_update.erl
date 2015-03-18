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

%% @doc : This module has functions to process updates from remote DCs

-module(inter_dc_repl_update).

-include("inter_dc_repl.hrl").
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([process_update/2]).

%% @doc enqueue_update: Put transaction into queue for processing later
-spec process_update(Transaction::ec_transaction_reader:transaction(), Partition::non_neg_integer()) -> ok.
process_update(Transaction, Partition) ->
    {_,Ops} = Transaction,
    Node = {Partition,node()},
    FirstOp = hd(Ops),
    FirstRecord = FirstOp#operation.payload,
    LogId = case FirstRecord#log_record.op_type of
                update ->
                    {Key1,_Type,_Op} = FirstRecord#log_record.op_payload,
                    log_utilities:get_logid_from_key(Key1);
                noop ->
                    error;
                _ ->
                    lager:error("Wrong transaction record format"),
                    erlang:error(bad_transaction_record)
            end,
    lists:foreach(
      fun(Op) ->
              Logrecord = Op#operation.payload,
              case Logrecord#log_record.op_type of
                  noop ->
                      lager:debug("Heartbeat Received");
                  update ->
                      logging_vnode:append(Node, LogId, Logrecord);
                  _ -> %% prepare or commit
                      logging_vnode:append(Node, LogId, Logrecord),
                      lager:debug("Prepare/Commit record")
                      %%TODO Write this to log
              end
      end, Ops),
    DownOps =
        ec_transaction_reader:get_update_ops_from_transaction(
          Transaction),
    lists:foreach( fun(DownOp) ->
                           Key = DownOp#ec_payload.key,
                           ok = materializer_vnode:update(Key, DownOp)
                   end, DownOps),
    lager:debug("Update from remote DC applied:",[payload]),
    ok.
