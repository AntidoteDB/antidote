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
-module(clocksi_downstream).

-include("antidote.hrl").

-export([generate_downstream_op/7]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Transaction :: tx(), Node :: index_node(), Key :: key(),
  Type :: type(), Update :: op_param(), list(), orddict:orddict()) ->
    {ok, effect()} | {error, atom()}.
generate_downstream_op(Transaction, IndexNode, Key, Type, Update, WriteSet, InternalReadSet) ->
    %% TODO: Check if read can be omitted for some types as registers
    NeedState = Type:require_state_downstream(Update),
    Result =
        %% If state is needed to generate downstream, get it from txn buffer or materializer cache.
        case NeedState of
          true ->
            case orddict:find(Key, InternalReadSet) of
              {ok, S} ->
                  S;
              error ->
                  case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
                      {ok, S}->
                          S;
                      {error, Reason}->
                          {error, {gen_downstream_read_failed, Reason}}
                  end
            end;
          false ->
              {ok, ignore} %Use a dummy value
        end,
    case Result of
        {error, R} ->
            {error, R}; %% {error, Reason} is returned here.
        Snapshot ->
            case Type of
                antidote_crdt_counter_b ->
                    %% bcounter data-type.
                    bcounter_mgr:generate_downstream(Key, Update, Snapshot);
                _ ->
                    Type:downstream(Update, Snapshot)
            end
    end.
