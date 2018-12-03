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

-module(clocksi_downstream).

-include("antidote.hrl").

-export([generate_downstream_op/6]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Transaction :: tx(), Node :: index_node(), Key :: key(),
  Type :: type(), Update :: op_param(), list()) ->
    {ok, effect()} | {error, atom()}.
generate_downstream_op(Transaction, IndexNode, Key, Type, Update, WriteSet) ->
    %% TODO: Check if read can be omitted for some types as registers
    NeedState = Type:require_state_downstream(Update),
    Result =
        %% If state is needed to generate downstream, read it from the partition.
        case NeedState of
          true ->
            case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
                {ok, S}->
                    S;
                {error, Reason}->
                    {error, {gen_downstream_read_failed, Reason}}
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
