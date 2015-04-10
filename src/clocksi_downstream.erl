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

-export([generate_downstream_op/5]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Transaction::tx(), Node::term(), Key::key(),
                             Type::type(), Update::op()) ->
                                    {ok, op()} | {error, atom()}.
generate_downstream_op(Transaction, Node, Key, Type, Update) ->
    {Op, Actor} =  Update,
    case clocksi_vnode:read_data_item(Node,
                                      Transaction,
                                      Key,
                                      Type) of
        {ok, Snapshot} ->
            DownstreamOp = case Type of
                            crdt_bcounter ->
                                case Type:generate_downstream(Op, Actor, Snapshot) of
                                    {ok, OpParam} -> {update, OpParam};
                                    {error, Error} -> {error, Error}
                                end;
                            crdt_orset ->
                                {ok, OpParam} = Type:generate_downstream(Op, Actor, Snapshot),
                                {update, OpParam};
                            crdt_pncounter ->
                                {ok, OpParam} = Type:generate_downstream(Op, Actor, Snapshot),
                                {update, OpParam};
                            _ ->
                                {ok, NewState} = Type:update(Op, Actor, Snapshot),
                                {merge, NewState}
                            end,
            case DownstreamOp of
                {error, Reason} -> {error, Reason};
                _ -> {ok, DownstreamOp}
            end;
        {error, no_snapshot} ->
            {error, no_snapshot}
    end.
