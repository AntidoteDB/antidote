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

-export([generate_downstream_op/6]).

%% @doc Returns downstream operation for upstream operation
-spec generate_downstream_op(#transaction{}, Node::term(), Key::key(),
                             Type::type(), Update::op(), [DownstreamOps::term()]) ->
                                    {ok, op()} | {error, atom()}.
generate_downstream_op(Transaction, Node, Key, Type, Update, DownstreamOps) ->
    {Op, Actor} =  Update,
    case clocksi_vnode:read_data_item(Node,
                                      Transaction,
                                      Key,
                                      Type) of
        {ok, Snapshot} ->
            DownstreamOp = case Type of
                            crdt_bcounter ->
                                Snapshot2 = apply_operations(Snapshot, Type, DownstreamOps),
                                case Type:generate_downstream(Op, Actor, Snapshot2) of
                                    {ok, OpParam} -> {update, OpParam};
                                    {error, Error} -> {error, Error}
                                end;
                            crdt_orset ->
                                Snapshot2 = apply_operations(Snapshot, Type, DownstreamOps),
                                {ok, OpParam} = Type:generate_downstream(Op, Actor, Snapshot2),
                                {update, OpParam};
                            crdt_pncounter ->
                                Snapshot2 = apply_operations(Snapshot, Type, DownstreamOps),
                                {ok, OpParam} = Type:generate_downstream(Op, Actor, Snapshot2),
                                {update, OpParam};
                            _ ->
                                Snapshot2 = update_snapshot(Snapshot, DownstreamOps), 
                                {ok, NewState} = Type:update(Op, Actor, Snapshot2),
                                {merge, NewState}
                            end,
            case DownstreamOp of
                {error, Reason} -> {error, Reason};
                _ -> {ok, DownstreamOp}
            end;
        {error, no_snapshot} ->
            {error, no_snapshot}
    end.

update_snapshot(Snapshot, DownstreamOps) ->
    case DownstreamOps of
        [] ->
            Snapshot;
        List ->
            {merge, Snapshot2} = lists:last(List),
            Snapshot2
    end.

apply_operations(Snapshot, _Type, []) ->
    Snapshot;

apply_operations(Snapshot, Type, [Op|Rest]) ->
    {update, OpParam} = Op,
    {ok, Snapshot2} = Type:update(OpParam, Snapshot),
    apply_operations(Snapshot2, Type, Rest).
