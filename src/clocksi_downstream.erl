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
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Transaction :: tx(), Node :: term(), Key :: key(),
    Type :: type(), Update :: op(), list()) ->
    {ok, op()} | {error, atom()}.
generate_downstream_op(Transaction, Node, Key, Type, Update, WriteSet) ->
    try
        {Op, Actor} = Update,
        case clocksi_vnode:read_data_item(Node,
            Transaction,
            Key,
            Type,
            WriteSet) of
            {ok, Snapshot} ->
                case (string:str(Type, "riak_dt")) of
                    0 -> %% Dealing with an op_based CRDT
                        {ok, OpParam} =
                            Type:generate_downstream(Op, Actor, Snapshot),
                        {ok, {update, OpParam}};
                    1 -> %% Dealing with a state_based CRDT
                        {ok, NewState} = Type:update(Op, Actor, Snapshot),
                        {ok, {merge, NewState}};
                    _ ->
                        {error, {"Unknown datatype.", Type}}
                end;
            {error, Reason} ->
                {error, Reason}
        end
    catch %% This catch aims at throwing an error when a key is not of Type.
        error : Error ->
            {error, Error}
    end.
