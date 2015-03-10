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
-spec generate_downstream_op(#transaction{}, Key::key(),
                             Type::type(), Update::op(), WriteSet::list(), IsLocal::atom()) ->
                                    {ok, op()} | {error, atom()}.
generate_downstream_op(Transaction, Key, Type, Update, WriteSet, IsLocal) ->
    {Op, Actor} =  Update,
    case clocksi_vnode:read_data_item(Transaction,
                                      Key, Type, IsLocal, self(), WriteSet) of
        {ok, Snapshot, internal} ->
            {ok, NewState} = Type:update(Op, Actor, Snapshot),
            DownstreamOp = {merge, NewState},
            {ok, DownstreamOp};
        {error, Reason} ->
            lager:error("Error: ~p", [Reason]),
            {error, Reason};
	Reply ->
	    lager:error("Got weird reply from read_data_item: ~p", [Reply]),
	    {error, Reply}
    end.
