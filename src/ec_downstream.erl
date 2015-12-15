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
-module(ec_downstream).

-include("antidote.hrl").

-export([generate_downstream_op/5]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Node :: term(), Key :: key(),
    Type :: type(), Update :: op(), list()) ->
  {ok, op()} | {error, atom()}.
generate_downstream_op(Node, Key, Type, Update, WriteSet) ->
  {Op, Actor} = Update,
  case ec_vnode:read_data_item(Node, Key, Type, WriteSet) of
    {ok, Snapshot} ->
      case Type:update(Op, Actor, Snapshot) of
        {ok, NewState} ->
          {ok, {merge, NewState}};
        {error, Reason} ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.