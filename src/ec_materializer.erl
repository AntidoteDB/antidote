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
-module(ec_materializer).
-include("antidote.hrl").

%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
         materialize/6,
         materialize_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec new(type()) -> term().
new(Type) ->
    Type:new().


%% @doc materialize_eager: apply updates in order without any checks
-spec materialize_eager(type(), snapshot(), [ec_payload()]) -> snapshot().
materialize_eager(_, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op|Rest]) ->
   case Op of
        {merge, State} ->
            NewSnapshot = Type:merge(Snapshot, State);
        {update, DownstreamOp} ->
            {ok, NewSnapshot} = Type:update(DownstreamOp, Snapshot)
    end,
    materialize_eager(Type, NewSnapshot, Rest).
