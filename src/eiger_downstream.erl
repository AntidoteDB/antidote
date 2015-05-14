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
-module(eiger_downstream).

-include("antidote.hrl").

-export([generate_downstream_op/1]).

generate_downstream_op(Update) ->
    {_Key, Type, {Op, Actor}} = Update,
    case Type of
        crdt_pncounter ->
            {ok, OpParam} = Type:generate_downstream(Op, Actor, useless),
            {update, OpParam};
        _ ->
            {error, type_not_supported_in_eiger}
    end.
