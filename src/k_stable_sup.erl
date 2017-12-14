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
%%
%% Supervisor for k-stable functions
%% Call path:
%% antidote_sup.erl starts this supervisor
%% this supervisor starts the gen_server
%%
%% -------------------------------------------------------------------
-module(k_stable_sup).

-behavior(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Init) ->
    lager:info("start_link ~p", [Init]),
    supervisor:start_link({local, ?MODULE}, ?MODULE, Init).

%% TODO: Something is wrong here

%% "{application_start_failure,antidote,{
%%      {shutdown,{failed_to_start_child,k_stable_sup,
%%          {shutdown,{failed_to_start_child,k_stable_server,
%%              {undef,[
%%              {global,init,
%%                  [k_stable, {global,'k_stable_antidote@127.0.0.1'}],[]}

init(Init) ->
    lager:info("init ~p", [Init]),
    ChildSpec = {k_stable_server,
        {k_stable, start_link, [Init]},
        permanent, 5000, worker, [k_stable]},
    SupFlags = {one_for_one, 5, 10},
    {ok, {SupFlags, [ChildSpec]}}.
