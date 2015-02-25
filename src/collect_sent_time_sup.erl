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
%% @doc Supervise the fsm.
-module(collect_sent_time_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(_Args) ->
    supervisor:start_child(?MODULE, _Args).

%% @doc Supervisor for the fsm to collect the sent times
%% should start one for each external DC, with the DC address
%% as the input args, but these args cn be put in when
%% starting the supervisor?
init([]) ->
    lager:info("collect_sent_time_sup: Starting fsm..."),
    Worker = {collect_sent_time_fsm,
              {collect_sent_time_fsm, start_link, []},
              transient, 5000, worker, []},
    lager:info("collect_sent_time_sup: done."),
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.
