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

-module(clocksi_interactive_coord_worker_sup).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behavior(supervisor).

-export([start_link/1]).

-export([init/1]).

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, []).

%% @doc Starts the coordinator of a ClockSI static transaction.
init([]) ->
    Worker = {undefined,
              {clocksi_interactive_coord, start_link, []},
               temporary, 5000, worker, [clocksi_interactive_coord]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.
