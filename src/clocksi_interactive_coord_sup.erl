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

-module(clocksi_interactive_coord_sup).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behavior(supervisor).

-include("antidote.hrl").

-export([start_fsm/1,
         start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    _Res = rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    Random = rand_compat:uniform(?NUM_SUP),
    Module = generate_module_name(Random),
    supervisor:start_child(Module, Args).

generate_module_name(N) ->
    list_to_atom(atom_to_list(?MODULE) ++ "-" ++ integer_to_list(N)).

generate_supervisor_spec(N) ->
    Module = generate_module_name(N),
    {Module,
     {clocksi_interactive_coord_worker_sup, start_link, [Module]},
      permanent, 5000, supervisor, [clocksi_interactive_coord_worker_sup]}.

%% @doc Starts the coordinator of a ClockSI interactive transaction.
init([]) ->
    Pool = [generate_supervisor_spec(N) || N <- lists:seq(1, ?NUM_SUP)],
    {ok, {{one_for_one, 5, 10}, Pool}}.
