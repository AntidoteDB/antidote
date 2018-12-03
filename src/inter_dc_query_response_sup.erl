%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(inter_dc_query_response_sup).

-behavior(supervisor).

-include("antidote.hrl").

-export([start_link/1]).

-export([init/1]).

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

generate_module_name(N) ->
    list_to_atom(atom_to_list(?MODULE) ++ "-" ++ integer_to_list(N)).

generate_worker_spec(N) ->
    Module = generate_module_name(N),
    {Module,
     {inter_dc_query_response, start_link, [N]},
     permanent, 5000, worker, [inter_dc_query_response]}.

%% @doc Start the log response readers
init(Num) ->
    Pool = [generate_worker_spec(N) || N <- lists:seq(1, Num)],
    {ok, {{one_for_one, 5, 10}, Pool}}.
