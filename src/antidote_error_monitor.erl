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

-module(antidote_error_monitor).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_info/2, terminate/2]).

init(_Args) ->
  {ok, []}.

handle_event({error, _Gleader, {_Pid, _Format, _Data}}, State) ->
  prometheus_counter:inc(antidote_error_count),
  {ok, State};

handle_event({error_report, _Gleader, {_Pid, std_error, _Report}}, State) ->
  prometheus_counter:inc(antidote_error_count),
  {ok, State};

handle_event({error_report, _Gleader, {_Pid, _Type, _Report}}, State) ->
  prometheus_counter:inc(antidote_error_count),
  {ok, State};

handle_event(_, State) ->
  {ok, State}.

handle_info(_, State) ->
  {ok, State}.

terminate(_Args, _State) ->
  ok.
