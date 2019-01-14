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

-module(zmq_context).
-behaviour(gen_server).

%% ZMQ context manager
%% In order to use ZeroMQ, a common context instance is needed (http://api.zeromq.org/4-0:zmq-ctx-new).
%% The sole purpose of this gen_server is to provide this instance, and to terminate it gracefully.

-export([start_link/0, get/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) -> erlzmq:context().

handle_call(get_context, _From, Ctx) ->
    {reply, Ctx, Ctx}.

handle_cast(_Request, Ctx) ->
    {noreply, Ctx}.

handle_info(_Info, Ctx) ->
    {noreply, Ctx}.

terminate(_Reason, Ctx) ->
    erlzmq:term(Ctx).

code_change(_OldVsn, Ctx, _Extra) ->
    {ok, Ctx}.

%% Context is a NIF object handle
get() ->
    gen_server:call(?MODULE, get_context).
