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

-module(antidote_utilities).

-export([execute_remote/2]).


%% @doc This calls a given function on a possible remote host. Therefor
%%      a new process is spawned on the node. This function waits for
%%      the result of the remote function.
-spec execute_remote(node(), fun(() -> Result)) -> Result.
execute_remote(Node, Function) ->
    Sender = self(),
    Ref = make_ref(),
    spawn_link(Node, fun() -> Sender ! {Ref, Function()} end),
    receive
        {Ref, Result} -> Result
    end.
