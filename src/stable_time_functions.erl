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

%% These functions are used to instantiate a meta_data_sender for vectorclocks.

-module(stable_time_functions).
-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-include_lib("kernel/include/logger.hrl").

-export([update/2,
         merge/1,
         lookup/2,
         fold/3,
         store/3,
         default/0,
         initial_local/0,
         initial_merged/0]).

default() ->
    vectorclock:new().

initial_merged() ->
    vectorclock:new().

initial_local() ->
    vectorclock:new().

fold(X, Y, Z) ->
    vectorclock:fold(X, Y, Z).

lookup(X, Y) ->
    vectorclock:get(X, Y).

store(X, Y, Z) ->
    vectorclock:set(X, Y, Z).


%% Checks whether entry should be updated.
-spec update(integer(), integer()) -> boolean().
update(Last, Time) ->
    case Last of
        undefined ->
            true;
        _ ->
            Time >= Last
    end.


%% The function merges all entries in a map of vectorclocks by taking the minimum of all entries per node per DC
%% This assumes the meta data being sent have all DCs
-spec merge(map()) -> vectorclock:vectorclock().
merge(VcMap) ->
    case all_defined(VcMap) of
        true -> vectorclock:min(maps:values(VcMap));
        false ->
            ?LOG_DEBUG("missing entries: ~p", [VcMap]),
            vectorclock:new()
    end.

all_defined(VcMap) ->
    maps:fold(fun (_K, V, Acc) -> Acc andalso V =/= undefined end, true, VcMap).