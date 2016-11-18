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

%%@doc: This module has methods to calculate different metrics. Currently only
%%      metric staleness is supported.
-module(antidote_stats).

%% API
-export([get_value/1, stats/0]).

%% List of configured metrics
stats() ->
    [[staleness]].

get_value(Metric) ->
    calculate(Metric).

%% Calculate staleness by comparing the current stable snapshot and current local
%% time. Staleness is local time - minimum(entry in stable snapshot)
%% Return staleness in millisecs
calculate([staleness]) ->
    {ok, SS} = dc_utilities:get_stable_snapshot(),
    CurrentClock = to_microsec(os:timestamp()),
    Staleness = dict:fold(fun(_K, C, Max) ->
                                   max(CurrentClock - C, Max)
                           end, 0, SS),
    Staleness/(1000); %% To millisecs

calculate(_) ->
    {error, metric_not_found}.

to_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.
