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

%%@doc: This module periodically collects different metrics (currently only staleness)
%%  and updates exometer metrics. Monitoring tools can then read it from exometer.

-module(antidote_stats_collector).

-behaviour(gen_server).
%% Interval to collect metrics
-define(INTERVAL, 10000). %% 10 sec
%% Metrics collection will be started after INIT_INTERVAL after application startup.
-define(INIT_INTERVAL, 10000). %% 10 seconds

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    init_metrics(),
    % set the error logger counting the number of errors during operation
    ok = error_logger:add_report_handler(antidote_error_monitor),
    % start the timer for updating the calculated metrics
    Timer = erlang:send_after(?INIT_INTERVAL, self(), periodic_update),
    {ok, Timer}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(periodic_update, OldTimer) ->
    erlang:cancel_timer(OldTimer),
    update_staleness(),
    Timer = erlang:send_after(?INTERVAL, self(), periodic_update),
    {noreply, Timer}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_staleness() ->
    Val = calculate_staleness(),
    prometheus_histogram:observe(antidote_staleness, Val).

init_metrics() ->
    prometheus_counter:new([{name, antidote_error_count}, {help, "The number of error encountered during operation"}]),
    prometheus_histogram:new([{name, antidote_staleness}, {help, "The staleness of the stable snapshot"}, {buckets, [1, 10, 100, 1000, 10000]}]),
    prometheus_gauge:new([{name, antidote_open_transactions}, {help, "Number of open transactions"}]),
    prometheus_counter:new([{name, antidote_aborted_transactions_total}, {help, "Number of aborted transactions"}]),
    prometheus_counter:new([{name, antidote_operations_total}, {help, "Number of operations executed"}, {labels, [type]}]).

calculate_staleness() ->
    {ok, SS} = dc_utilities:get_stable_snapshot(),
    CurrentClock = to_microsec(os:timestamp()),
    Staleness = dict:fold(fun(_K, C, Max) ->
                                   max(CurrentClock - C, Max)
                           end, 0, SS),
    round(Staleness/(1000)). %% To millisecs

to_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.
