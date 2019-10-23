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

%% This module accepts different metric calls and provides them to the prometheus endpoint

-module(antidote_stats_collector).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    logger:info("Initialized stats process"),
    init_metrics(),
    {ok, []}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.


handle_cast(log_error, State) ->
    prometheus_counter:inc(antidote_error_count),
    {noreply, State};

handle_cast(open_transaction, State) ->
    prometheus_gauge:inc(antidote_open_transactions),
    {noreply, State};

handle_cast(transaction_aborted, State) ->
    prometheus_counter:inc(antidote_aborted_transactions_total),
    {noreply, State};

handle_cast(transaction_finished, State) ->
    prometheus_gauge:dec(antidote_open_transactions),
    {noreply, State};

handle_cast(operation_read_async, State) ->
    prometheus_counter:inc(antidote_operations_total, [read_async]),
    {noreply, State};

handle_cast(operation_read, State) ->
    prometheus_counter:inc(antidote_operations_total, [read]),
    {noreply, State};

handle_cast(operation_update, State) ->
    prometheus_counter:inc(antidote_operations_total, [update]),
    {noreply, State};

handle_cast({update_staleness, Val}, State) ->
    prometheus_histogram:observe(antidote_staleness, Val),
    {noreply, State};

handle_cast(StatRequest, State) ->
    logger:warning("Unknown stat update requested: ~p",[StatRequest]),
    {noreply, State}.

handle_info(ok, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

init_metrics() ->
    prometheus_counter:new([{name, antidote_error_count}, {help, "The number of error encountered during operation"}]),
    prometheus_histogram:new([{name, antidote_staleness}, {help, "The staleness of the stable snapshot"}, {buckets, [1, 10, 100, 1000, 10000]}]),
    prometheus_gauge:new([{name, antidote_open_transactions}, {help, "Number of open transactions"}]),
    prometheus_counter:new([{name, antidote_aborted_transactions_total}, {help, "Number of aborted transactions"}]),
    prometheus_counter:new([{name, antidote_operations_total}, {help, "Number of operations executed"}, {labels, [type]}]).

