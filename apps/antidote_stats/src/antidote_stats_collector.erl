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

-record(state, {}).
-type state() :: #state{}.

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init([term()]) -> {ok, state()}.
init([]) ->
    logger:info("Initialized stats process"),
    init_metrics(),
    {ok, #state{}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.


handle_cast(log_error, State) ->
    prometheus_counter:inc(antidote_error_count),
    {noreply, State};

handle_cast(log_warning, State) ->
    prometheus_counter:inc(antidote_warning_count),
    {noreply, State};

handle_cast({process_scrape_time, TimeMs}, State) ->
    prometheus_gauge:set(antidote_process_scrape_time, TimeMs),
    {noreply, State};

handle_cast(open_transaction, State) ->
    prometheus_gauge:inc(antidote_open_transactions),
    prometheus_counter:inc(antidote_transaction, [open]),
    {noreply, State};

handle_cast(transaction_aborted, State) ->
    prometheus_counter:inc(antidote_transaction, [aborted]),
    {noreply, State};

handle_cast(transaction_finished, State) ->
    prometheus_counter:inc(antidote_transaction, [finished]),
    prometheus_gauge:dec(antidote_open_transactions),
    {noreply, State};

handle_cast(operation_read_async, State) ->
    prometheus_counter:inc(antidote_operations_total, [read_async]),
    {noreply, State};

handle_cast(operation_read_async_internal, State) ->
    prometheus_counter:inc(antidote_operations_internal_total, [read_async]),
    {noreply, State};

handle_cast(operation_read, State) ->
    prometheus_counter:inc(antidote_operations_total, [read]),
    {noreply, State};

handle_cast(operation_read_internal, State) ->
    prometheus_counter:inc(antidote_operations_internal_total, [read]),
    {noreply, State};

handle_cast(operation_update, State) ->
    prometheus_counter:inc(antidote_operations_total, [update]),
    {noreply, State};

handle_cast(operation_update_internal, State) ->
    prometheus_counter:inc(antidote_operations_internal_total, [update]),
    {noreply, State};

handle_cast({update_staleness, Val}, State) ->
    _ = prometheus_histogram:observe(antidote_staleness, Val),
    {noreply, State};

handle_cast({log_append, LogId, Size}, State) ->
    %% log id is very long, use hashing to shorten it (actual path is not that important)
    prometheus_counter:inc(antidote_log_size, [erlang:phash2(LogId)], Size),
    {noreply, State};

handle_cast({log_reset, LogId}, State) ->
    prometheus_counter:reset(antidote_log_size, [erlang:phash2(LogId)]),
    {noreply, State};

handle_cast(log_read_read, State) ->
    prometheus_counter:inc(antidote_log_access, [read]),
    {noreply, State};

handle_cast(log_read_from, State) ->
    prometheus_counter:inc(antidote_log_access, [from]),
    {noreply, State};

handle_cast({dc_ops_received, Count}, State) ->
    prometheus_counter:inc(antidote_dc_ops_received, Count),
    {noreply, State};

handle_cast({dc_ops_received_size, Bytes}, State) ->
    prometheus_counter:inc(antidote_dc_ops_received_size, Bytes),
    {noreply, State};

handle_cast({dc_count, Number}, State) ->
    prometheus_gauge:set(antidote_dc_count, Number),
    {noreply, State};

handle_cast({process_reductions, Name, Reductions}, State) ->
    prometheus_gauge:set(antidote_process_reductions, [Name], Reductions),
    {noreply, State};

handle_cast({process_message_queue_length, Name, Length}, State) ->
    prometheus_gauge:set(antidote_process_message_queue_length, [Name], Length),
    {noreply, State};

%% =================
%% RIAK_CORE METRICS
%% =================

%% riak core ring status ready/not ready
handle_cast({ring_ready, true}, State) ->
    prometheus_gauge:set(antidote_cluster_ring_state, 1),
    {noreply, State};
handle_cast({ring_ready, false}, State) ->
    prometheus_gauge:set(antidote_cluster_ring_state, 0),
    {noreply, State};

%% riak core node state
%% see type member_status in riak_core
%% -type member_status() :: valid | joining | invalid | leaving | exiting | down.
handle_cast({node_state, Node, valid}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 0),
    {noreply, State};
handle_cast({node_state, Node, joining}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 1),
    {noreply, State};
handle_cast({node_state, Node, invalid}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 2),
    {noreply, State};
handle_cast({node_state, Node, leaving}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 3),
    {noreply, State};
handle_cast({node_state, Node, exiting}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 4),
    {noreply, State};
handle_cast({node_state, Node, down}, State) ->
    prometheus_gauge:set(antidote_ring_node_state, [Node], 5),
    {noreply, State};

%% riak core ring claimed
handle_cast({ring_claimed, Node, Percent}, State) ->
    prometheus_gauge:set(antidote_ring_claimed, [Node], Percent),
    {noreply, State};

%% riak core ring pending
handle_cast({ring_pending, Node, Percent}, State) ->
    prometheus_gauge:set(antidote_ring_pending, [Node], Percent),
    {noreply, State};

%% riak core ring member availability (-2,-1,1,2)
handle_cast({ring_member_availability, Node, Value}, State) ->
    prometheus_gauge:set(antidote_ring_member_availability, [Node], Value),
    {noreply, State};


handle_cast(StatRequest, State) ->
    logger:notice("Unknown stat update requested: ~p", [StatRequest]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

init_metrics() ->
    %% riak_core metrics
    prometheus_gauge:new([{name, antidote_cluster_ring_state}, {help, "If this node agrees on the cluster ring state of the cluster"}]),
    prometheus_gauge:new([{name, antidote_ring_node_state}, {help, "State of the node members in the riak_core cluster"}, {labels, [node]}]),
    prometheus_gauge:new([{name, antidote_ring_claimed}, {help, "How much of the ring this node has claimed"}, {labels, [node]}]),
    prometheus_gauge:new([{name, antidote_ring_pending}, {help, "How much of the ring this node will claim after resize"}, {labels, [node]}]),
    prometheus_gauge:new([{name, antidote_ring_member_availability}, {help, "Current availability of every member of the ring"}, {labels, [node]}]),
    prometheus_gauge:new([{name, antidote_process_scrape_time}, {help, "How long it took to scrape erlang process information"}]),

    prometheus_gauge:new([{name, antidote_process_reductions}, {help, "Reductions of processes"}, {labels, [process]}]),
    prometheus_gauge:new([{name, antidote_process_message_queue_length}, {help, "Message queue length of processes"}, {labels, [process]}]),

    prometheus_counter:new([{name, antidote_log_size}, {help, "The size of a single log file in bytes"}, {labels, [log]}]),
    prometheus_counter:new([{name, antidote_error_count}, {help, "The number of errors encountered during operation"}]),
    prometheus_counter:new([{name, antidote_warning_count}, {help, "The number of warnings encountered during operation"}]),
    prometheus_histogram:new([{name, antidote_staleness}, {help, "The staleness of the stable snapshot"}, {buckets, [1, 10, 100, 1000, 10000]}]),
    prometheus_gauge:new([{name, antidote_open_transactions}, {help, "Number of open transactions"}]),
    prometheus_counter:new([{name, antidote_transaction}, {help, "Number of transactions by type"}, {labels, [type]}]),
    prometheus_counter:new([{name, antidote_operations_total}, {help, "Number of operations executed"}, {labels, [type]}]),
    prometheus_counter:new([{name, antidote_operations_internal_total}, {help, "Number of operations executed internally"}, {labels, [type]}]),
    prometheus_counter:new([{name, antidote_dc_ops_received}, {help, "Number of operations received from other antidote datacenters"}]),
    prometheus_counter:new([{name, antidote_dc_ops_received_size}, {help, "Size of operations received from other antidote datacenters"}]),
    prometheus_counter:new([{name, antidote_log_access}, {help, "How many times the log was accessed"}, {labels, [type]}]),
    prometheus_gauge:new([{name, antidote_dc_count}, {help, "Number of known DCs including self"}]).
