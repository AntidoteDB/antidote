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

-module(antidote_stats).

-include("antidote.hrl").

-behaviour(gen_server).
%% Interval to collect metrics
-define(INTERVAL, 10000). %% 10 sec
%% Interval to collect expensive metrics
-define(INTERVAL_LONG, 30000). %% 30 seconds
%% Metrics collection will be started after INIT_INTERVAL after application startup.
-define(INIT_INTERVAL, 10000). %% 10 seconds
%% What message queue length should log a warning
-define(QUEUE_LENGTH_THRESHOLD, 50).
%% If collection takes too long, turn it off and alert user
-define(TIME_METRIC_COLLECTION_THRESHOLD_MS, 5000).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    % start the timer for updating the calculated metrics
    TimerCheap = erlang:send_after(?INIT_INTERVAL, self(), periodic_update),

    % start the timer for updating the calculated expensive metrics
    TimerExpensive = erlang:send_after(?INIT_INTERVAL, self(), periodic_expensive_update),

    {ok, {TimerCheap, TimerExpensive}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(periodic_update, {CheapTimer, T}) ->
    %% ?
    _ = erlang:cancel_timer(CheapTimer),

    %% update all known stats
    _ = update_staleness(),

    update_dc_count(),

    %% schedule tick if continue
    Timer = erlang:send_after(?INTERVAL, self(), periodic_update),
    {noreply, {Timer, T}};

handle_info(periodic_expensive_update, {T, ExpensiveTimer}) ->
    %% ?
    _ = erlang:cancel_timer(ExpensiveTimer),

    %% update all known stats
    Continue = update_processes_info(),

    %% schedule tick if continue
    case Continue of
        true -> Timer = erlang:send_after(?INTERVAL_LONG, self(), periodic_expensive_update);
        _ -> Timer = undefined
    end,
    {noreply, {T, Timer}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ==
%% Internal functions
%% ==

update_staleness() ->
    Val = calculate_staleness(),
    ?STATS({update_staleness, Val}).


calculate_staleness() ->
    {ok, SS} = dc_utilities:get_stable_snapshot(),
    CurrentClock = to_microsec(os:timestamp()),
    Staleness = vectorclock:fold(fun(_K, C, Max) ->
                                   max(CurrentClock - C, Max)
                           end, 0, SS),
    round(Staleness/(1000)). %% To millisecs

to_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.


update_dc_count() ->
    DCs = dc_meta_data_utilities:get_dc_ids(true),
    ?STATS({dc_count, length(DCs)}).

update_processes_info() ->
    TimeStart = erlang:system_time(millisecond),
    Processes = erlang:processes(),
    Infos = [erlang:process_info(P) || P <- Processes],
    ToSortList = [
        {proplists:get_value(message_queue_len,ProcessInfo),
            proplists:get_value(reductions, ProcessInfo),
            proplists:get_value(registered_name, ProcessInfo)} || ProcessInfo <- Infos, ProcessInfo /= undefined],

    SortedList = lists:sort(ToSortList),
    FilteredList = [{Messages, Reductions, Name} || {Messages = {message_queue_len, Length}, Reductions, Name} <- SortedList, Length > ?QUEUE_LENGTH_THRESHOLD],
    case length(FilteredList) > 0 of
        true -> logger:warning("Message queues for ~p processes", [length(FilteredList)]);
            _ -> ok
    end,

    lists:foreach(
        fun({Messages, Reductions, Name}) ->
            ?STATS({process_reductions, Name, Reductions}),
            ?STATS({process_message_queue_length, Name, Messages})
        end,
        FilteredList
    ),

    TimeMs = erlang:system_time(millisecond) - TimeStart,
    case TimeMs > ?TIME_METRIC_COLLECTION_THRESHOLD_MS of
        true -> logger:alert("System metric process collection took too long (~p ms over ~p ms threshold), turning process info collection off", [TimeMs, ?TIME_METRIC_COLLECTION_THRESHOLD_MS]), false;
            _ -> true
    end .
