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
-define(INTERVAL_LONG, 60000). %% 60 seconds
%% Metrics collection will be started after INIT_INTERVAL after application startup.
-define(INIT_INTERVAL, 10000). %% 10 seconds
%% What message queue length should log a warning
-define(QUEUE_LENGTH_THRESHOLD, 10).
%% If process collection takes too long, turn it off and alert user
-define(TIME_METRIC_COLLECTION_THRESHOLD_MS, 40000).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    timer :: any(),
    timer_expensive :: any(),
    monitored_processes :: list()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    % start the timer for updating the calculated metrics
    TimerCheap = erlang:send_after(?INIT_INTERVAL, self(), periodic_update),

    % start the timer for updating the calculated expensive metrics
    TimerExpensive = erlang:send_after(?INIT_INTERVAL, self(), periodic_expensive_update),

    {ok, #state{
        timer = TimerCheap,
        timer_expensive = TimerExpensive,
        % start only monitoring inter_dc_query and processes without registered names (undefined)
        monitored_processes = [inter_dc_query, undefined]
    }}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(periodic_update, State = #state{timer = CheapTimer}) ->
    %% ?
    _ = erlang:cancel_timer(CheapTimer),

    %% update all known stats
    _ = update_staleness(),

    update_dc_count(),

    %% update ring state
    antidote_ring_event_handler:update_status(),

    %% schedule tick if continue
    Timer = erlang:send_after(?INTERVAL, self(), periodic_update),
    {noreply, State#state{timer = Timer}};

handle_info(periodic_expensive_update, State = #state{timer_expensive =  ExpensiveTimer, monitored_processes = Monitored}) ->
    %% ?
    _ = erlang:cancel_timer(ExpensiveTimer),

    %% only collect extended stats if enabled
    case application:get_env(antidote, extended_stats) of
        {ok, true} ->
            %% update process infos
            {Continue, NewMonitored} = update_processes_info(Monitored),

            %% schedule tick if continue
            case Continue of
                true -> Timer = erlang:send_after(?INTERVAL_LONG, self(), periodic_expensive_update);
                _ -> Timer = undefined
            end,
            {noreply, State#state{timer_expensive = Timer, monitored_processes = NewMonitored}};

        _ ->
            {noreply, State#state{timer_expensive = undefined}}
    end.


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
    DCs = dc_meta_data_utilities:get_dc_descriptors(),
    ?STATS({dc_count, length(DCs)}).


update_processes_info(Monitored) ->
    TimeStart = erlang:system_time(millisecond),

    %% get all processes
    Processes = erlang:processes(),

    %% collect info of each process
    Infos = [erlang:process_info(P) || P <- Processes],


    %% get only name, queue, and reductions
    KeyValueList = [
        {
            proplists:get_value(registered_name, ProcessInfo),
            proplists:get_value(message_queue_len, ProcessInfo),
            proplists:get_value(reductions, ProcessInfo)
        } || ProcessInfo <- Infos, ProcessInfo /= undefined],


    %% fold over list, group by name
    {QueueMap, ReductionsMap} = lists:foldl(
        fun({Name, Messages, Reductions}, {QMap, RMap}) ->
            {
                maps:put(Name, maps:get(Name, QMap, 0) + Messages, QMap),
                maps:put(Name, maps:get(Name, RMap, 0) + Reductions, RMap)
            }
        end,
        {maps:new(), maps:new()},
        KeyValueList
    ),

    %% for each process, update queue length and reductions only if monitored
    NewMonitored = maps:fold(fun(Name, Messages, MonitorAcc) ->
        case lists:any(fun(E) -> E == Name end, MonitorAcc) of
            true ->
                ?STATS({process_message_queue_length, Name, Messages}),
                ?STATS({process_reductions, Name, maps:get(Name, ReductionsMap)}),
                MonitorAcc;
            _ ->
                %% if a process is not monitored and the threshold is reached, add to monitored list
                case Messages > ?QUEUE_LENGTH_THRESHOLD of
                    true ->
                        logger:warning("New process has a message queue and is now being monitored ~p: ~p", [Name, Messages]),
                        ?STATS({process_message_queue_length, Name, Messages}),
                        ?STATS({process_reductions, Name, maps:get(Name, ReductionsMap)}),
                        MonitorAcc ++ [Name];
                    false ->
                        MonitorAcc
                end
        end

              end,
        Monitored,
        QueueMap
    ),


    %% measure time to scrape and report
    TimeMs = erlang:system_time(millisecond) - TimeStart,
    ?STATS({process_scrape_time, TimeMs}),
    case TimeMs > ?TIME_METRIC_COLLECTION_THRESHOLD_MS of
        true ->
            logger:alert("System metric process collection took too long (~p ms over ~p ms threshold), turning process info collection off", [TimeMs, ?TIME_METRIC_COLLECTION_THRESHOLD_MS]),
            {false, NewMonitored};
        _ ->
            logger:debug("Took ~p ms to scrape processes", [TimeMs]),
            {true, NewMonitored}
    end .
