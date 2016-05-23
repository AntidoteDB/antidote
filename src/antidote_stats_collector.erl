-module(antidote_stats_collector).

-behaviour(gen_server).
-define(INTERVAL, 10000). %% 10 sec
-define(INIT_INTERVAL, 60000). %% 1 minute

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(StatsPid) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StatsPid], []).

init([StatsPid]) ->
    init_metrics(),
    Timer = erlang:send_after(?INIT_INTERVAL, self(), periodic_update),
    {ok, {Timer, StatsPid}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Metric}, State) ->
    Val = antidote_stat:get_value(Metric),
    exometer:update(Metric, Val),
    {noreply, State};

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(periodic_update, {OldTimer, Pid}) ->
    erlang:cancel_timer(OldTimer),
    update(Pid, [staleness]),
    Timer = erlang:send_after(?INTERVAL, self(), periodic_update),    
    {noreply, {Timer, Pid}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update(_Pid, Metric) ->
    Val = antidote_stats:get_value(Metric),
    exometer:update(Metric, Val).

init_metrics() ->   
    Metrics = antidote_stats:stats(),
    lists:foreach(fun(Metric) ->
                          exometer:new(Metric, histogram, [{time_span, timer:seconds(60)}])
                  end, Metrics).
    
