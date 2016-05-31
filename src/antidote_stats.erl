-module(antidote_stats).

-behaviour(gen_server).

%% API
-export([start_link/0,  get_value/1, stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% List of configured metrics
stats() ->
    [[staleness]].

get_value(Metric) ->    
    gen_server:call(?MODULE, {get_value, Metric}).

init([]) ->
        {ok, ok}.

handle_call({get_value, Metric}, _From, State) ->
    {reply, calculate(Metric), State};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

calculate([staleness]) ->
    {ok, SS} = vectorclock:get_stable_snapshot(),
    CurrentClock = to_microsec(os:timestamp()),
    Staleness = dict:fold(fun(_K, C, Max) ->
                                   max(CurrentClock - C, Max)
                           end, 0, SS),
    Staleness/(1000); %% To millisecs

calculate(_) ->
    {error, metric_not_found}.

to_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.
    
