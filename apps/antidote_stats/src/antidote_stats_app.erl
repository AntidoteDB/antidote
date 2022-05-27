%%%-------------------------------------------------------------------
%% @doc antidote_stats public API
%% @end
%%%-------------------------------------------------------------------

-module(antidote_stats_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    antidote_stats_sup:start_link().

stop(_State) ->
    ok.
