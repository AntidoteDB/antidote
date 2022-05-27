%%%-------------------------------------------------------------------
%% @doc antidote_stats top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(antidote_stats_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    Config = [{mods, [{elli_prometheus, []}]}],
    {ok, MetricsPort} = application:get_env(antidote_stats, metrics_port),
    ElliOpts = [{callback, elli_middleware}, {callback_args, Config}, {port, MetricsPort}],
    Elli = {elli_server, {elli, start_link, [ElliOpts]}, permanent, 5000, worker, [elli]},

    StatsCollector = {
        antidote_stats_collector,
        {antidote_stats_collector, start_link, []},
        permanent,
        5000,
        worker,
        [antidote_stats_collector]
    },

    ChildSpecs = [Elli, StatsCollector],
    {ok, {SupFlags, ChildSpecs}}.
