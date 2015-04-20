-module(ec_tx_coord_super_sup).
-export([start_link/1, init/1]).
-behaviour(supervisor).

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, []).

init(_Args) ->
    MaxRestart = 5,
    MaxTime = 10,
    {ok, {{simple_one_for_one, MaxRestart, MaxTime},
          [{ec_static_tx_coord_sup,
            {ec_static_tx_coord_sup, start_link, []},
            permanent, 5000, supervisor, [ec_static_tx_coord_sup]}]}}.
