-module(ec_tx_coord_super_sup).
-export([start_link/0, init/0]).
-behaviour(supervisor).

start_link() ->
    supervisor:start_link(?MODULE, []).

init() ->
    MaxRestart = 5,
    MaxTime = 100,
    {ok, {{simple_one_for_one, MaxRestart, MaxTime},
          [{ec_tx_coord_server,
            {ec_tx_coord_server, start_link, []},
            permanent, 5000, supervisor, [ec_tx_coord_server]}]}}.
