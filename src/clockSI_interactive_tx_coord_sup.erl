%% @doc Supervise the fsm.
-module(clockSI_interactive_tx_coord_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

%% @doc Starts the coordinator of a ClockSI interactive transaction. 
init([]) ->
	lager:info("clockSI_interactive_tx_coord_sup: Starting fsm..."),
    Worker = {clockSI_interactive_tx_coord_fsm,
                {clockSI_interactive_tx_coord_fsm, start_link, []},
                transient, 5000, worker, [clockSI_interactive_tx_coord_fsm]},
    lager:info("clockSI_interactive_tx_coord_sup: done."),
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.