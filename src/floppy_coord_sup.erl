%% @doc Supervisor for the coordinator FSMs.
%%

-module(floppy_coord_sup).

-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

init([]) ->
    Worker = {floppy_coord_fsm,
              {floppy_coord_fsm, start_link, []},
              transient, 5000, worker, [floppy_coord_fsm]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.
