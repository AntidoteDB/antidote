%% @doc Supervise the fsm.
-module(floppy_coord_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    io:format('Creatting a new worker/child~n'),
    supervisor:start_child(?MODULE, Args).

init([]) ->
    Worker = {floppy_coord_fms,
                {floppy_coord_fsm, start_link, []},
                transient, 5000, worker, [floppy_coord_fms]},
    {ok, {{simple_one_for_one, 10, 10}, [Worker]}}.
