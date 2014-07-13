%% @doc Supervise the fsm.
-module(floppy_rep_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

init([]) ->
    Worker = {floppy_rep_fms,
              {floppy_rep_fsm, start_link, []},
              transient, 5000, worker, [floppy_rep_fms]},
    {ok, {{simple_one_for_one, 10, 10}, [Worker]}}.
