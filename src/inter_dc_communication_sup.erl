%%% The supervisor in charge of all the socket acceptors.
-module(inter_dc_communication_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Port) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Port]).

init([Port]) ->
    Listener = {inter_dc_communication_recvr,
                {inter_dc_communication_recvr, start_link, [Port]}, % pass the socket!
                permanent, 1000, worker, [inter_dc_communication_recvr]},

    SupWorkers = {inter_dc_communication_fsm_sup,
                {inter_dc_communication_fsm_sup, start_link, []},
                permanent, 1000, supervisor, [inter_dc_communication_fsm_sup]},
    {ok, {{one_for_one, 60, 3600}, [Listener, SupWorkers]}}.
