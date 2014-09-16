%%% The supervisor in charge of all the socket acceptors.
-module(inter_dc_communication_sup).
-behaviour(supervisor).

-export([start_link/1, start_socket/0]).
-export([init/1]).

start_link(Port) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Port]).

init([Port]) ->
    {ok, ListenSocket} = gen_tcp:listen(
                           Port,
                           [{active,false}, binary,
                            {packet,2},{reuseaddr, true}
                           ]),
    spawn_link(fun empty_listeners/0),
    {ok, {{simple_one_for_one, 60, 3600},
         [{socket,
          {inter_dc_communication_recvr, start_link, [ListenSocket]}, % pass the socket!
          temporary, 1000, worker, [inter_dc_communication_recvr]}
         ]}}.

start_socket() ->
    supervisor:start_child(?MODULE, []).

%% Start with 20 listeners so that many multiple connections can
%% be started at once, without serialization.
empty_listeners() ->
    [start_socket() || _ <- lists:seq(1,20)],
    lager:info("Starting listeners"),
    ok.
