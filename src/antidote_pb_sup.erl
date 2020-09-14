% Antidote protocol buffer supervisor.
-module(antidote_pb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  SupFlags = #{strategy => rest_for_one, intensity => 1, period => 5},
  {ok, {SupFlags, [
    pb_listener()
  ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

pb_listener() ->
  NumberOfAcceptors = application:get_env(ranch, pb_pool_size, 100),
  MaxConnections = application:get_env(ranch, pb_max_connections, 1024),
  Port = application:get_env(ranch, pb_port, 8087),

  ListenerSpec = ranch:child_spec({?MODULE, antidote_pb_process},
    ranch_tcp, #{
      num_acceptors => NumberOfAcceptors,
      max_connections => MaxConnections,
      socket_opts => [{port, Port}]
    }, antidote_pb_protocol, []
  ),

  ListenerSpec.

