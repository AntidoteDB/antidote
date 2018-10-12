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
  % we use the rest_for_one restart strategy here,
  % because we want the listeners to restart, if the main ranch
  % process crashes, but not vice versa.
  % We tolerate only 1 error per 5 seconds, because most errors should
  % be handled at a lower level.
  SupFlags = #{strategy => rest_for_one, intensity => 1, period => 5},
  RanchSupSpec = #{
    id => ranch_sup,
    start => {ranch_sup, start_link, []},
    restart => permanent,
    shutdown => 1000,
    type => supervisor,
    modules => [ranch_sup]},
  {ok, {SupFlags, [
    RanchSupSpec,
    pb_listener()
  ]}}.

%%====================================================================
%% Internal functions
%%====================================================================

pb_listener() ->
  NumberOfAcceptors = application:get_env(ranch, pb_pool_size, 100),
  Port = application:get_env(ranch, pb_port, 8087),
  MaxConnections = application:get_env(ranch, pb_max_connections, 1024),
  RanchOptions = [{port, Port}, {max_connections, MaxConnections}],
  ranch:child_spec(antidote_pb_process, NumberOfAcceptors,
    ranch_tcp, RanchOptions,
    antidote_pb_protocol, []
  ).
