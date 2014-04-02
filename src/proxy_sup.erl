%% @doc Supervise the proxy gen_server.
-module(proxy_sup).
-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Proxy = {proxy,
                {proxy, start_link, []},
                permanent, 5000, worker, [proxy]},
    catch  {ok, {{one_for_one, 5, 10}, [Proxy]}}.
