-module(antidote_SUITE).
-author("Annette Bieniusa <bieniusa@cs.uni-kl.de>").

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([dummy_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    %% this might help, might not...
    os:cmd(os:find_executable("epmd")++" -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@"++Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _},_}} -> ok
    end,
    Config.

end_per_suite(Config) ->
    %application:stop(lager),
    Config.

init_per_testcase(Case, Config) ->
    %% have the slave nodes monitor the runner node, so they can't outlive it
    ct:pal("Hello"),
    %Nodes = test_utils:pmap(fun(N) ->
     %               test_utils:start_node(N, Config, Case)
     %       end, [testserver2]),

    Node = test_utils:start_node(testserver2, Config, Case),
    [{nodes, [Node]}|Config].

end_per_testcase(_, _) ->
    ok.

all() ->
    [
     dummy_test
    ].


dummy_test(Config) ->
  [Node1 | _Nodes] = proplists:get_value(nodes, Config),
  ct:print("Test on ~p!",[Node1]),
  %timer:sleep(10000),
  %application:set_env(antidote, txn_cert, true),
  %application:set_env(antidote, txn_prot, clocksi),

  {ok,_} = rpc:call(Node1, antidote, append, [myKey, riak_dt_gcounter, {increment, 4}]),
  ok.



