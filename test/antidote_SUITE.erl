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
    test_utils:at_init_testsuite(),
    Config.

end_per_suite(Config) ->
    %application:stop(lager),
    Config.

init_per_testcase(Case, Config) ->
    %lager_common_test_backend:bounce(debug),
    %% have the slave nodes monitor the runner node, so they can't outlive it
    ct:pal("Hello"),
    Nodes = test_utils:pmap(fun(N) ->
                    test_utils:start_node(N, Config, Case)
            end, [dev1, dev2]),

    test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].

end_per_testcase(_, _) ->
    ok.

all() ->
    [
     dummy_test
    ].


dummy_test(Config) ->
  [Node1, Node2 | _Nodes] = proplists:get_value(nodes, Config),
  ct:print("Test on ~p!",[Node1]),
  %timer:sleep(10000),
  %application:set_env(antidote, txn_cert, true),
  %application:set_env(antidote, txn_prot, clocksi),

  {ok,_} = rpc:call(Node1, antidote, append, [myKey2, crdt_pncounter, {increment, a}]),
  {ok,_} = rpc:call(Node1, antidote, append, [myKey2, crdt_pncounter, {increment, a}]),
  {ok,_} = rpc:call(Node2, antidote, append, [myKey2, crdt_pncounter, {increment, a}]),

  % Propagation of updates
  F = fun() -> 
    rpc:call(Node2, antidote, read, [myKey2, crdt_pncounter])
    end,
  Delay = 100,
  Retry = 360000 div Delay, %wait for max 1 min
  ok = test_utils:wait_until_result(F, {ok, 3}, Retry, Delay),

  ok.



