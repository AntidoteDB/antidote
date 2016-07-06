%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(mvreg_SUITE).
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
-export([mvreg_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Config.


end_per_suite(Config) ->
    Config.

init_per_testcase(Case, Config) ->
    Nodes = test_utils:pmap(fun(N) ->
                    test_utils:start_node(N, Config, Case)
            end, [dev1]),

    test_utils:connect_dcs(Nodes),
    [{nodes, Nodes}|Config].
    
end_per_testcase(_, _) ->
    ok.

all() -> [mvreg_test].

mvreg_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    Node = hd(Nodes),
    
    %Test 1: Assigning different values by the same actor; 
    % last value should be returned when reading
    N = 10,
    ListIds = lists:seq(1, N),
    F = fun(Elem) ->
            lager:info("Assigning ~w to node ~w~n",[Elem,Node]),
            {ok, _} = rpc:call(Node, antidote, append, [key1, riak_dt_mvreg, {{assign, Elem}, actor1}])
        end,
    lists:map(F, ListIds),
    lager:info("Sending read to node ~w~n",[Node]),
    {ok, [N]} = rpc:call(Node, antidote, read, [key1, riak_dt_mvreg]),

    %Test 2: Propagate "concurrent" update by same actor; should return both values
    {ok, _} = rpc:call(Node, antidote, append, [key1, riak_dt_mvreg, {{propagate, value2, [{actor2, 5}]}, actor1}]),
    {ok, ReadResult1} = rpc:call(Node, antidote, read, [key1, riak_dt_mvreg]),
    Result1 = [N, value2],
    ?assertEqual(lists:sort(Result1), lists:sort(ReadResult1)),

    %Test 3: Propagate causally-dependent update by same actor; should return latest value
    {ok,_} = rpc:call(Node, antidote, append, [key1, riak_dt_mvreg, {{propagate, value3, [{actor2, 6}, {actor1, N+5}]}, actor1}]),
    {ok, [value3]} = rpc:call(Node, antidote, read, [key1, riak_dt_mvreg]),

    pass.