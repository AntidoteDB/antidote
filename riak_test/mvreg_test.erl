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
-module(mvreg_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    N = 3,
    ListIds = [random:uniform(N) || _ <- lists:seq(1, 10)],
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    lists:foreach(fun(Node) ->
			  rt:wait_until(Node, fun wait_init:check_ready/1)
		  end, Nodes),
    lager:info("Vnodes are started up"),


    F = fun(Elem) ->
            Node = lists:nth(Elem, Nodes),
            lager:info("Sending asign to Node ~w~n",[Node]),
            AssignResult = rpc:call(Node, antidote, append, [abc, riak_dt_mvreg, {{assign, Elem}, actor1}]),
            ?assertMatch({ok, _}, AssignResult)
    end,

    lists:map(F, ListIds),
    FirstNode = hd(Nodes),
    Result = hd(lists:reverse(ListIds)),
    lager:info("Sending read to Node ~w~n",[FirstNode]),
    {ok, ReadResult} = rpc:call(FirstNode, antidote, read, [abc, riak_dt_mvreg]),
    ?assertEqual([Result], ReadResult),

    PropagateResult1 = rpc:call(FirstNode, antidote, append, [abc, riak_dt_mvreg, {{propagate, value2, [{actor2, 5}]}, actor1}]),
    ?assertMatch({ok, _}, PropagateResult1),
    {ok, ReadResult1} = rpc:call(FirstNode, antidote, read, [abc, riak_dt_mvreg]),
    Result1 = [Result, value2],
    ?assertEqual(lists:sort(Result1), lists:sort(ReadResult1)),

    PropagateResult2 = rpc:call(FirstNode, antidote, append, [abc, riak_dt_mvreg, {{propagate, value3, [{actor2, 6}, {actor1, 11}]}, actor1}]),
    ?assertMatch({ok, _}, PropagateResult2),
    {ok, ReadResult2} = rpc:call(FirstNode, antidote, read, [abc, riak_dt_mvreg]),
    ?assertEqual([value3], ReadResult2),

    pass.
