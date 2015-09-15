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
-module(log_handoff_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    NTestItems    = 10,
    NTestNodes    = 3,

    lager:info("Testing handoff (items ~p)", [NTestItems]),

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up..."),
    rt:setup_harness(dummy, dummy),

    lager:info("Spinning up test nodes"),
    Versions = [current || _ <- lists:seq(1, NTestNodes)],
    [RootNode | TestNodes] = rt:deploy_nodes(Versions,[logging]),
    rt:wait_for_service(RootNode, logging),


    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(RootNode,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Populating root node."),
    multiple_writes(RootNode, 1, NTestItems, ucl),

    %% Test handoff on each node:
    lager:info("Testing handoff for cluster."),
    lists:foreach(fun(TestNode) ->
                test_handoff(RootNode, TestNode, NTestItems)
        end, TestNodes),

    %% Prepare for the next call to our test (we aren't polite about it,
    %% it's faster that way):
    lager:info("Bringing down test nodes."),
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, TestNodes),

    %% The "root" node can't leave() since it's the only node left:
    lager:info("Stopping root node."),
    rt:brutal_kill(RootNode),

    pass.

%% See if we get the same data back from our new nodes as we put into the root node:
test_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, logging),

    lager:info("Joining new node with cluster."),
    rt:join(NewNode, RootNode),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, NewNode])),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),
    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(RootNode,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),



    %% See if we get the same data back from the joined node that we
    %% added to the root node.  Note: systest_read() returns
    %% /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff:"),
    Results = multiple_reads(NewNode, 1, NTestItems),
    lager:info("The read data looks like: ~p", [Results]),
    ?assertEqual(0, length(Results)),
    lager:info("Data looks ok."),

    pass.

multiple_writes(Node, Start, End, Actor)->
    F = fun(N, Acc) ->
            case rpc:call(Node, antidote, append, [N, riak_dt_gcounter, {{increment, N}, Actor}]) of
                {ok, _} ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).

multiple_reads(Node, Start, End) ->
    F = fun(N, Acc) ->
            case rpc:call(Node, antidote, read, [N, riak_dt_gcounter]) of
                {error, _} ->
                    [{N, error} | Acc];
                {ok, Value} ->
                    case Value =:= N of
                        true ->
                            Acc;
                        false ->
                            [{N, {wrong_val, Value}} | Acc]
                    end
            end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).
