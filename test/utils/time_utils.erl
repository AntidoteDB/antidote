%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(time_utils).

-include_lib("eunit/include/eunit.hrl").

%% API
-export([
    wait_until/1,
    wait_until/2,
    wait_until/3,
    wait_until_result/4,
    wait_until_registered/2,
    wait_until_offline/1,
    wait_until_connection/3,
    wait_until_disconnected/2,
    wait_until_connected/2,
    wait_until_nodes_agree_about_ownership/1,
    wait_until_owners_according_to/2,

    retry_delay/0,
    rt_retry_delay/0,
    retries/0
]).


%% @doc Convenience wrapper for wait_until for the myriad functions that
%% take a node as single argument.
-spec wait_until(node(), fun()) -> true | {fail, any()}.
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).


%% @doc Utility function used to construct test predicates. Retries the
%%      function 'Fun' until it returns 'true', or until the maximum
%%      number of retries is reached. Returns true or the failure reason.
-spec wait_until(fun()) -> true | {fail, any()}.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000, %% @TODO use config
    Delay = 1000, %% @TODO use config
    Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).


%% @doc Retries function 'Fun' until 'true' is returned or retries are reached with a specified
%% function execution delay in ms.
-spec wait_until(fun(), integer(), integer()) -> true | {fail, any()}.
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    wait_until_result(Fun, true, Retry, Delay).


%% @doc Retries function 'Fun' until the expected result is returned or retries are reached with a specified
%% function execution delay in ms.
-spec wait_until_result(fun(), any(), integer(), integer()) -> ok | {fail, any()}.
wait_until_result(Fun, ExpectedResult, Retry, Delay) when Retry > 0 ->
    ActualResult = Fun(),
    case ActualResult of
        ExpectedResult ->
            ok;
        _ when Retry == 1 ->
            {fail, ActualResult};
        _ ->
            timer:sleep(Delay),
            wait_until_result(Fun, ExpectedResult, Retry-1, Delay)
    end.


%% @doc Waits until no connection to the target node can be established anymore.
-spec wait_until_offline(node()) -> any().
wait_until_offline(Node) ->
    wait_until(fun() ->
        pang == net_adm:ping(Node)
               end, retries(), retry_delay()).


%% @doc Waits until node1 can or connect to node2
-spec wait_until_connection(node(), node(), pang | pong) -> any().
wait_until_connection(Node1, Node2, Expected) ->
    wait_until(fun() ->
        Expected == rpc:call(Node1, net_adm, ping, [Node2])
               end, retries(), retry_delay()).


%% @doc Waits until node1 cannot connect to node2 anymore
-spec wait_until_disconnected(node(), node()) -> any().
wait_until_disconnected(Node1, Node2) ->
    wait_until_connection(Node1, Node2, pang).


%% @doc Waits until node1 can connect to node2
-spec wait_until_connected(node(), node()) -> any().
wait_until_connected(Node1, Node2) ->
    wait_until_connection(Node1, Node2, pong).


%% @doc Waits until a certain registered name pops up on the remote node.
-spec wait_until_registered(node(), any()) -> any().
wait_until_registered(Node, Name) ->
    IsNameRegisteredMember = fun() ->
                Registered = rpc:call(Node, erlang, registered, []),
                lists:member(Name, Registered)
        end,
    Delay = rt_retry_delay(),
    Retry = 360000 div Delay,
    wait_until(IsNameRegisteredMember, Retry, Delay).


%% @doc Waits until nodes agree about ownership
-spec wait_until_nodes_agree_about_ownership([node()]) -> any().
wait_until_nodes_agree_about_ownership(Nodes) ->
    Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    ?assert(lists:all(fun(X) -> ok =:= X end, Results)).


%% @doc Waits until ring owners of the given node match the expected ring owners
-spec wait_until_owners_according_to(node(), [node()]) -> ok.
wait_until_owners_according_to(Node, NodeRingOwners) ->
    ExpectedOwners = lists:usort(NodeRingOwners),
    AreNodesOwners = fun(N) ->
        riak_utils:owners_according_to(N) =:= ExpectedOwners
                     end,
    ?assertEqual(ok, wait_until(Node, AreNodesOwners)),
    ok.


%TODO Move to config
rt_retry_delay() -> 500.

retry_delay() -> 1000.

retries() -> 60*2.

%% UNUSED FUNCTIONS

%wait_until_left(Nodes, LeavingNode) ->
%    wait_until(fun() ->
%                lists:all(fun(X) -> X == true end,
%                          pmap(fun(Node) ->
%                                not
%                                lists:member(LeavingNode,
%                                             get_cluster_members(Node))
%                        end, Nodes))
%        end, 60*2, 500).

%wait_until_joined(Nodes, ExpectedCluster) ->
%    wait_until(fun() ->
%                lists:all(fun(X) -> X == true end,
%                          pmap(fun(Node) ->
%                                lists:sort(ExpectedCluster) ==
%                                lists:sort(get_cluster_members(Node))
%                        end, Nodes))
%        end, 60*2, 500).
