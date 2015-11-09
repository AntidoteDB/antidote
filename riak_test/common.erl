%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(common).

-export([clean_clusters/1,
         setup_dc_manager/3]).

%% It cleans the cluster and formes a new one with the same characteristic of the input one.
%% In case the clean_cluster parameter is set to false in the riak_test configuration file
%% the function simply returns the inputed set of clusters
%% Clusters: A list of clusters as defined by rt
clean_clusters(Clusters)->
    Clean = rt_config:get(clean_cluster, true),
    case Clean of
        true ->
            Sizes = lists:foldl(fun(Cluster, Acc) ->
                                    rt:clean_cluster(Cluster),
                                    Acc ++ [length(Cluster)]
                                end, [], Clusters),
            Clusters1 = rt:build_clusters(Sizes),
            lists:foreach(fun(Cluster) ->
                            rt:wait_until_ring_converged(Cluster)
                          end, Clusters1),
            Clusters1;
        false ->
            Clusters
    end.

%% It set ups the listeners and connect the different dcs automatically
%% Input:   Clusters:   List of clusters
%%          Ports:      Port number where setup the listeners. One per dc
%%          Clean:      If true, the setting is done, otherwise, it is ignore. This is useful
%%                      for the configurable clean_cluster parameter. 
setup_dc_manager(Clusters, Ports, Clean) ->
    case Clean of
        true ->
            {_, CombinedList} = lists:foldl(fun(Cluster, {Index, Result}) ->
                                                {Index+1, Result ++ [{Cluster, lists:nth(Index, Ports)}]}
                                            end, {1, []}, Clusters),
            Heads = lists:foldl(fun({Cluster, Port}, Acc) ->
                                    Node = hd(Cluster),
                                    rt:wait_until_registered(Node, inter_dc_manager),
                                    {ok, DC} = rpc:call(Node, inter_dc_manager, start_receiver,[Port]),
                                    rt:wait_until(Node,fun wait_init:check_ready/1),
                                    Acc ++ [{Node, DC}]
                                end, [], CombinedList),
            lists:foreach(fun({Node, DC}) ->
                            Sublist = lists:subtract(Heads, [{Node, DC}]),
                            lists:foreach(fun({_, RemoteDC}) ->
                                            ok = rpc:call(Node, inter_dc_manager, add_dc,[RemoteDC])
                                          end, Sublist)
                          end, Heads),
            ok;
        false ->
            ok
    end.
