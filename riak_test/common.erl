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

-export([
  clean_clusters/1,
  setup_dc_manager/2]).

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
%%          Clean:      If true, the setting is done, otherwise, it is ignore. This is useful
%%                      for the configurable clean_cluster parameter.
setup_dc_manager(Clusters, first_run) -> connect_dcs(Clusters);
setup_dc_manager(_Clusters, false) -> ok;
setup_dc_manager(Clusters, true) -> disconnect_dcs(Clusters), connect_dcs(Clusters).

connect_dcs(Clusters) ->
  lager:info("Connecting DC clusters..."),
  Descriptors = descriptors(Clusters),
  lists:foreach(fun(Cluster) ->
    Node = hd(Cluster),
    lager:info("Waiting until vnodes start on node ~p", [Node]),
    rt:wait_until_registered(Node, inter_dc_pub),
    rt:wait_until_registered(Node, inter_dc_log_reader_response),
    rt:wait_until_registered(Node, inter_dc_log_reader_query),
    rt:wait_until_registered(Node, inter_dc_sub),
    lager:info("Making node ~p observe other DCs...", [Node]),
    %% It is safe to make the DC observe itself, the observe() call will be ignored silently.
    rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
  end, Clusters),
  lager:info("DC clusters connected!").

disconnect_dcs(Clusters) ->
  lager:info("Disconnecting DC clusters..."),
  Descriptors = descriptors(Clusters),
  lists:foreach(fun(Cluster) ->
    Node = hd(Cluster),
    lager:info("Making node ~p forget other DCs...", [Node]),
    rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
  end, Clusters),
  lager:info("DC clusters disconnected!").

descriptors(Clusters) ->
  lists:map(fun(Cluster) ->
    {ok, Descriptor} = rpc:call(hd(Cluster), inter_dc_manager, get_descriptor, []),
    Descriptor
  end, Clusters).
