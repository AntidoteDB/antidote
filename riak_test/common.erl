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

-export([clean_cluster/1]).

clean_cluster(Nodes0)->
    Clean = rt_config:get(clean_cluster, true),
    case Clean of
        true ->
            rt:clean_cluster(Nodes0),
            [Nodes1] = rt:build_clusters([length(Nodes0)]),
            rt:wait_until_ring_converged(Nodes1),
            Nodes1;
        false ->
            Nodes0
    end.
