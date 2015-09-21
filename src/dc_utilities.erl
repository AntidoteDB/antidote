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
-module(dc_utilities).
-include("antidote.hrl").

-export([
    get_my_dc_id/0,
    get_my_dc_nodes/0,
    call_vnode_sync/3,
    bcast_vnode_sync/2,
    partition_to_indexnode/1,
    call_vnode/3,
    get_all_partitions/0,
    bcast_vnode/2,
    get_my_partitions/0]).

get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

get_my_dc_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

-spec partition_to_indexnode(partition_id()) -> {partition_id(), any()}.
partition_to_indexnode(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Node = riak_core_ring:index_owner(Ring, Partition),
    {Partition, Node}.

-spec get_my_partitions() -> list(partition_id()).
get_my_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:my_indices(Ring).

-spec get_all_partitions() -> list(partition_id()).
get_all_partitions() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    CHash = riak_core_ring:chash(Ring),
    Nodes = chash:nodes(CHash),
    [I || {I, _} <- Nodes].

-spec call_vnode_sync(partition_id(), atom(), any()) -> any().
call_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_command(partition_to_indexnode(Partition), Request, VMaster).

-spec call_vnode(partition_id(), atom(), any()) -> ok.
call_vnode(Partition, VMaster, Request) ->
    riak_core_vnode_master:command(partition_to_indexnode(Partition), Request, VMaster).

-spec bcast_vnode_sync(atom(), any()) -> any().
bcast_vnode_sync(VMaster, Request) ->
    lists:map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_all_partitions()).

-spec bcast_vnode(atom(), any()) -> any().
bcast_vnode(VMaster, Request) ->
    lists:map(fun(P) -> {P, call_vnode(P, VMaster, Request)} end, get_all_partitions()).
