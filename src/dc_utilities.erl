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

-export([get_my_dc_id/0, get_my_dc_nodes/0, call_vnode/3, bcast_vnode/3]).

get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

get_my_dc_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

%% TODO: implement safely
call_vnode(Partition, VMaster, Request) -> riak_core_vnode_master:sync_command({Partition, node()}, Request, VMaster).

%% TODO: this is a quick 'n dirty solution - must be properly implemented
bcast_vnode(VMaster, VMod, Request) ->
    VNodes = riak_core_vnode_manager:all_index_pid(VMod),
    case length(VNodes) of
        64 ->
            Partitions = lists:map(fun({P, _}) -> P end, VNodes),
            F = fun(P) -> call_vnode(P, VMaster, Request) end,
            lists:foreach(F, Partitions);
        _ ->
            lager:info("Waiting for the VNodes to start..."),
            timer:sleep(500),
            bcast_vnode(VMaster, VMod, Request)
    end.

