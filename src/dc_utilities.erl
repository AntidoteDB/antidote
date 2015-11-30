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
    get_my_partitions/0,
    ensure_all_vnodes_running/1,
    ensure_all_vnodes_running_master/1,
    get_partitions_num/0,
    check_staleness/0]).

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

-spec get_partitions_num() -> non_neg_integer().
get_partitions_num() -> length(get_all_partitions()).

-spec call_vnode_sync(partition_id(), atom(), any()) -> any().
call_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_command(partition_to_indexnode(Partition), Request, VMaster).

-spec call_vnode(partition_id(), atom(), any()) -> ok.
call_vnode(Partition, VMaster, Request) ->
    riak_core_vnode_master:command(partition_to_indexnode(Partition), Request, VMaster).

-spec bcast_vnode_sync(atom(), any()) -> any().
bcast_vnode_sync(VMaster, Request) ->
    %% TODO: a parallel map function would be nice here
    lists:map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_all_partitions()).

-spec bcast_vnode(atom(), any()) -> any().
bcast_vnode(VMaster, Request) ->
    lists:map(fun(P) -> {P, call_vnode(P, VMaster, Request)} end, get_all_partitions()).

ensure_all_vnodes_running(VnodeType) ->
    Partitions = get_partitions_num(),
    Running = length(riak_core_vnode_manager:all_vnodes(VnodeType)),
    case Partitions == Running of
        true -> ok;
        false ->
            lager:info("Waiting for vnode ~p: required ~p, spawned ~p", [VnodeType, Partitions, Running]),
            timer:sleep(250),
            ensure_all_vnodes_running(VnodeType)
    end.

bcast_vnode_check_up(_VMaster,_Request,[]) ->
    ok;
bcast_vnode_check_up(VMaster,Request,[P|Rest]) ->
    Err = try
	      case call_vnode_sync(P,VMaster,Request) of
		  ok ->
		      false;
		  Msg ->
		      true
	      end
	  catch
	      _Ex:_Res ->
		  true
	  end,
    case Err of
	true ->
	    lager:info("Vnode not up retrying, ~p, ~p", [VMaster,P]),
	    timer:sleep(1000),
	    bcast_vnode_check_up(VMaster,Request,[P|Rest]);
	false ->
	    bcast_vnode_check_up(VMaster,Request,Rest)
    end.
    

ensure_all_vnodes_running_master(VnodeType) ->
    check_registered(VnodeType),
    bcast_vnode_check_up(VnodeType,{hello}, get_all_partitions()).

-spec check_staleness() -> ok.
check_staleness() ->
    Now = clocksi_vnode:now_microsec(erlang:now()),
    {ok, SS} = vectorclock:get_stable_snapshot(),
    dict:fold(fun(DcId,Time,_Acc) ->
		      io:format("~w staleness: ~w ms ~n", [DcId,(Now-Time)/1000]),
		      ok
	      end, ok, SS).

check_registered(Name) ->
    case whereis(Name) of
	undefined ->
	    timer:sleep(100),
	    check_registered(Name);
	_ ->
	    ok
    end.
