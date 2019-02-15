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

-module(dc_utilities).
-include("antidote.hrl").

-export([
  get_my_dc_id/0,
  get_my_dc_nodes/0,
  call_vnode_sync/3,
  bcast_vnode_sync/2,
  bcast_my_vnode_sync/2,
  partition_to_indexnode/1,
  call_vnode/3,
  call_local_vnode/3,
  call_local_vnode_sync/3,
  get_all_partitions/0,
  get_all_partitions_nodes/0,
  bcast_vnode/2,
  get_my_partitions/0,
  ensure_all_vnodes_running/1,
  ensure_local_vnodes_running_master/1,
  ensure_all_vnodes_running_master/1,
  get_partitions_num/0,
  check_staleness/0,
  check_registered/1,
  get_scalar_stable_time/0,
  get_partition_snapshot/1,
  get_stable_snapshot/0,
  check_registered_global/1,
  now_microsec/0,
  now_millisec/0]).

%% Returns the ID of the current DC.
%% This should not be called manually (it is only used the very
%% first time the DC is started), instead if you need to know
%% the id of the DC use the following:
%% dc_meta_data_utilites:get_my_dc_id
%% The reason is that the dcid can change on fail and restart, but
%% the original name is stored on disk in the meta_data_utilities
-spec get_my_dc_id() -> dcid().
get_my_dc_id() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:cluster_name(Ring).

%% Returns the list of all node addresses in the cluster.
-spec get_my_dc_nodes() -> [node()].
get_my_dc_nodes() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

%% Returns the IndexNode tuple used by riak_core_vnode_master:command functions.
-spec partition_to_indexnode(partition_id()) -> {partition_id(), any()}.
partition_to_indexnode(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Node = riak_core_ring:index_owner(Ring, Partition),
    {Partition, Node}.

%% Returns a list of all partition indices in the cluster.
%% The partitions indices are 160-bit numbers that equally division the keyspace.
%% For example, for a cluster with 8 partitions, the indices would take following values:
%% 0, 1 * 2^157, 2 * 2^157, 3 * 2^157, 4 * 2^157, 5 * 2^157, 6 * 2^157, 7 * 2^157.
%% The partition numbers are erlang integers. To obtain the binary representation of the index,
%% use the inter_dc_txn:partition_to_bin/1 function.
-spec get_all_partitions() -> [partition_id()].
get_all_partitions() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        Nodes = chash:nodes(CHash),
        [I || {I, _} <- Nodes]
    catch
        _Ex:Res ->
            logger:debug("Error loading partition names: ~p, will retry", [Res]),
            get_all_partitions()
    end.

%% Returns a list of all partition indcies plus the node each
%% belongs to
-spec get_all_partitions_nodes() -> [{partition_id(), node()}].
get_all_partitions_nodes() ->
    try
        {ok, Ring} = riak_core_ring_manager:get_my_ring(),
        CHash = riak_core_ring:chash(Ring),
        chash:nodes(CHash)
    catch
        _Ex:Res ->
            logger:debug("Error loading partition-node names ~p, will retry", [Res]),
            get_all_partitions_nodes()
    end.

%% Returns the partition indices hosted by the local (caller) node.
-spec get_my_partitions() -> [partition_id()].
get_my_partitions() ->
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  riak_core_ring:my_indices(Ring).

%% Returns the number of partitions.
-spec get_partitions_num() -> non_neg_integer().
get_partitions_num() -> length(get_all_partitions()).

%% Sends the synchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode_sync(partition_id(), atom(), any()) -> any().
call_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number.
-spec call_vnode(partition_id(), atom(), any()) -> ok.
call_vnode(Partition, VMaster, Request) ->
    riak_core_vnode_master:command(partition_to_indexnode(Partition), Request, VMaster).

%% Sends the asynchronous command to a vnode of a specified type and responsible for a specified partition number,
%% the partition must be on the same node that the command is run on
-spec call_local_vnode(partition_id(), atom(), any()) -> ok.
call_local_vnode(Partition, VMaster, Request) ->
    riak_core_vnode_master:command({Partition, node()}, Request, VMaster).

-spec call_local_vnode_sync(partition_id(), atom(), any()) -> any().
call_local_vnode_sync(Partition, VMaster, Request) ->
    riak_core_vnode_master:sync_command({Partition, node()}, Request, VMaster).

%% Sends the same (synchronous) command to all vnodes of a given type.
-spec bcast_vnode_sync(atom(), any()) -> any().
bcast_vnode_sync(VMaster, Request) ->
    %% TODO: a parallel map function would be nice here
    lists:map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_all_partitions()).

%% Broadcasts a message to all vnodes of the given type
%% located on the physical node from which this method is called
-spec bcast_my_vnode_sync(atom(), any()) -> any().
bcast_my_vnode_sync(VMaster, Request) ->
    %% TODO: a parallel map function would be nice here
    lists:map(fun(P) -> {P, call_vnode_sync(P, VMaster, Request)} end, get_my_partitions()).

%% Sends the same (asynchronous) command to all vnodes of a given type.
-spec bcast_vnode(atom(), any()) -> any().
bcast_vnode(VMaster, Request) ->
    lists:map(fun(P) -> {P, call_vnode(P, VMaster, Request)} end, get_all_partitions()).

%% Checks if all vnodes of a particular type are running.
%% The method uses riak_core methods to perform the check and was
%% shown to be unreliable in some very specific circumstances.
%% Use with caution.
-spec ensure_all_vnodes_running(atom()) -> ok.
ensure_all_vnodes_running(VnodeType) ->
    Partitions = get_partitions_num(),
    Running = length(riak_core_vnode_manager:all_vnodes(VnodeType)),
    case Partitions == Running of
        true -> ok;
        false ->
            logger:debug("Waiting for vnode ~p: required ~p, spawned ~p", [VnodeType, Partitions, Running]),
            timer:sleep(250),
            ensure_all_vnodes_running(VnodeType)
    end.

%% Internal function that loops until a given vnode type is running
-spec bcast_vnode_check_up(atom(), {hello}, [partition_id()]) -> ok.
bcast_vnode_check_up(_VMaster, _Request, []) ->
    ok;
bcast_vnode_check_up(VMaster, Request, [P|Rest]) ->
    Err = try
              case call_vnode_sync(P, VMaster, Request) of
                  ok ->
                      false;
                  _Msg ->
                      true
              end
          catch
              _Ex:_Res ->
                  true
          end,
    case Err of
        true ->
            logger:debug("Vnode not up retrying, ~p, ~p", [VMaster, P]),
            timer:sleep(1000),
            bcast_vnode_check_up(VMaster, Request, [P|Rest]);
        false ->
            bcast_vnode_check_up(VMaster, Request, Rest)
    end.

%% Loops until all vnodes of a given type are running
%% on the local phyical node from which this was funciton called
-spec ensure_local_vnodes_running_master(atom()) -> ok.
ensure_local_vnodes_running_master(VnodeType) ->
    check_registered(VnodeType),
    bcast_vnode_check_up(VnodeType, {hello}, get_my_partitions()).

%% Loops until all vnodes of a given type are running on all
%% nodes in the cluster
-spec ensure_all_vnodes_running_master(atom()) -> ok.
ensure_all_vnodes_running_master(VnodeType) ->
    check_registered(VnodeType),
    bcast_vnode_check_up(VnodeType, {hello}, get_all_partitions()).

%% Prints to the console the staleness between this DC and all
%% other DCs that it is connected to
-spec check_staleness() -> ok.
check_staleness() ->
    Now = dc_utilities:now_microsec(),
    {ok, SS} = get_stable_snapshot(),
    dict:fold(fun(DcId, Time, _Acc) ->
                  io:format("~w staleness: ~w ms ~n", [DcId, (Now-Time)/1000]),
                  ok
              end, ok, SS).

%% Loops until a process with the given name is registered locally
-spec check_registered(atom()) -> ok.
check_registered(Name) ->
    case whereis(Name) of
        undefined ->
            logger:debug("Wait for ~p to register", [Name]),
            timer:sleep(100),
            check_registered(Name);
        _ ->
            ok
    end.

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
-spec get_stable_snapshot() -> {ok, snapshot_time()}.
get_stable_snapshot() ->
    case meta_data_sender:get_merged_data(stable) of
        undefined ->
            %% The snapshot isn't realy yet, need to wait for startup
            timer:sleep(10),
            get_stable_snapshot();
        SS ->
            case application:get_env(antidote, txn_prot) of
                {ok, clocksi} ->
                    %% This is fine if transactions coordinators exists on the ring (i.e. they have access
                    %% to riak core meta-data) otherwise will have to change this
                    {ok, SS};
                {ok, gr} ->
                    %% For gentlerain use the same format as clocksi
                    %% But, replicate GST to all entries in the dict
                    StableSnapshot = SS,
                    case dict:size(StableSnapshot) of
                        0 ->
                            {ok, StableSnapshot};
                        _ ->
                            ListTime = dict:fold(
                                         fun(_Key, Value, Acc) ->
                                                 [Value | Acc ]
                                         end, [], StableSnapshot),
                            GST = lists:min(ListTime),
                            {ok, dict:map(
                                   fun(_K, _V) ->
                                           GST
                                   end,
                                   StableSnapshot)}
                    end
            end
    end.

-spec get_partition_snapshot(partition_id()) -> snapshot_time().
get_partition_snapshot(Partition) ->
    case meta_data_sender:get_meta_dict(stable, Partition) of
        undefined ->
            %% The partition isn't ready yet, wait for startup
            timer:sleep(10),
            get_partition_snapshot(Partition);
        SS ->
            SS
    end.

%% Returns the minimum value in the stable vector snapshot time
%% Useful for gentlerain protocol.
-spec get_scalar_stable_time() -> {ok, non_neg_integer(), vectorclock()}.
get_scalar_stable_time() ->
    {ok, StableSnapshot} = get_stable_snapshot(),
    %% dict:is_empty/1 is not available, hence using dict:size/1
    %% to check whether it is empty
    case dict:size(StableSnapshot) of
        0 ->
            %% This case occur when updates from remote replicas has not yet received
            %% or when there are no remote replicas
            %% Since with current setup there is no mechanism
            %% to distinguish these, we assume the second case
            Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
            {ok, Now, StableSnapshot};
        _ ->
            %% This is correct only if stablesnapshot has entries for
            %% all DCs. Inorder to check that we need to configure the
            %% number of DCs in advance, which is not possible now.
            ListTime = dict:fold(
                         fun(_Key, Value, Acc) ->
                                 [Value | Acc ]
                         end, [], StableSnapshot),
            GST = lists:min(ListTime),
            {ok, GST, StableSnapshot}
    end.

%% Loops until a process with the given name is registered globally
-spec check_registered_global(atom()) -> ok.
check_registered_global(Name) ->
    case global:whereis_name(Name) of
        undefined ->
            timer:sleep(100),
            check_registered_global(Name);
        _ ->
            ok
    end.



-spec now_microsec() -> non_neg_integer().
now_microsec() ->
  erlang:system_time(micro_seconds). % TODO 19 this is not correct, since it is not monotonic (Question: must it be unique as well?)

-spec now_millisec() -> non_neg_integer().
now_millisec() ->
  now_microsec() div 1000.
