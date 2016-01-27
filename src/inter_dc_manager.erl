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
-module(inter_dc_manager).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

-export([
  get_descriptor/0,
  start_bg_processes/1,
  observe_dc/1,
  observe_dc_sync/1,
  observe/1,
  observe_dcs/1,
  observe_dcs_sync/1,
  add_network_delays/1,
  add_network_delay/1,
  forget_dc/1,
  forget_dcs/1]).

-spec get_descriptor() -> {ok, #descriptor{}}.
get_descriptor() ->
  %% Wait until all needed vnodes are spawned, so that the heartbeats are already being sent
  ok = dc_utilities:ensure_all_vnodes_running_master(inter_dc_log_sender_vnode_master),
  Nodes = dc_utilities:get_my_dc_nodes(),
  Publishers = lists:map(fun(Node) -> rpc:call(Node, inter_dc_pub, get_address, []) end, Nodes),
  LogReaders = lists:map(fun(Node) -> rpc:call(Node, inter_dc_log_reader_response, get_address, []) end, Nodes),
  {ok, #descriptor{
    dcid = dc_utilities:get_my_dc_id(),
    partition_num = dc_utilities:get_partitions_num(),
    publishers = Publishers,
    logreaders = LogReaders
  }}.


-spec start_bg_processes(list()) -> ok.
start_bg_processes(Name) ->
    %% Start the meta-data senders
    Nodes = dc_utilities:get_my_dc_nodes(),
    lists:foreach(fun(Node) -> ok = rpc:call(Node, meta_data_sender, start, [Name]) end, Nodes),
    %% Start the timers sending the heartbeats
    %% FIXME: Shouldn't the return value be matched??
    _ = dc_utilities:bcast_vnode_sync(inter_dc_log_sender_vnode_master, {start_timer}),
    ok.

-spec observe_dc(#descriptor{}) -> ok.
observe_dc(#descriptor{dcid = DCID, partition_num = PartitionsNumRemote, publishers = Publishers, logreaders = LogReaders}) ->
  PartitionsNumLocal = dc_utilities:get_partitions_num(),
  case PartitionsNumRemote == PartitionsNumLocal of
    false ->
      lager:error("Cannot observe remote DC: partition number mismatch"),
      {error, {partition_num_mismatch, PartitionsNumRemote, PartitionsNumLocal}};
    true ->
      case DCID == dc_utilities:get_my_dc_id() of
        true -> ok;
        false ->
          lager:info("Observing DC ~p", [DCID]),
          dc_utilities:ensure_all_vnodes_running_master(inter_dc_log_sender_vnode_master),
          %% Announce the new publisher addresses to all subscribers in this DC.
          %% Equivalently, we could just pick one node in the DC and delegate all the subscription work to it.
          %% But we want to balance the work, so all nodes take part in subscribing.
          Nodes = dc_utilities:get_my_dc_nodes(),
          lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_log_reader_query, add_dc, [DCID, LogReaders]) end, Nodes),
          lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_sub, add_dc, [DCID, Publishers]) end, Nodes)
      end
  end.

-spec observe_dcs([#descriptor{}]) -> ok.
observe_dcs(Descriptors) -> lists:foreach(fun observe_dc/1, Descriptors).

-spec observe_dcs_sync([#descriptor{}]) -> ok.
observe_dcs_sync(Descriptors) ->
  {ok, SS} = vectorclock:get_stable_snapshot(),
  observe_dcs(Descriptors),
  lists:foreach(fun(#descriptor{dcid = DCID}) ->
    Value = vectorclock:get_clock_of_dc(DCID, SS),
    wait_for_stable_snapshot(DCID, Value)
  end, Descriptors).

-spec add_network_delays([{#descriptor{}, non_neg_integer()}]) -> ok.
add_network_delays(Descriptors) ->
    lists:foreach(fun add_network_delay/1, Descriptors).

-spec add_network_delay({#descriptor{}, non_neg_integer()}) -> ok.
add_network_delay({#descriptor{dcid = DCID}, Delay}) ->
    lager:info("Adding network delay ~p ms to DC ~p", [Delay, DCID]),
    dc_utilities:bcast_vnode_sync(inter_dc_sub_vnode_master, {add_delay, DCID, Delay}).

-spec observe_dc_sync(#descriptor{}) -> ok.
observe_dc_sync(Descriptor) -> observe_dcs_sync([Descriptor]).

-spec forget_dc(#descriptor{}) -> ok.
forget_dc(#descriptor{dcid = DCID}) ->
  case DCID == dc_utilities:get_my_dc_id() of
    true -> ok;
    false ->
      lager:info("Forgetting DC ~p", [DCID]),
      Nodes = dc_utilities:get_my_dc_nodes(),
      lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_log_reader_query, del_dc, [DCID]) end, Nodes),
      lists:foreach(fun(Node) -> ok = rpc:call(Node, inter_dc_sub, del_dc, [DCID]) end, Nodes)
  end.

-spec forget_dcs([#descriptor{}]) -> ok.
forget_dcs(Descriptors) -> lists:foreach(fun forget_dc/1, Descriptors).

%%%%%%%%%%%%%
%% Utils

observe(DcNodeAddress) ->
  {ok, Desc} = rpc:call(DcNodeAddress, inter_dc_manager, get_descriptor, []),
  observe_dc(Desc).

wait_for_stable_snapshot(DCID, MinValue) ->
  case DCID == dc_utilities:get_my_dc_id() of
    true -> ok;
    false ->
      {ok, SS} = vectorclock:get_stable_snapshot(),
      Value = vectorclock:get_clock_of_dc(DCID, SS),
      case Value > MinValue of
        true ->
          lager:info("Connected to DC ~p", [DCID]),
          ok;
        false ->
          lager:info("Waiting for DC ~p", [DCID]),
          timer:sleep(1000),
          wait_for_stable_snapshot(DCID, MinValue)
      end
  end.
