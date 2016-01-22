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

-export([
	 %% get_my_dc/0,
	 %% get_my_dc_wid/0,
         %% start_receiver/1,
         get_dcs/0,
	 get_dcs_wids/0,
         %% add_dc/1,
         %% add_list_dcs/1,
	 %% get_my_read_dc/0,
	 %% get_my_read_dc_wid/0,
         %% start_read_receiver/1,
         %% get_read_dcs/0,
	 %% get_read_dcs_wids/0,
         %% add_read_dc/1,
         %% add_list_read_dcs/1,
	 set_replication_fun/4,
	 set_replication_list/1
	 %% simulate_partitions/1,
         %% stop_receiver/0]
	).

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

%% -define(META_PREFIX_DC, {dcid,port}).
%% -define(META_PREFIX_MY_DC, {mydcid,port}).
%% -define(META_PREFIX_READ_DC, {dcidread,port}).
%% -define(META_PREFIX_MY_READ_DC, {mydcidread,port}).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% ===================================================================
%% Public API
%% ===================================================================

%% get_my_dc_wid() ->
%%     riak_core_metadata:get(?META_PREFIX_MY_DC,mydc).

%% get_my_dc() ->
%%     {_Id,Dc} = riak_core_metadata:get(?META_PREFIX_MY_DC,mydc),
%%     Dc.

%% start_receiver({Id,{DcIp, Port}}) ->
%%     riak_core_metadata:put(?META_PREFIX_MY_DC,mydc,{Id,{DcIp,Port}}),
%%     ok.

%% stop_receiver() ->
%%     antidote_sup:stop_rep().

%% Returns all DCs known to this DC.
-spec get_dcs() ->[dc_address()].
get_dcs() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({{_Id,DC},[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

get_dcs_wids() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({{Id,DC},[0|_T]},NewAcc) ->
			lists:append([{Id,DC}],NewAcc) end,
		[], DcList).


%% %% Add info about a new DC. This info could be
%% %% used by other modules to communicate to other DC
%% -spec add_dc(dc_address()) -> ok.
%% add_dc({Id,{Ip,Port}}) ->
%%     riak_core_metadata:put(?META_PREFIX_DC,{Id,{Ip,Port}},0),
%%     ok.

%% %% Add a list of DCs to this DC
%% -spec add_list_dcs([dc_address()]) -> ok.
%% add_list_dcs(DCs) ->
%%     lists:foldl(fun({Id,{Ip,Port}},_Acc) ->
%% 			riak_core_metadata:put(?META_PREFIX_DC,{Id,{Ip,Port}},0)
%% 		end, 0, DCs),
%%     ok.

%% simulate_partitions(Partition) ->
%%     dc_utilities:bcast_vnode(inter_dc_repl_vnode_master,{pause,Partition}).

%% get_my_read_dc() ->
%%     {_Id,Dc} = riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc),
%%     Dc.
    
%% get_my_read_dc_wid() ->
%%     riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc).

%% start_read_receiver({Id,{DcIp,Port}}) ->
%%     riak_core_metadata:put(?META_PREFIX_MY_READ_DC,mydc,{Id,{DcIp,Port}}),
%%     ok.

%% get_read_dcs() ->
%%     DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
%%     lists:foldl(fun({{_Id,DC},[0|_T]},NewAcc) ->
%% 			lists:append([DC],NewAcc) end,
%% 		[], DcList).

%% get_read_dcs_wids() ->
%%     DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
%%     lists:foldl(fun({{Id,DC},[0|_T]},NewAcc) ->
%% 			lists:append([{Id,DC}],NewAcc) end,
%% 		[], DcList).


%% add_read_dc({Id,{Ip,Port}}) ->
%%     riak_core_metadata:put(?META_PREFIX_READ_DC,{Id,{Ip,Port}},0),
%%     ok.

%% add_list_read_dcs(DCs) ->
%%     lists:foldl(fun({Id,{Ip,Port}},_Acc) ->
%% 			riak_core_metadata:put(?META_PREFIX_READ_DC,{Id,{Ip,Port}},0)
%% 		end, 0, DCs),
%%     ok.

set_replication_fun(KeyDescription,Id,RepFactor,NumDcs) ->
    replication_check:set_replication_fun(KeyDescription,Id,RepFactor,NumDcs).

set_replication_list(List) ->
    replication_check:set_replication_list(List).

-spec get_descriptor() -> {ok, #descriptor{}}.
get_descriptor() ->
    %% Wait until all needed vnodes are spawned, so that the heartbeats are already being sent
    ok = dc_utilities:ensure_all_vnodes_running_master(inter_dc_log_sender_vnode_master),
    Nodes = dc_utilities:get_my_dc_nodes(),
    Publishers = lists:map(fun(Node) -> rpc:call(Node, inter_dc_pub, get_address_list, []) end, Nodes),
    LogReaders = lists:map(fun(Node) -> rpc:call(Node, inter_dc_log_reader_response, get_address_list, []) end, Nodes),
    Partitions = dc_utilities:get_all_partitions(),
    {ok, #descriptor{
	    dcid = dc_utilities:get_my_dc_id(),
	    partition_num = dc_utilities:get_partitions_num(),
	    publishers = Publishers,
	    logreaders = LogReaders,
	    partitions = Partitions
	   }}.


-spec observe_dc(#descriptor{}) -> ok | inter_dc_conn_err().
observe_dc(Desc = #descriptor{dcid = DCID, partition_num = PartitionsNumRemote, publishers = Publishers, logreaders = LogReaders}) ->
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
		    connect_nodes(Nodes, DCID, LogReaders, Publishers, Desc)
	    end
    end.

-spec start_bg_processes(list()) -> ok.
start_bg_processes(Name) ->
    %% Start the meta-data senders
    Nodes = dc_utilities:get_my_dc_nodes(),
    lists:foreach(fun(Node) -> ok = rpc:call(Node, meta_data_sender, start, [Name]) end, Nodes),
    %% Start the timers sending the heartbeats
    %% FIXME: Shouldn't the return value be matched??
    _ = dc_utilities:bcast_vnode_sync(inter_dc_log_sender_vnode_master, {start_timer}),
    ok.

-spec connect_nodes([node()], dcid(), [socket_address()], [socket_address()], #descriptor{}) -> ok | {error, connection_error}.
connect_nodes([], DCID, _LogReaders, _Publishers, _Desc) ->
    riak_core_metadata:put(?META_PREFIX_DC,{DCID,DCID},0),
    ok;
connect_nodes([Node|Rest], DCID, LogReaders, Publishers, Desc) ->
    case rpc:call(Node, inter_dc_log_reader_query, add_dc, [DCID, LogReaders], ?COMM_TIMEOUT) of
	ok ->
	    case rpc:call(Node, inter_dc_sub, add_dc, [DCID, Publishers], ?COMM_TIMEOUT) of
		ok ->
		    connect_nodes(Rest, DCID, LogReaders, Publishers, Desc);
		_ ->
		    lager:error("Unable to connect to publisher ~p", [DCID]),
		    ok = forget_dc(Desc),
		    {error, connection_error}
	    end;
	_ ->
	    lager:error("Unable to connect to log reader ~p", [DCID]),
	    ok = forget_dc(Desc),
	    {error, connection_error}
    end.

-spec observe_dcs([#descriptor{}]) -> [ok | inter_dc_conn_err()].
observe_dcs(Descriptors) -> lists:map(fun observe_dc/1, Descriptors).

-spec observe_dcs_sync([#descriptor{}]) -> [ok | inter_dc_conn_err()].
observe_dcs_sync(Descriptors) ->
    {ok, SS} = vectorclock:get_stable_snapshot(),
    DCs = lists:map(fun(DC) ->
			    {observe_dc(DC), DC}
		    end, Descriptors),
    lists:foreach(fun({Res, #descriptor{dcid = DCID}}) ->
			  case Res of
			      ok ->
				  Value = vectorclock:get_clock_of_dc(DCID, SS),
				  wait_for_stable_snapshot(DCID, Value);
			      _ ->
				  ok
			  end
		  end, DCs),
    [Result1 || {Result1, _DC1} <- DCs].

-spec observe_dc_sync(#descriptor{}) -> ok | inter_dc_conn_err().
observe_dc_sync(Descriptor) ->
    [Res] = observe_dcs_sync([Descriptor]),
    Res.

-spec add_network_delays([{#descriptor{}, non_neg_integer()}]) -> ok.
add_network_delays(Descriptors) ->
    lists:foreach(fun add_network_delay/1, Descriptors).

-spec add_network_delay({#descriptor{}, non_neg_integer()}) -> ok.
add_network_delay({#descriptor{dcid = DCID}, Delay}) ->
    lager:info("Adding network delay ~p ms to DC ~p", [Delay, DCID]),
    dc_utilities:bcast_vnode_sync(inter_dc_sub_vnode_master, {add_delay, DCID, Delay}).

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

