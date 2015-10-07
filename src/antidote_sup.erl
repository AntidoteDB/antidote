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
-module(antidote_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
	 start_rep/1,
	 stop_rep/0,
	 start_cross_dc_read_communication_recvr/1,
	 start_collect_sent/0,
	 start_safe_time_sender/0,
	 start_ext_read_connection/0,
	 start_senders/0,
	 start_recvrs/4]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc: start_rep(Port) - starts a server managed by Pid which listens for 
%% incomming tcp connection on port Port. Server receives updates to replicate 
%% from other DCs 
start_rep({Id,{DcIp,Ports}}) ->
    lager:info("Starting sup inter dc recvr..."),
    inter_dc_manager:start_receiver({Id,{DcIp,hd(Ports)}}),
    supervisor:start_child(?MODULE, {inter_dc_communication_sup,
                    {inter_dc_communication_sup, start_link, [self(),Ports]},
				     permanent, 5000, supervisor, [inter_dc_communication_sup]}),
    ListPorts = lists:foldl(fun(Port,Acc) ->
				    receive
					ready ->
					    Acc ++ [{DcIp,Port}]
				    end
			    end, [], Ports),
    lager:info("Done starting sup inter dc recvr..."),
    {ok, ListPorts}.


start_cross_dc_read_communication_recvr({Id,{DcIp,Port}}) ->
    lager:info("Starting sup cross read dc recvr..."),
    inter_dc_manager:start_read_receiver({Id,{DcIp,Port}}),
    supervisor:start_child(?MODULE, {cross_dc_read_communication_sup,
                    {cross_dc_read_communication_sup, start_link, [Port]},
				     permanent, 5000, supervisor, [cross_dc_read_communication_sup]}),
    {ok, {DcIp,Port}}.


start_safe_time_sender() ->
    lager:info("Starting safe time sender..."),
    supervisor:start_child(?MODULE, {inter_dc_safe_send_sup,
                    {inter_dc_safe_send_sup, start_link, []},
				     permanent, 5000, supervisor, [inter_dc_safe_send_sup]}),
    ok.

start_collect_sent() ->
    DCs = inter_dc_manager:get_dcs(),
    lager:info("Starting sup collect sent..."),
    supervisor:start_child(?MODULE, {collect_sent_time_sup,
				     {collect_sent_time_sup, start_link, [DCs,0]},
				     permanent, 5000, supervisor, [collect_sent_time_sup]}),
    ok.


start_ext_read_connection() ->
    DCs = inter_dc_manager:get_read_dcs(),
    lager:info("Starting external read connection..."),
    supervisor:start_child(?MODULE, {ext_read_connection_sup,
				     {ext_read_connection_sup, start_link, [DCs]},
				     permanent, 5000, supervisor, [ext_read_connection_sup]}),
    ok.


start_senders() ->
    start_collect_sent(),
    start_safe_time_sender(),
    ok.

start_recvrs(AddrType, Id, RecvrPorts, ReadPort) ->
    Ip = my_ip(AddrType),
    RepPorts = start_rep({Id,{Ip, RecvrPorts}}),
    start_cross_dc_read_communication_recvr({Id,{Ip, ReadPort}}),
    {ok,RepPorts,{Ip,ReadPort}}.
    

my_ip(internet) ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    list_to_atom(inet_parse:ntoa(Ip));
my_ip(local) ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = lists:last(List),
    list_to_atom(inet_parse:ntoa(Ip)).

stop_rep() ->
    ok = supervisor:terminate_child(inter_dc_communication_sup, inter_dc_communication_recvr),
    _ = supervisor:delete_child(inter_dc_communication_sup, inter_dc_communication_recvr),
    ok = supervisor:terminate_child(inter_dc_communication_sup, inter_dc_communication_fsm_sup),
    _ = supervisor:delete_child(inter_dc_communication_sup, inter_dc_communication_fsm_sup),
    ok = supervisor:terminate_child(?MODULE, inter_dc_communication_sup),
    _ = supervisor:delete_child(?MODULE, inter_dc_communication_sup),
    ok.
    
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    LoggingMaster = {logging_vnode_master,
                     {riak_core_vnode_master, start_link, [logging_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},
    
    ClockSIMaster = {clocksi_vnode_master,
		     {riak_core_vnode_master, start_link, [clocksi_vnode]},
		     permanent, 5000, worker, [riak_core_vnode_master]},
    
    InterDcRepMaster = {inter_dc_repl_vnode_master,
                        {riak_core_vnode_master, start_link,
                         [inter_dc_repl_vnode]},
                        permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRecvrMaster = {inter_dc_recvr_vnode_master,
			  {riak_core_vnode_master, start_link,
			   [inter_dc_recvr_vnode]},
			  permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIsTxCoordSup =  {clocksi_static_tx_coord_sup,
                           {clocksi_static_tx_coord_sup, start_link, []},
                           permanent, 5000, supervisor, [clockSI_static_tx_coord_sup]},

    ClockSIiTxCoordSup =  {clocksi_interactive_tx_coord_sup,
			   {clocksi_interactive_tx_coord_sup, start_link, []},
			   permanent, 5000, supervisor,
			   [clockSI_interactive_tx_coord_sup]},
    
    ClockSIReadSup = {clocksi_readitem_sup,
    		      {clocksi_readitem_sup, start_link, []},
    		      permanent, 5000, supervisor,
    		      [clocksi_readitem_sup]},
        
    MaterializerMaster = {materializer_vnode_master,
                          {riak_core_vnode_master,  start_link,
                           [materializer_vnode]},
                          permanent, 5000, worker, [riak_core_vnode_master]},
    
    InterDcSenderSup = {inter_dc_communication_sender_fsm_sup,
			{inter_dc_communication_sender_fsm_sup, start_link, []},
			permanent, 5000, supervisor,
			[inter_dc_communication_sender_fsm_sup]},
    
    MetaDataManagerSup = {meta_data_manager_sup,
			  {meta_data_manager_sup, start_link, [stable]},
			  permanent, 5000, supervisor,
			  [meta_data_manager_sup]},

    MetaDataSenderSup = {meta_data_sender_sup,
			  {meta_data_sender_sup, start_link, [stable_time_functions:export_funcs_and_vals()]},
			  permanent, 5000, supervisor,
			  [meta_data_sender_sup]},
    
    %% InterDcManager = {inter_dc_manager,
    %% 		      {inter_dc_manager, start_link, []},
    %% 		      permanent, 5000, worker, [inter_dc_manager]},

    {ok,
     {{one_for_one, 5, 10},
      [LoggingMaster,
       ClockSIMaster,
       %% DataMaster,
       ClockSIsTxCoordSup,
       ClockSIiTxCoordSup,
       ClockSIReadSup,
       InterDcRepMaster,
       InterDcRecvrMaster,
       %% InterDcManager,
       MetaDataManagerSup,
       MetaDataSenderSup,
       InterDcSenderSup,
       MaterializerMaster]}}.
