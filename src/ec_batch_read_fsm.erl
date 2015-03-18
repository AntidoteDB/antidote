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
-module(ec_batch_read_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([return/2]).

%% Spawn

-record(state, {reads,
                tx_id,
                tx_coordinator,
                vnode}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, TxId, Reads) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 TxId, Reads], []).


%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, TxId, Reads]) ->
    SD = #state{vnode=Vnode,
                tx_coordinator=Coordinator,
                tx_id=TxId,
                reads=Reads},
    {ok, return, SD, 0}.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(timeout, SD0=#state{tx_coordinator=Coordinator,
                           vnode=Vnode,
                           reads=Reads,
                           tx_id=TxId}) ->
    case materializer_vnode:multi_read(Vnode, Reads, TxId) of
        {ok, PartitionReadSet} ->
			Reply = {batch_read_result, PartitionReadSet};
        {error, Reason} ->
            Reply={error, Reason}
    end,
    riak_core_vnode:reply(Coordinator, Reply),
    {stop, normal, SD0};
    
    
    
    
    
return(_SomeMessage, SDO) ->
    {next_state, return, SDO,0}.

handle_info(_Info, StateName, StateData) ->
    {next_state,StateName,StateData,1}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
