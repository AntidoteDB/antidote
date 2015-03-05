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

%% @doc : This gen_fsm is similar to inter_dc_repl_vnode, but instead of sending
%% transactions, it send safe_time messages to external DCs when they have recieved
%% all updates up to the given time

-module(inter_dc_safe_send_fsm).
-behaviour(gen_fsm).
-include("antidote.hrl").



-export([start_link/0]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([loop_send_safe/2]).

-record(state, {last_sent,
                dcid}).

%% SAFE_SEND_PERIOD: Frequency of checking new transactions and sending to other DC
-define(SAFE_SEND_PERIOD, 5000).


start_link() ->
    gen_fsm:start_link(?MODULE, [], []).
%%{ok, Pid} = riak_core_vnode_master:get_vnode_pid(I, ?MODULE),
%% Starts replication process by sending a trigger message
%%riak_core_vnode:send_command(Pid, trigger),
%%{ok, Pid}.

%% riak_core_vnode call backs
init([]) ->
    DcId = dc_utilities:get_my_dc_id(),
    DCs = inter_dc_manager:get_dcs(),
    NewDcDict = lists:foldl(fun(Dc, DcDict) ->
    				   dict:store(Dc,0,DcDict)
    			   end,
    			   dict:new(), DCs),
    {ok, loop_send_safe, #state{last_sent=NewDcDict,
				dcid=DcId},0}.



%% do i need to export this????
loop_send_safe(timeout, State=#state{last_sent=LastSent,
				     dcid=DcId}) ->
    NewSent = dict:fold(fun(Dc, LastSentTs, LastSentAcc) ->
				NewMax = collect_sent_time_fsm:get_max_sent_time(
					   Dc, LastSentTs),
				case NewMax > LastSentTs of
				    true -> 
					%% Send safetime just like doing a heartbeat transaction
					SafeTime = [#operation
						    {payload =
							 #log_record{op_type=safe_update, op_payload = 0}
						    }],
					DcId = dc_utilities:get_my_dc_id(),
					%% Dont need clock, should just give an empty value
					%% {ok, Clock} = vectorclock:get_clock(Partition),
					Clock = 0,
					Time = NewMax,
					TxId = 0,
					%% Receiving DC treats safe time like a transaction
					%% So wrap safe time in a transaction structure
					Transaction = {TxId, {DcId, Time}, Clock, SafeTime},
					%% Send safe to the given Dc
					case inter_dc_communication_sender:propagate_sync_safe_time(
					       Dc, Transaction) of
					    ok ->
						dict:store(Dc, NewMax, LastSentAcc);
					    _ ->
						%% Keep the old time since there was an error sending the message
						LastSentAcc
					end;
				    
				    _  ->
					LastSentAcc
				end,
				LastSentAcc 
			end,
			LastSent, LastSent),
    {next_state, loop_send_safe, State#state{last_sent=NewSent},?SAFE_SEND_PERIOD}.

    
    

%% Old way of doing this, using riak_metadata, keep for now
%% to test
%% loop_send_safe2(timeout, State=#state{last_sent=LastSent,
%% 				     dcid=DcId}) ->
%%     {ok, DCs} = inter_dc_manager:get_dcs(),
%%     %% Is this the way to get the ids of the partitions?
%%     %% Or are their ids given in some different way?
%%     {ok, Ring} = riak_core_ring_manager:get_my_ring(),
%%     NumPartitions = riak_core_ring:num_partitions(Ring),
%%     FirstMaxDict = lists:foldl(fun(DC,FirstDictAcc) ->
%% 				       dict:store(DC,riak_core_metadata:get(
%% 						       ?META_PREFIX_DC,{NumPartitions-1,DC}), FirstDictAcc)
%% 			       end,
%% 			       dict:new(), DCs),
%%     MaxToSend = list:foldl(fun(Dc,MaxToSendAcc) ->
%% 				   min_dc_sent_dict(NumPartitions-2, MaxToSendAcc, Dc)
%% 			   end,
%% 			   FirstMaxDict, DCs),
%%     %% Loops through the possible max to send, seing if they were larger than what was sent previoulsy
%%     %% If they are, then a new safe_time message is sent to the given external DC
%%     NewSent = dict:fold(fun(Dc,Timestamp,SentDict) ->
%% 				case Timestamp > dict:find(Dc, LastSent) of
%% 				    true -> 
%% 					%% Send safetime just like doing a heartbeat transaction
%% 					SafeTime = [#operation
%% 						    {payload =
%% 							 #log_record{op_type=safe_time, op_payload = Partition}
%% 						    }],
%% 					DcId = dc_utilities:get_my_dc_id(),
%% 					{ok, Clock} = vectorclock:get_clock(Partition),
%% 					Time = Timestamp,
%% 					TxId = 0,
%% 					%% Receiving DC treats safe time like a transaction
%% 					%% So wrap safe time in a transaction structure
%% 					Transaction = {TxId, {DcId, Time}, Clock, SafeTime},
%% 					%% Send safe to the given Dc
%% 					case inter_dc_communication_sender:propagate_sync_safe_time(
%% 					       Dc, Transaction) of
%% 					    ok ->
%% 						Acc = SentDict;
%% 					    _ ->
%% 						%% Keep the old time since there was an error sending the message
%% 						Acc = dict:update(Dc, dict:fetch(Dc, LastSent), SentDict)
%% 					end;
				    
%% 				    _  ->
%% 				end,
%% 				Acc 
%% 			end,
%% 			MaxToSend, MaxToSend),
%%     %% Update the time of the final 
%%     timer:sleep(?SAFE_SEND_PERIOD),
%%     %%riak_core_vnode:send_command(self(), trigger),
%%     %%{reply, ok, State#state{lastSent=NewSent}}.
%%     {next_state, loop_send_safe, State#state{lastSent=NewSent},0}.




%% %% Is this the right way to get the partition ids?
%% %% This is a helper function
%% min_dc_sent_dict(-1,DcSent,Dc) ->
%%     DcSent;
%% min_dc_sent_dict(N,DcSent,Dc) ->
%%     min_dc_sent_dict(N-1, dict:store(Dc, erlang:min(dict:get(Dc, DcSent),
%% 						    riak_core_metadata:get(?META_PREFIX_DC, {N, Dc})),
%% 				     DcSent), Dc).




    



handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
