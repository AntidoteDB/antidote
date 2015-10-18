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
		con_dict,
                dcid}).


start_link() ->
    gen_fsm:start_link(?MODULE, [], []).


init([]) ->
    DcId = dc_utilities:get_my_dc_id(),
    DCs = inter_dc_manager:get_dcs(),
    NewDcDict = lists:foldl(fun(Dc, DcDict) ->
    				   dict:store(Dc,0,DcDict)
    			   end,
    			   dict:new(), DCs),
    {ok, loop_send_safe, #state{last_sent=NewDcDict,
				dcid=DcId,con_dict=dict:new()},0}.


loop_send_safe(timeout, State=#state{last_sent=LastSent,
				     dcid=DcId, con_dict=CD}) ->
    {NewSent,NewCD} =
	dict:fold(fun(Dc, LastSentTs, {LastSentAcc, ConDict}) ->
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
				  Clock = 0,
				  Time = NewMax,
				  TxId = 0,
				  %% Receiving DC treats safe time like a transaction
				  %% So wrap safe time in a transaction structure
				  Transaction = {TxId, {DcId, Time}, Clock, SafeTime},
				  %% Send safe to the given Dc
				  case inter_dc_communication_sender:propagate_sync_safe_time(
					 Dc, Transaction,ConDict) of
				      {ok,NewConDict} ->
					  {dict:store(Dc, NewMax, LastSentAcc),NewConDict};
				      {error,NewConDict1} ->
					  %% Keep the old time since there was an error sending the message
					  lager:error("Error safe send ~w", [error]),
					  {LastSentAcc,NewConDict1}
				  end;
			      _  ->
				  {LastSentAcc,ConDict}
			  end
		  end,
		  {LastSent,CD}, LastSent),
    {next_state, loop_send_safe, State#state{last_sent=NewSent,con_dict=NewCD},?SAFE_SEND_PERIOD}.


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
