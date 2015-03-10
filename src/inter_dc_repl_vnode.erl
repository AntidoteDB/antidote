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

%% @doc : This vnode is responsible for sending transaction committed in local
%%  DCs to remote DCs in commit-time order

-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-record(state, {partition,
                dcid,
                last_op=empty,
                reader}).

%% REPL_PERIOD: Frequency of checking new transactions and sending to other DC
-define(REPL_PERIOD, 5000).

start_vnode(I) ->
    {ok, Pid} = riak_core_vnode_master:get_vnode_pid(I, ?MODULE),
    %% Starts replication process by sending a trigger message
    riak_core_vnode:send_command(Pid, trigger),
    {ok, Pid}.

%% riak_core_vnode call backs
init([Partition]) ->
    DcId = dc_utilities:get_my_dc_id(),
    {ok, Reader} = clocksi_transaction_reader:init(Partition, DcId),
    {ok, #state{partition=Partition,
                dcid=DcId,
                reader = Reader}}.

handle_command(trigger, _Sender, State=#state{partition=Partition,
                                              reader=Reader}) ->
    {NewReaderState, DictTransactionsDcs, StableTime} =
        clocksi_transaction_reader:get_next_transactions(Reader),
    case dict:size(DictTransactionsDcs) of
        0 ->
            %% %% Send heartbeat
            %% Heartbeat = [#operation
            %%              {payload =
            %%                   #log_record{op_type=noop, op_payload = Partition}
            %%              }],
            %% DcId = dc_utilities:get_my_dc_id(),
            %% {ok, Clock} = vectorclock:get_clock(Partition),
            %% Time = clocksi_transaction_reader:get_prev_stable_time(NewReaderState),
            %% TxId = 0,
            %% %% Receiving DC treats hearbeat like a transaction
            %% %% So wrap heartbeat in a transaction structure
            %% Transaction = {TxId, {DcId, Time}, Clock, Heartbeat},
	    %% %% Send heartbeat to all DCs in partial replication alg
	    %% %% Still need to decide what to do with heartbeats in partial replication alg
	    %% {ok, DCs} = inter_dc_manager:get_dcs(),
            %% case inter_dc_communication_sender:propagate_sync(
            %%        dict:append(DCs, Transaction, dict:new()), StableTime, Partition) of
            %%     ok ->
            %%         NewReader = NewReaderState;
            %%     _ ->
            %%         NewReader = NewReaderState
            %% end;

	    %% No need to send a heartbeat
	    %% have to send safe time
	    DCs = inter_dc_manager:get_dcs(),
	    lists:foldl(fun({DcAddress,Port},_Acc) ->
				vectorclock:update_sent_clock({DcAddress,Port}, Partition, StableTime)
			end,
			0, DCs),
	    NewReader = NewReaderState;
	%% For partial replication, need to check if the external DC replicates
	%% the ops before sending the transactions
	%% For now this can be done just statically I guess
	%% To do it dynamically, before a DC changes what it replicates needs to tell
	%% each DC at what time it is changing, so the external DCs can safely
	%% send all the values for the new keys it is replicating
	_ ->
	    lists:foldl(fun({DCs,Message},_Acc) ->
				lager:info("DCs to send the trans: ~p", [DCs]),
				lager:info("The transaction: ~p", [Message])
			end,0,dict:to_list(DictTransactionsDcs)),
	    
            case inter_dc_communication_sender:propagate_sync(
                   DictTransactionsDcs, StableTime, Partition) of
                ok ->
		    DCs = inter_dc_manager:get_dcs(),
		    lists:foldl(fun({DcAddress,Port},_Acc) ->
					vectorclock:update_sent_clock({DcAddress,Port}, Partition, StableTime)
				end,
				0, DCs),
                    NewReader = NewReaderState;
                _ ->
		    lager:error("UnnnnnnnnnnnnnnnnnnnSuccessful send"),
                    NewReader = Reader
            end
    end,
    %% Update the time of the final 
    timer:sleep(?REPL_PERIOD),
    riak_core_vnode:send_command(self(), trigger),
    {reply, ok, State#state{reader=NewReader}}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
