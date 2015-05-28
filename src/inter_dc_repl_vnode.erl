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
    timer:sleep(?REPL_PERIOD),
    {ok, DCs} = inter_dc_manager:get_dcs(),
    NewState = case DCs of
        [] -> State;
        DCs ->
            {NewReaderState, Transactions} =
                clocksi_transaction_reader:get_next_transactions(Reader),
            NewReader = case Transactions of
                [] ->
                    %% Send heartbeat
                    Heartbeat = [#operation
                                 {payload =
                                      #log_record{op_type=noop, op_payload = Partition}
                                 }],
                    DcId = dc_utilities:get_my_dc_id(),
                    {ok, Clock} = vectorclock:get_clock(Partition),
                    Time = clocksi_transaction_reader:get_prev_stable_time(NewReaderState),
                    TxId = 0,
                    %% Receiving DC treats hearbeat like a transaction
                    %% So wrap heartbeat in a transaction structure
                    Transaction = {TxId, {DcId, Time}, Clock, Heartbeat},
                    inter_dc_communication_sender:propagate_sync(
                        {replicate, [Transaction]}, DCs),
                    NewReaderState;
                [_H|_T] ->
                    ok = inter_dc_communication_sender:propagate_sync(
                           {replicate, Transactions}, DCs),
                    NewReaderState
            end,
            State#state{reader=NewReader}
    end,
    riak_core_vnode:send_command(self(), trigger),
    {noreply,NewState}.

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
