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
-module(inter_dc_repl).
-behaviour(gen_fsm).
-include("floppy.hrl").

%%gen_fsm call backs
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([ready/2, await_ack/2]).

%%public api
-export([propogate/1, propogate_sync/1, acknowledge/2]).

-record(state,
        { otherdc :: node(),
          otherpid :: pid(),
          payload :: payload(),
          retries :: integer(),
          from :: {non_neg_integer(),pid()} | ignore
        }).

-type state() :: #state{}.
 
-define(TIMEOUT,60000).

%%public api
propogate_sync(Payload) ->
    Me = self(),
    ReqId = 1, %Generate reqID here -> adapt then type in the state record!
    _ = gen_fsm:start_link(?MODULE, {?OTHER_DC, inter_dc_recvr, Payload, {ReqId, Me}}, []),
    receive
        {ReqId, normal} -> done;
        {ReqId, Reason} -> Reason
    end.

propogate(Payload) ->
    gen_fsm:start_link(?MODULE, {?OTHER_DC, inter_dc_recvr, Payload, ignore}, []).

-spec acknowledge(pid(),payload()) -> ok.
acknowledge(Pid, Payload) ->
    gen_fsm:send_event(Pid, {ok, Payload}),
    ok.

%% gen_fsm callbacks
-spec init({node(),pid(),payload(),{non_neg_integer(),pid()} | ignore}) -> {ok,ready,state(),0}.
init({OtherDC, OtherPid, Payload, From}) ->
    StateData = #state{otherdc = OtherDC, otherpid = OtherPid, payload = Payload, retries = 5, from = From},
    {ok, ready, StateData, 0}.

-spec ready(timeout,state()) -> {next_state, await_ack,state(),?TIMEOUT}.
ready(timeout, StateData = #state{otherdc = Other, otherpid = PID, retries = RetryLeft, payload = Payload}) ->
    inter_dc_recvr:replicate({PID,Other}, Payload, {self(),node()}),
    {next_state, await_ack,
     StateData#state{retries = RetryLeft -1},
     ?TIMEOUT}.

-spec await_ack({ok,payload()},state()) -> {stop,normal,state()};
               (timeout,state()) -> {stop,request_timeout, state()} | {next_state, ready,state(),0}.
await_ack({ok,Payload}, StateData) ->
    lager:info("Replicated ~p ~n",[Payload]),
    {stop, normal, StateData};
await_ack(timeout,StateData = #state{retries = RetryLeft}) ->
    case RetryLeft of
        0 -> {stop, request_timeout, StateData};
        _ -> {next_state, ready, StateData, 0}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State=#state{from = From}) ->
    case From of
        {ReqId, Pid} ->
            Pid ! {ReqId, Reason},
            Reason;
        ignore ->
            Reason
    end.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
