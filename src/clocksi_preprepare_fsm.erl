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
-module(clocksi_preprepare_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([get_prepare_time/2,
         downstream/2]).

%% Spawn

-record(state, {updates,
                transaction,
                tx_coordinator,
                vnode,
                tx_type,
                prepare_time}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, Transaction, Updates, TxType) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 Transaction, Updates, TxType], []).

now_milisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, Transaction, Updates, TxType]) ->
    SD = #state{vnode=Vnode,
                tx_coordinator=Coordinator,
                transaction=Transaction,
                tx_type=TxType,
                updates=Updates},
    {ok, get_prepare_time, SD, 0}.

get_prepare_time(timeout, SD0=#state{transaction=Transaction}) ->
    Vclock = Transaction#transaction.vec_snapshot_time,
    DcId = dc_utilities:get_my_dc_id(),
    {ok, T_TS} = vectorclock:get_clock_of_dc(DcId, Vclock),
    Time = now_milisec(erlang:now()),
    case T_TS > Time of
        true ->
            timer:sleep((T_TS - Time) div 1000 + 1),
            PrepareTime = now_milisec(erlang:now());
        false ->
            PrepareTime = Time
    end,
    {next_state, downstream, SD0#state{prepare_time=PrepareTime}, 0}.

downstream(timeout, SD0=#state{transaction=Tx,
                               updates=Updates,
                               tx_type=TxType,
                               tx_coordinator=Coordinator,
                               prepare_time=PrepareTime,
                               vnode=Vnode}) ->
    case generate_downstream_ops(Updates, Tx, Vnode, []) of        
        {ok, Ops} ->
            case TxType of
                single ->
                    clocksi_vnode:single_commit(Vnode, Tx, Ops, Coordinator, PrepareTime);
                multi ->
                    clocksi_vnode:prepare(Vnode, Tx, Ops, Coordinator, PrepareTime)
            end;
        error ->
            clocksi_vnode:reply_coordinator(Coordinator, abort)
    end,
    {stop, normal, SD0}.
            
handle_info(_Info, StateName, StateData) ->
    {next_state,StateName,StateData,1}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Internal functions

generate_downstream_ops([], _Txn, _Vnode, Acc) ->
    {ok, Acc};

generate_downstream_ops([{Key, List}|Rest], Txn, Vnode, Acc0) ->
    case generate_downstream_key(List, Key, Txn, Vnode, Acc0, []) of
        {ok, Acc} ->
            generate_downstream_ops(Rest, Txn, Vnode, Acc);
        error ->
            error
    end.

generate_downstream_key([], _Key, _Txn, _State, DSOps, _PreviousDSOps) ->
    {ok, DSOps};

generate_downstream_key([Op|Rest], Key, Txn, Vnode, DSOps, PreviousDSOps) ->
    {Type, Param} = Op,
    case clocksi_downstream:generate_downstream_op(Txn, Vnode, Key, Type, Param, PreviousDSOps) of
        {ok, DownstreamRecord} ->
            generate_downstream_key(Rest, Key, Txn, Vnode, DSOps ++ [{Key, Type, DownstreamRecord}], PreviousDSOps ++ [DownstreamRecord]);
        {error, _} ->
            error
    end.
