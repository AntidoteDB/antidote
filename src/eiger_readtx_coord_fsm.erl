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
-module(eiger_readtx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute_op/2,
         collect_reads/2,
         compute_efft/2,
         second_round/2,
         collect_second_reads/2,
         reply/2]).

-record(state, {
          from,
          transaction,
          received=[] :: list(),
          final_results=[] :: list(),
          final_deps=[] :: list(),
          max_evt=0 :: integer(),
          eff_time,
          keys_type,
          my_dc :: term(),
          total :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, KeysType) ->
    gen_fsm:start_link(?MODULE, [From, KeysType], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, KeysType]) ->
    DcId = dc_utilities:get_my_dc_id(),
    SD = #state{keys_type=KeysType,
                from=From,
                my_dc=DcId,
                total=length(KeysType)},
    {ok, execute_op, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op(timeout, SD0=#state{keys_type=KeysType, my_dc=DcId}) ->
    {ok, SnapshotTime} = clocksi_interactive_tx_coord_fsm:get_snapshot_time(),
    {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    Transaction = #transaction{snapshot_time=LocalClock,
                               vec_snapshot_time=SnapshotTime,
                               txn_id=TransactionId},
    lists:foreach(fun({Key, Type}) ->
                    Preflist = log_utilities:get_preflist_from_key(Key),
                    IndexNode = hd(Preflist),
                    eiger_vnode:read_key(IndexNode, Key, Type, TransactionId)
                  end, KeysType),
    {next_state, collect_reads, SD0#state{transaction=Transaction}}.

collect_reads({Key, Value, EVT, LVT, Timestamp}, SD0=#state{received=Received0,
                                                 max_evt=MaxEVT0,
                                                 total=Total}) ->
    lager:info("Collecting reads Key ~p, Value ~p, EVT ~p, LVT ~p" ,[Key, Value, EVT, LVT]),
    MaxEVT = case EVT of
                empty ->
                    MaxEVT0;
                _ ->
                    max(MaxEVT0, EVT)
             end,
    Received = [{Key, Value, EVT, LVT, Timestamp}|Received0],
    case length(Received) of
        Total ->
            {next_state, compute_efft, SD0#state{received=Received, max_evt=MaxEVT}, 0};
        _ ->
            {next_state, collect_reads, SD0#state{received=Received, max_evt=MaxEVT}}
    end.
     
compute_efft(timeout, SD0=#state{received=Received,
                                 max_evt=MaxEVT}) ->
    EffT = lists:foldl(fun(Elem, Min) ->
                        {_Key, _Value, _EVT, LVT, _} = Elem,
                        case LVT >= MaxEVT of
                            true ->
                                case Min of
                                    infinity ->
                                        LVT;
                                    _ ->
                                        min(Min, LVT)
                                end;
                            false ->
                                Min
                        end
                    end, infinity, Received),
    {next_state, second_round, SD0#state{eff_time=EffT}, 0}.

second_round(timeout, SD0=#state{eff_time=EffT, 
                                 total=Total, transaction=Transaction, 
                                 received=Received}) ->
    {FinalResults, FinalDeps} = lists:foldl(fun(Elem, {Results, Deps}) ->
                                {Key, Value, EVT, LVT, Timestamp} = Elem,
                                case (LVT < EffT) orelse (EVT == empty) of
                                    true ->
                                        Preflist = log_utilities:get_preflist_from_key(Key),
                                        IndexNode = hd(Preflist),
                                        %%This is ad-hoc..
                                        eiger_vnode:read_key_time(IndexNode, Key, riak_dt_lwwreg, 
                                            Transaction#transaction.txn_id, EffT),
                                        {Results, Deps};
                                    _ ->
                                        {[{Key, Value}|Results], [{Key, Timestamp}|Deps]}
                                end
                               end, {[], []}, Received),
    case length(FinalResults) of
        Total ->
            {next_state, reply, SD0#state{final_results=FinalResults, final_deps=FinalDeps}, 0};
        _ ->
            {next_state, collect_second_reads, SD0#state{final_results=FinalResults, final_deps=FinalDeps}}
    end.

collect_second_reads({Key, Value, Timestamp}, SD0=#state{final_results=FinalResults0, final_deps=FinalDeps0,
                                              total=Total}) ->
    FinalResults = [{Key, Value}|FinalResults0],
    FinalDeps = [{Key, Timestamp}|FinalDeps0],
    case length(FinalResults) of
        Total ->
            {next_state, reply, SD0#state{final_results=FinalResults, final_deps=FinalDeps}, 0};
        _ ->
            {next_state, collect_second_reads, SD0#state{final_results=FinalResults, final_deps=FinalDeps}}
    end.

reply(timeout, SD0=#state{final_results=FinalResults, final_deps=FinalDeps,
                          from=From}) ->
    From ! {ok, lists:reverse(FinalResults), lists:reverse(FinalDeps)},
    {stop, normal, SD0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
