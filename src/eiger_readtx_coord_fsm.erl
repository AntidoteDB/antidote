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
-export([start_link/2, start_link/1]).

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
          received=[] :: list(),
          final_results=[] :: list()
          max_evt=0 :: integer(),
          eff_time,
          total :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Keys) ->
    gen_fsm:start_link(?MODULE, [From, Keys], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, Keys]) ->
    SD = #state{keys=Keys,
                total=length(Keys)}
    {ok, execute_op, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op(timeout, SD0=#state{keys=Keys}) ->
    lists:foreach(fun(Key) ->
                    Preflist = log_utilities:get_preflist_from_key(Key),
                    IndexNode = hd(Preflist),
                    eiger:read_key(IndexNode, Key)
                  end, Keys),
    {next_state, collect_reads, SD0}.


collect_reads({Key, Value, EVT, LVT}, SD0=#state{received=Received0,
                                                 max_evt=MaxEVT0,
                                                 total=Total}) ->
    MaxEVT=max(MaxEVT0, EVT),
    Received = Received ++ [{Key, Value, EVT, LVT}],
    case length(Received) of
        Total ->
            {next_state, compute_efft, SD0#state{received=Received, max_evt=MaxEVT}, 0};
        _ ->
            {next_state, collect_reads, SD0#state{received=Received, max_evt=MaxEVT}}
    end.
     
compute_efft(timeout, SD0=#state{received=Received,
                                 max_evt=MaxEVT}) ->
    EffT = lists:foldl(fun(Elem, Min) ->
                        {_Key, _Value, _EVT, LVT} = Elem,
                        case LVT >= MaxEVT of
                            true ->
                                case EffT of
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
                                 received=Received}) ->
    FinalResults = lists:foldl(fun(Elem, Results) ->
                                {Key, Value, _EVT, LVT} = Elem,
                                case LVT < EffT of
                                    true ->
                                        Preflist = log_utilities:get_preflist_from_key(Key),
                                        IndexNode = hd(Preflist),
                                        eiger:read_key_time(IndexNode, Key, EffT),
                                        Results;
                                    _ ->
                                        Results ++ [{Key, Value}]
                                end
                               end, [], Received),
    {next_state, collect_second_reads, SD0#state{final_results=FinalResults}}.

collect_second_reads({Key, Value}, SD0=#state{final_results=FinalResults0,
                                              total=Total}) ->
    FinalResults = FinalResults0 ++ [{Key, Value],
    case length(FinalResults) of
        Total ->
            {next_state, reply, SD0#state{final_results=FinalResults}, 0};
        _ ->
            {next_state, collect_second_reads, SD0#state{final_results=FinalResults}}
    end.

reply(timeout, SD0=#state{final_results=FinalResults,
                          from=From}) ->
    gen_fsm:reply(From, {ok, FinalResults}). 

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
