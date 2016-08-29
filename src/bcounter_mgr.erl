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

%% Module for handling bounded counter operations.
%% Allows safe increment, decrement and transfer operations.
%% Basic inter-dc reservations manager only requests remote reservations
%% when necessay.
%% Transfer requests are throttled to prevent distribution unbalancing
%% (TODO: implement inter-dc transference policy E.g, round-robin).

-module(bcounter_mgr).
-behaviour(gen_server).

-export([start_link/0,
         generate_downstream/3,
         process_transfer/1,
         request_response/2
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-record(state, {req_queue, last_transfers, transfer_timer}).
-define(LOG_UTIL, log_utilities).


%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:info("Started Bounded counter manager at node ~p", [node()]),
    Timer=erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {ok, #state{req_queue=orddict:new(), transfer_timer=Timer, last_transfers=orddict:new()}}.

generate_downstream(Key, {{decrement, _}=Operation, _Actor}, BCounter) ->
    gen_server:call(?MODULE, {consume, Key, Operation, BCounter});

generate_downstream(Key, {{increment, _}=Operation, _Actor}, BCounter) ->
    gen_server:call(?MODULE, {generate, Key, Operation, BCounter});

generate_downstream(_Key, {Operation, _Actor}, BCounter) ->
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    crdt_bcounter:generate_downstream(Operation, MyId, BCounter).

process_transfer({transfer, Operation}) ->
    gen_server:cast(?MODULE, {transfer, Operation}).

%% ===================================================================
%% Callbacks
%% ===================================================================

handle_cast({transfer, {Key,Amount,Requester}}, #state{last_transfers=LT}=State) ->
    NewLT = cancel_consecutive_req(LT, ?GRACE_PERIOD),
    case can_process(Key, Requester, NewLT) of
        true ->
            antidote:append(Key, crdt_bcounter, {{transfer, Amount, Requester}, dc}),
            {noreply, State#state{last_transfers=orddict:store({Key, Requester}, erlang:now(), NewLT)}};
        _ ->
            {noreply, State#state{last_transfers=NewLT}}
    end.

handle_call({_IncOrDec, Key, {_,Amount}=Operation, BCounter}, _From, #state{req_queue=RQ}=State) ->
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    case crdt_bcounter:generate_downstream(Operation, MyId, BCounter) of
        {error, no_permissions} = FailedResult ->
            Available = crdt_bcounter:local_permissions(MyId, BCounter),
            UpdtQueue=queue_request(Key, Amount - Available, RQ),
            {reply, FailedResult, State#state{req_queue=UpdtQueue}};
        Result ->
            {reply, Result, State}
    end.

handle_info(transfer_periodic, #state{req_queue=RQ0,transfer_timer=OldTimer}=State) ->
    erlang:cancel_timer(OldTimer),
    RQ = clear_pending_req(RQ0, ?REQUEST_TIMEOUT),
    RQNew = orddict:fold(
              fun(Key, Queue, Accum) ->
                      case Queue of
                          [] -> Accum;
                          Queue ->
                              RequiredSum = lists:foldl(fun({Request, _Timeout}, Sum) ->
                                                                Sum + Request end, 0, Queue),
                              Remaining = request_remote( RequiredSum, Key),

                              %% No remote resourecs available, cancel further requests.
                              case Remaining == RequiredSum of
                                  false -> queue_request(Key, Remaining, Accum);
                                  true -> Accum
                              end
                      end
              end, orddict:new(), RQ),
    NewTimer=erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {noreply,State#state{transfer_timer=NewTimer, req_queue=RQNew}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

queue_request(_Key, 0, RequestsQueue) -> RequestsQueue;

queue_request(Key, Amount, RequestsQueue) ->
    QueueForKey = case orddict:find(Key, RequestsQueue) of
                      {ok, Value} -> Value;
                      error -> orddict:new()
                  end,
    CurrTime = erlang:now(),
    orddict:store(Key, [{Amount, CurrTime} | QueueForKey], RequestsQueue).

request_remote(0, _Key) -> 0;

request_remote(RequiredSum, Key) ->
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    {ok,Obj} = antidote:read(Key, crdt_bcounter),
    PrefList= pref_list(Obj),
    lists:foldl(
      fun({RemoteId, AvailableRemotely}, Remaining0) ->
              case Remaining0 > 0 of
                  true when AvailableRemotely > 0 ->
                      ToRequest = case AvailableRemotely - Remaining0 >= 0 of
                                      true -> Remaining0;
                                      false -> AvailableRemotely
                                  end,
                      do_request(MyId, RemoteId, Key, ToRequest),
                      Remaining0 - ToRequest;
                  _ -> Remaining0
              end
      end, RequiredSum, PrefList).

do_request(MyId, RemoteId, Key, Amount) ->
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    {LocalPartition, _} = hd(Preflist),
    BinaryMsg = term_to_binary({request_permissions,
                                {transfer, {Key, Amount, MyId}}, LocalPartition, MyId, RemoteId}),
    inter_dc_query:perform_request(?BCOUNTER_REQUEST, {RemoteId, LocalPartition},
                                   BinaryMsg, fun bcounter_mgr:request_response/2).

%% Orders the reservation of each DC, from high to low.
pref_list(Obj) ->
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    OtherDCIds = lists:foldl(fun(#descriptor{dcid=Id}, IdsList) ->
                                     case Id == MyId of
                                         true -> IdsList;
                                         false -> [Id | IdsList]
                                     end
                             end, [], OtherDCDescriptors),
    lists:sort(
      fun({_, A}, {_, B}) -> A =< B end,
      lists:foldl(fun(Id, Accum) ->
                          Permissions = crdt_bcounter:local_permissions(Id, Obj),
                          [{Id, Permissions} | Accum]
                  end, [], OtherDCIds)).

%% Request response - do nothing.
request_response(_BinaryRep,_RequestCacheEntry) -> ok.

cancel_consecutive_req(LastTransfers, Period) ->
    CurrTime = erlang:now(),
    orddict:filter(
      fun(_, Timeout) ->
              timer:now_diff(Timeout,CurrTime) < Period end, LastTransfers).

clear_pending_req(LastRequests, Period) ->
    CurrTime = erlang:now(),
    orddict:filter(fun(_, ListRequests) ->
                   FilteredList = lists:filter(fun({_, Timeout}) ->
                                   timer:now_diff(Timeout,CurrTime) < Period end, ListRequests),
                   length(FilteredList) /= 0
                   end , LastRequests).

can_process(Key, Requester, LastTransfers) ->
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    case Requester == MyId of
        false ->
            case orddict:find({Key, Requester}, LastTransfers) of
                {ok, _Timeout} ->
                    true;
                error -> true
            end;
        true -> false
    end
    .


