%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% Module for handling bounded counter operations.
%% Allows safe increment, decrement and transfer operations.
%% Basic inter-dc reservations manager only requests remote reservations
%% when necessary.
%% Transfer requests are throttled to prevent distribution unbalancing
%% (TODO: implement inter-dc transference policy E.g, round-robin).

-module(bcounter_mgr).
-behaviour(gen_server).

-export([generate_downstream/3,
    process_transfer/1,
    request_response/2
]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

%%TODO clock_time is a timestamp
-type last_transfers() :: orddict:orddict({{key(), bucket()}, dcid()}, clock_time()).
-type request_queue() :: orddict:orddict({key(), bucket()}, [{pos_integer(), clock_time()}]).
-record(state, {
    request_queue = orddict:new() :: request_queue(),
    last_transfers = orddict:new() :: last_transfers(),
    transfer_timer :: reference()
}).
-type antidote_crdt_counter_b_downstream_op() :: {{increment, pos_integer()} | {decrement, pos_integer()} | {transfer, pos_integer(), dcid()}, dcid()}. %%TODO maybe to specific here
-type antidote_crdt_counter_b_downstream_op_or_error() :: antidote_crdt_counter_b_downstream_op() | {error, no_permission}. %%TODO maybe to specific here

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Processes a decrement operation for a bounded counter.
%% If the operation is unsafe (i.e. the value of the counter can go
%% below 0), operation fails, otherwise a downstream for the decrement
%% is generated.
-spec generate_downstream(key() | {key(), bucket()}, antidote_crdt_counter_b:antidote_crdt_counter_b_anon_op(), antidote_crdt_counter_b:antidote_crdt_counter_b()) -> antidote_crdt_counter_b_downstream_op() | {error, no_permissions}.
generate_downstream(Key, {decrement, {Amount, _}}, BCounter) when Amount > 0 ->
    MyDCID = dc_utilities:get_my_dc_id(),
    gen_server:call(?MODULE, {consume, {Key, {decrement, {Amount, MyDCID}}, BCounter}});

%% @doc Processes an increment operation for the bounded counter.
%% Operation is always safe.
generate_downstream(_Key, {increment, {Amount, _}}, BCounter) when Amount > 0 ->
    MyDCID = dc_utilities:get_my_dc_id(),
    antidote_crdt_counter_b:downstream({increment, {Amount, MyDCID}}, BCounter);

%% @doc Processes a transfer operation between two owners of the
%% counter.
generate_downstream(_Key, {transfer, {Amount, ToDCID, FromDCID}}, BCounter) when Amount > 0 ->
    antidote_crdt_counter_b:downstream({transfer, {Amount, ToDCID, FromDCID}}, BCounter).

%% @doc Handles a remote transfer request.
-spec process_transfer({transfer, {key() | {key(), bucket()}, pos_integer(), dcid()}}) -> ok.
process_transfer({transfer, TransferOp = {_, Amount, _}}) when Amount > 0 ->
    gen_server:cast(?MODULE, {transfer, TransferOp}).

%% Request response - do nothing.
-spec request_response(binary(), request_cache_entry()) -> ok.
request_response(_BinaryResponse, _RequestCacheEntry) -> ok.

%% ===================================================================
%% Callbacks
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Timer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {ok, #state{transfer_timer = Timer}}.

handle_call({consume, Request}, _From, State = #state{request_queue = RequestQueue}) ->
    {Result, NewRequestQueue} = consume(Request, RequestQueue),
    {reply, Result, State#state{request_queue = NewRequestQueue}}.

handle_cast({transfer, Request}, State = #state{last_transfers = LastTransfers}) ->
    NewLastTransfers = transfer(Request, LastTransfers),
    {no_reply, State#state{last_transfers = NewLastTransfers}}.

handle_info(transfer_periodic, State = #state{request_queue = RequestQueue, transfer_timer = OldTimer}) ->
    _ = erlang:cancel_timer(OldTimer),
    NewRequestQueue = transfer_periodic(RequestQueue),
    NewTimer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {noreply, State#state{transfer_timer = NewTimer, request_queue = NewRequestQueue}}.

terminate(_Reason, #state{transfer_timer = OldTimer}) ->
    _ = erlang:cancel_timer(OldTimer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Private functions
%% ===================================================================

%%TODO this may not work correctly if the key is a tuple
-spec transfer({key() | {key(), bucket()}, pos_integer(), dcid()}, last_transfers()) -> last_transfers().
transfer({KeyBucketTuple = {Key, Bucket}, Amount, RequesterDCID}, LastTransfers) ->
    NewLastTransfers = cancel_consecutive_transfers(LastTransfers, ?GRACE_PERIOD),
    MyDCID = dc_utilities:get_my_dc_id(),
    case can_transfer(KeyBucketTuple, RequesterDCID, NewLastTransfers) of
        true ->
            BCounterObject = {Key, antidote_crdt_counter_b, Bucket},
            % try to transfer locks, might return {error, no_permissions} if not enough permissions are available locally
            _ = antidote:update_objects(ignore, [], [{BCounterObject, transfer, {Amount, RequesterDCID, MyDCID}}]),
            orddict:store({KeyBucketTuple, RequesterDCID}, erlang:timestamp(), NewLastTransfers);
        _ ->
            NewLastTransfers
    end;
transfer({Key, Amount, RequesterDCID}, State) ->
    transfer({{Key, ?BUCKET}, Amount, RequesterDCID}, State).

%%TODO this will not work correctly if the key is a tuple
-spec consume({key() | {key() | bucket()}, {decrement, {pos_integer(), dcid()}}, antidote_crdt_counter_b:antidote_crdt_counter_b()}, request_queue()) -> {antidote_crdt_counter_b_downstream_op_or_error(), request_queue()}.
consume({KeyBucketTuple = {_Key, _Bucket}, {Op, {Amount, _}}, BCounter}, RequestQueue) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    case antidote_crdt_counter_b:generate_downstream_check({Op, Amount}, MyDCID, BCounter, Amount) of
        {error, no_permissions} = FailedResult ->
            Available = antidote_crdt_counter_b:localPermissions(MyDCID, BCounter),
            UpdatedRequestQueue = queue_request(KeyBucketTuple, Amount - Available, RequestQueue),
            {FailedResult, UpdatedRequestQueue};
        Result ->
            {Result, RequestQueue}
    end;
consume({Key, Update, BCounter}, RequestQueue) ->
    consume({{Key, ?BUCKET}, Update, BCounter}, RequestQueue).

-spec transfer_periodic(request_queue()) -> request_queue().
transfer_periodic(RequestQueue) ->
    ClearedRequestQueue = clear_pending_requests(RequestQueue, ?REQUEST_TIMEOUT),
    orddict:fold(
        fun(KeyBucketTuple, RequestList, RemainingRequestQueue) ->
            case RequestList of
                [] -> RemainingRequestQueue;
                _ ->
                    RequiredSum =
                        lists:foldl(
                            fun({Request, _Timeout}, Sum) ->
                                Sum + Request
                            end, 0, RequestList),
                    Remaining = request_remote(RequiredSum, KeyBucketTuple),

                    %% No remote resources available, cancel further requests.
                    case Remaining == RequiredSum of
                        false -> queue_request(KeyBucketTuple, Remaining, RemainingRequestQueue);
                        true -> RemainingRequestQueue
                    end
            end
        end, orddict:new(), ClearedRequestQueue).

-spec queue_request({key(), bucket()}, integer(), request_queue()) -> request_queue().
queue_request(_, 0, RequestsQueue) -> RequestsQueue;
queue_request(KeyBucketTuple, Amount, RequestsQueue) ->
    QueueForKey =
        case orddict:find(KeyBucketTuple, RequestsQueue) of
            {ok, Value} -> Value;
            error -> orddict:new()
        end,
    CurrentTime = erlang:timestamp(),
    orddict:store(KeyBucketTuple, [{Amount, CurrentTime} | QueueForKey], RequestsQueue).

-spec request_remote(non_neg_integer(), {key(), bucket()}) -> non_neg_integer().
request_remote(0, _) -> 0;
request_remote(RequiredSum, KeyBucketTuple = {Key, Bucket}) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    BCounterObject = {Key, antidote_crdt_counter_b, Bucket},
    {ok, [BCounter], _} = antidote:read_objects(ignore, [], [BCounterObject]),
    PrefList = pref_list(BCounter),
    lists:foldl(
        fun({RemoteDCID, AvailableRemotely}, Remaining) ->
            case Remaining > 0 of
                true when AvailableRemotely > 0 ->
                    ToRequest =
                        case AvailableRemotely - Remaining >= 0 of
                            true -> Remaining;
                            false -> AvailableRemotely
                        end,
                    do_request(MyDCID, RemoteDCID, KeyBucketTuple, ToRequest),
                    Remaining - ToRequest;
                _ -> Remaining
            end
        end, RequiredSum, PrefList).

-spec do_request(dcid(), dcid(), {key(), bucket()}, pos_integer()) -> ok | unknown_dc.
do_request(MyDCID, RemoteDCID, KeyBucketTuple = {Key, _Bucket}, Amount) ->
    {LocalPartition, _} = log_utilities:get_key_partition(Key),
    BinaryMessage =
        term_to_binary(
            {request_permissions,
                {transfer, {KeyBucketTuple, Amount, MyDCID}},
                LocalPartition,
                MyDCID,
                RemoteDCID
            }),
    inter_dc_query:perform_request(?BCOUNTER_REQUEST, {RemoteDCID, LocalPartition},
        BinaryMessage, fun bcounter_mgr:request_response/2).

%%TODO name seems confusing
%% Orders the reservation of each DC, from high to low.
-spec pref_list(antidote_crdt_counter_b:antidote_crdt_counter_b()) -> [{dcid(), non_neg_integer()}].
pref_list(BCounter) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    OtherDCIDs = [DCID || #descriptor{dcid = DCID} <- OtherDCDescriptors, DCID /= MyDCID],
    OtherDCPermissions = [{DCID, antidote_crdt_counter_b:localPermissions(DCID, BCounter)} || DCID <- OtherDCIDs],
    lists:sort(fun({_, A}, {_, B}) -> A =< B end, OtherDCPermissions).

-spec cancel_consecutive_transfers(last_transfers(), microsecond()) -> last_transfers().
cancel_consecutive_transfers(LastTransfers, TimeoutMicroseconds) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, Timeout) ->
            timer:now_diff(Timeout, CurrentTime) < TimeoutMicroseconds
        end, LastTransfers).

-spec clear_pending_requests(request_queue(), microsecond()) -> request_queue().
clear_pending_requests(RequestQueue, TimeoutMicroseconds) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, RequestList) ->
            FilteredRequestList =
                lists:filter(
                    fun({_, Timeout}) ->
                        timer:now_diff(Timeout, CurrentTime) < TimeoutMicroseconds
                    end, RequestList),
            length(FilteredRequestList) /= 0
        end, RequestQueue).

%%TODO currently this returns true if RequesterDCID /= MyDCID (probably this is not the intention)
-spec can_transfer({key(), bucket()}, dcid(), last_transfers()) -> boolean().
can_transfer(KeyBucketTuple, RequesterDCID, LastTransfers) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    case RequesterDCID == MyDCID of
        false ->
            case orddict:find({KeyBucketTuple, RequesterDCID}, LastTransfers) of
                {ok, _Timeout} ->
                    true; %%TODO this should probably be false since we do not want to allow transfer that already exist (see ?GRACE_PERIOD) %% Period during which transfer requests from the same DC to the same key are ignored.
                error -> true
            end;
        true -> false
    end.
