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
%% Can be crashed and restarted without too much harm (only the temporary state is lost)
%% (TODO: implement inter-dc transference policy E.g, round-robin).
%% TODO check whether only local operations are allowed (currently yes to prevent odd behaviour)

-module(bcounter_mgr).
-behaviour(gen_server).

-export([generate_downstream/3,
    process_transfer/1,
    request_response/1,
    get_pending_transfer_requests/0,
    set_transfer_periodic_active/1]).

-export([start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

-type pending_transfer_requests() :: orddict:orddict(internal_key(), [{pos_integer(), clock_time()}]).
-type last_transfers() :: orddict:orddict({internal_key(), dcid()}, clock_time()).
-record(state, {
    pending_transfer_requests = orddict:new() :: pending_transfer_requests(),
    last_transfers = orddict:new() :: last_transfers(),
    transfer_timer :: reference(),
    transfer_periodic_active = true :: boolean()
}).
-type state() :: #state{}.

-type counter_b() :: antidote_crdt_counter_b:antidote_crdt_counter_b().
-type counter_b_op() :: antidote_crdt_counter_b:antidote_crdt_counter_b_anon_op().
-type counter_b_downstream_op_result() :: {ok, {{increment, pos_integer()} | {decrement, pos_integer()} | {transfer, pos_integer(), dcid()}, dcid()}}.
-type counter_b_downstream_op_result_or_error() :: counter_b_downstream_op_result() | {error, no_permission} | {error, {invalid_dcid, term()}}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Processes a decrement operation for a bounded counter.
%% If the operation is unsafe (i.e. the value of the counter can go
%% below 0), operation fails, otherwise a downstream for the decrement
%% is generated.
-spec generate_downstream(internal_key(), counter_b_op(), counter_b()) -> counter_b_downstream_op_result_or_error().
generate_downstream(Key, {decrement, Amount}, BCounter) when is_integer(Amount) ->
    generate_downstream(Key, {decrement, {Amount, undefined}}, BCounter);
generate_downstream(Key, {decrement, {Amount, undefined}}, BCounter) ->
    generate_decrement_downstream(Key, {decrement, {Amount, dc_utilities:get_my_dc_id()}}, BCounter);
generate_downstream(Key, {decrement, {Amount, DCID}}, BCounter) ->
    case DCID == dc_utilities:get_my_dc_id() of
        true -> generate_decrement_downstream(Key, {decrement, {Amount, DCID}}, BCounter);
        false -> {error, {invalid_dcid, DCID}}
    end;

%% @doc Processes an increment operation for the bounded counter.
%% Operation is always safe.
generate_downstream(Key, {increment, Amount}, BCounter) when is_integer(Amount) ->
    generate_downstream(Key, {increment, {Amount, undefined}}, BCounter);
generate_downstream(_Key, {increment, {Amount, undefined}}, BCounter) ->
    generate_increment_downstream({increment, {Amount, dc_utilities:get_my_dc_id()}}, BCounter);
generate_downstream(_Key, {increment, {Amount, DCID}}, BCounter) ->
    case DCID == dc_utilities:get_my_dc_id() of
        true -> generate_increment_downstream({increment, {Amount, DCID}}, BCounter);
        false -> {error, {invalid_dcid, DCID}}
    end;

%% @doc Processes a transfer operation between two owners of the
%% counter.
generate_downstream(Key, {transfer, {Amount, ToDCID}}, BCounter) ->
    generate_downstream(Key, {transfer, {Amount, ToDCID, undefined}}, BCounter);
generate_downstream(_Key, {transfer, {Amount, ToDCID, undefined}}, BCounter) ->
    generate_transfer_downstream({transfer, {Amount, ToDCID, dc_utilities:get_my_dc_id()}}, BCounter);
generate_downstream(_Key, {transfer, {Amount, ToDCID, FromDCID}}, BCounter) ->
    case FromDCID == dc_utilities:get_my_dc_id() of
        true -> generate_transfer_downstream({transfer, {Amount, ToDCID, FromDCID}}, BCounter);
        false -> {error, {invalid_dcid, FromDCID}}
    end.

%% @doc Handles a remote transfer request.
-spec process_transfer({transfer, {internal_key(), pos_integer(), dcid()}}) -> ok.
process_transfer({transfer, TransferOp = {_, _, _}}) ->
    gen_server:cast(?MODULE, {transfer, TransferOp}).

%% @doc Request response - do nothing.
-spec request_response(binary()) -> ok.
request_response(_BinaryResponse) -> ok.

-spec get_pending_transfer_requests() -> pending_transfer_requests().
get_pending_transfer_requests() ->
    gen_server:call(?MODULE, get_pending_transfer_requests).

-spec set_transfer_periodic_active(boolean()) -> ok.
set_transfer_periodic_active(Active) ->
    gen_server:call(?MODULE, {set_transfer_periodic_active, Active}).

%% ===================================================================
%% Callbacks
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Timer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    {ok, #state{transfer_timer = Timer}}.

handle_call(Request = {_, {decrement, {_, _}}, _}, _From, State = #state{pending_transfer_requests = PendingTransferRequests}) ->
    {Result, NewPendingTransferRequests} = decrement(Request, PendingTransferRequests),
    {reply, Result, State#state{pending_transfer_requests = NewPendingTransferRequests}};

handle_call(get_pending_transfer_requests, _From, State = #state{pending_transfer_requests = PendingTransferRequests}) ->
    {reply, PendingTransferRequests, State};

handle_call({set_transfer_periodic_active, Active}, _From, State) ->
    {reply, ok, State#state{transfer_periodic_active = Active}}.

handle_cast({transfer, Request}, State = #state{last_transfers = LastTransfers}) ->
    NewLastTransfers = transfer(Request, LastTransfers),
    {noreply, State#state{last_transfers = NewLastTransfers}}.

handle_info(transfer_periodic, State = #state{transfer_periodic_active = false}) -> {noreply, restart_timer(State)};
handle_info(transfer_periodic, State = #state{pending_transfer_requests = PendingTransferRequests}) ->
    NewPendingTransferRequests = transfer_periodic(PendingTransferRequests),
    {noreply, restart_timer(State#state{pending_transfer_requests = NewPendingTransferRequests})}.

terminate(_Reason, #state{transfer_timer = OldTimer}) ->
    _ = erlang:cancel_timer(OldTimer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Private functions
%% ===================================================================

-spec generate_decrement_downstream(internal_key(), counter_b_op(), counter_b()) -> counter_b_downstream_op_result_or_error().
generate_decrement_downstream(Key, {decrement, {Amount, ValidDCID}}, BCounter) ->
    gen_server:call(?MODULE, {Key, {decrement, {Amount, ValidDCID}}, BCounter}).

-spec generate_increment_downstream(counter_b_op(), counter_b()) -> counter_b_downstream_op_result().
generate_increment_downstream({increment, {Amount, ValidDCID}}, BCounter) ->
    antidote_crdt_counter_b:downstream({increment, {Amount, ValidDCID}}, BCounter).

-spec generate_transfer_downstream(counter_b_op(), counter_b()) -> counter_b_downstream_op_result_or_error().
generate_transfer_downstream({transfer, {Amount, ToDCID, ValidFromDCID}}, BCounter) ->
    case check_valid_dcid(ToDCID) of %%prevent transferring to arbitrary DCIDs
        true ->
            case ToDCID /= ValidFromDCID of
                true -> antidote_crdt_counter_b:downstream({transfer, {Amount, ToDCID, ValidFromDCID}}, BCounter);
                false ->
                    %%TODO this transfer is equal to increment
                    generate_increment_downstream({increment, {Amount, ValidFromDCID}}, BCounter)
            end;
        false -> {error, {invalid_dcid, ToDCID}}
    end.

-spec restart_timer(state()) -> state().
restart_timer(State = #state{transfer_timer = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    NewTimer = erlang:send_after(?TRANSFER_FREQ, self(), transfer_periodic),
    State#state{transfer_timer = NewTimer}.

%% @doc Checks whether a dcid is valid in a bcounter operation
-spec check_valid_dcid(dcid()) -> boolean().
check_valid_dcid(DCID) ->
    lists:any(
        fun(#descriptor{dcid = DescriptorDCID}) ->
            DCID == DescriptorDCID
        end, dc_meta_data_utilities:get_dc_descriptors()).

%% @doc Only for decrement.
%% In case enough permissions exist locally the decrement is performed locally.
%% Otherwise the decrement fails and a transfer request is added to the pending transfer requests.
%% Requests are sent periodically to remote dcs so it might take some time until the requested amount is available.
-spec decrement({internal_key(), {decrement, {pos_integer(), dcid()}}, counter_b()}, pending_transfer_requests()) -> {counter_b_downstream_op_result_or_error(), pending_transfer_requests()}.
decrement({Key, {Op, {Amount, MyDCID}}, BCounter}, PendingTransferRequests) ->
    case antidote_crdt_counter_b:generate_downstream_check({Op, Amount}, MyDCID, BCounter, Amount) of
        {error, no_permissions} = FailedResult ->
            Available = antidote_crdt_counter_b:localPermissions(MyDCID, BCounter),
            UpdatedPendingTransferRequests = add_transfer_request(Key, Amount - Available, PendingTransferRequests),
            {FailedResult, UpdatedPendingTransferRequests};
        Result ->
            {Result, PendingTransferRequests}
    end.

%% @doc Performs a transfer request for another dc.
%% Transfers on a key are limited by a timeout (GRACE_PERIOD).
%% The transfer is performed locally (which gets replicated eventually) and can also fail (in case the transfer amount is larger than what is available locally)
-spec transfer({internal_key(), pos_integer(), dcid()}, last_transfers()) -> last_transfers().
transfer({KeyBucket = {Key, Bucket}, Amount, RequesterDCID}, LastTransfers) ->
    ClearedLastTransfers = clear_old_transfers(LastTransfers, ?GRACE_PERIOD),
    MyDCID = dc_utilities:get_my_dc_id(),
    case can_transfer(KeyBucket, MyDCID, RequesterDCID, ClearedLastTransfers) of
        true ->
            BCounterObject = {Key, antidote_crdt_counter_b, Bucket},
            % try to transfer locks, might return {error, no_permissions} if not enough permissions are available locally
            _ = antidote:update_objects(ignore, [], [{BCounterObject, transfer, {Amount, RequesterDCID, MyDCID}}]),
            orddict:store({KeyBucket, RequesterDCID}, erlang:timestamp(), ClearedLastTransfers);
        false ->
            ClearedLastTransfers
    end.

%% @doc Sends pending transfer requests to remote dcs.
%% Called periodically to combine transfer requests to reduce network traffic (less total messages but larger messages)
-spec transfer_periodic(pending_transfer_requests()) -> pending_transfer_requests().
transfer_periodic(PendingTransferRequests) ->
    ClearedPendingTransferRequests = clear_old_transfer_requests(PendingTransferRequests, ?REQUEST_TIMEOUT),
    orddict:fold(
        fun(Key, TransferRequestList, RemainingPendingTransferRequests) ->
            %%TransferRequestList is never empty because of clear_old_transfer_requests
            AmountRequiredSum =
                lists:foldl(
                    fun({Request, _Timeout}, Sum) ->
                        Sum + Request
                    end, 0, TransferRequestList),
            AmountRemaining = send_transfer_request_to_remote_dcs(Key, AmountRequiredSum),

            %% No remote resources available, cancel further requests.
            case AmountRemaining == AmountRequiredSum of
                true -> RemainingPendingTransferRequests;
                false -> add_transfer_request(Key, AmountRemaining, RemainingPendingTransferRequests)
            end
        end, orddict:new(), ClearedPendingTransferRequests).

%% @doc Adds a transfer request to the pending transfer requests.
%% The transfer request are sent periodically in a single batch.
-spec add_transfer_request(internal_key(), pos_integer(), pending_transfer_requests()) -> pending_transfer_requests().
add_transfer_request(Key, Amount, PendingTransferRequests) ->
    PendingTransferRequestsForKey =
        case orddict:find(Key, PendingTransferRequests) of
            {ok, Value} -> Value;
            error -> orddict:new()
        end,
    CurrentTime = erlang:timestamp(),
    orddict:store(Key, [{Amount, CurrentTime} | PendingTransferRequestsForKey], PendingTransferRequests).

%% @doc Sends a transfer request to remote dcs to fulfill decrement requests eventually.
-spec send_transfer_request_to_remote_dcs(internal_key(), pos_integer()) -> non_neg_integer().
send_transfer_request_to_remote_dcs(KeyBucket = {Key, Bucket}, AmountRequiredSum) ->
    MyDCID = dc_utilities:get_my_dc_id(),
    BCounterObject = {Key, antidote_crdt_counter_b, Bucket},
    {ok, [BCounter], _} = antidote:read_objects(ignore, [], [BCounterObject]),
    PrefList = dcid_available_permissions_tuple_pref_list(MyDCID, BCounter),
    lists:foldl(
        fun
            ({RemoteDCID, AmountAvailableRemotely}, AmountRemaining) when AmountAvailableRemotely > 0 andalso AmountRemaining > 0 ->
                AmountToRequest =
                    case AmountAvailableRemotely - AmountRemaining >= 0 of
                        true -> AmountRemaining;
                        false -> AmountAvailableRemotely
                    end,
                perform_transfer_request(KeyBucket, AmountToRequest, MyDCID, RemoteDCID),
                AmountRemaining - AmountToRequest;
            (_, AmountRemaining) ->
                AmountRemaining
        end, AmountRequiredSum, PrefList).

%%TODO unknown_dc should not happen since we checked DCID before (special cases like individual crash possible)
%% @doc Send the request to a specified DC. Works asynchronously.
-spec perform_transfer_request(internal_key(), pos_integer(), dcid(), dcid()) -> ok | unknown_dc.
perform_transfer_request(Key, Amount, MyDCID, RemoteDCID) ->
    {LocalPartition, _} = log_utilities:get_key_partition(Key),
    BinaryMessage =
        term_to_binary(
            {request_permissions,
                {transfer, {Key, Amount, MyDCID}},
                LocalPartition,
                MyDCID,
                RemoteDCID
            }),
    inter_dc_query_req:perform_request(?BCOUNTER_REQUEST, {RemoteDCID, LocalPartition},
        BinaryMessage, fun bcounter_mgr:request_response/1).

%% Orders the reservation of each DC, from high to low.
-spec dcid_available_permissions_tuple_pref_list(dcid(), counter_b()) -> [{dcid(), non_neg_integer()}].
dcid_available_permissions_tuple_pref_list(MyDCID, BCounter) ->
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    OtherDCIDs = [DCID || #descriptor{dcid = DCID} <- OtherDCDescriptors, DCID /= MyDCID],
    OtherDCPermissions = [{DCID, antidote_crdt_counter_b:localPermissions(DCID, BCounter)} || DCID <- OtherDCIDs],
    lists:sort(fun({_, A}, {_, B}) -> A =< B end, OtherDCPermissions).

%% @doc Removes transfers that are considered old.
%% This allows sending new transfers for a key.
-spec clear_old_transfers(last_transfers(), non_neg_integer()) -> last_transfers().
clear_old_transfers(LastTransfers, TimeoutMicroseconds) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, Timeout) ->
            timer:now_diff(Timeout, CurrentTime) < TimeoutMicroseconds
        end, LastTransfers).

%% @doc Removes transfer requests that are considered old.
%% The request might be fulfilled already or the requester does not actually need it anymore.
-spec clear_old_transfer_requests(pending_transfer_requests(), non_neg_integer()) -> pending_transfer_requests().
clear_old_transfer_requests(PendingTransferRequests, TimeoutMicroseconds) ->
    CurrentTime = erlang:timestamp(),
    orddict:filter(
        fun(_, RequestList) ->
            FilteredRequestList =
                lists:filter(
                    fun({_, Timeout}) ->
                        timer:now_diff(Timeout, CurrentTime) < TimeoutMicroseconds
                    end, RequestList),
            length(FilteredRequestList) /= 0
        end, PendingTransferRequests).

%% @doc This checks whether a transfer for a key was performed to frequently
%% and prevents further transfers until a timeout is reached.
%% Also prevents transferring to invalid actors (unknown DCID)
-spec can_transfer(internal_key(), dcid(), dcid(), last_transfers()) -> boolean().
can_transfer(_, DCID, DCID, _) -> false;
can_transfer(Key, _, RequesterDCID, LastTransfers) ->
    ValidDCID = check_valid_dcid(RequesterDCID),
    case orddict:find({Key, RequesterDCID}, LastTransfers) of
        {ok, _Timeout} -> false;
        error -> ValidDCID
    end.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%%TODO use meck

setup() ->
    process_flag(trap_exit, true),
    {ok, Pid} = bcounter_mgr:start_link(),
    Pid.

cleanup(Pid) ->
    exit(Pid, normal),
    timer:sleep(50), %%TODO maybe check with recursion and lower intervals
    ?assertEqual(false, is_process_alive(Pid)).

bcounter_mgr_test() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun server_is_alive/1
    ]}.

server_is_alive(Pid) ->
    fun() ->
        ?assertEqual(true, is_process_alive(Pid))
    end.

-endif.
