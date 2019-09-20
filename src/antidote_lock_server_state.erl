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

%% @doc This module implements the main logic for distributing the lock.
%% It is a purely functional module, where each request takes the current state
%% and returns the new state and a list of actions to execute.
%%
-module(antidote_lock_server_state).
%%
-include("antidote.hrl").
-include("antidote_message_types.hrl").
-include("antidote_locks.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([state/0, actions/0, read_crdt_state_onresponse_data/0, update_crdt_state_onresponse_data/0, inter_dc_message/0]).

-export([
    initial/5,
    my_dc_id/1,
    new_request/5,
    remove_locks/4,
    check_release_locks/2,
    get_snapshot_time/1,
    set_timer_active/2,
    get_timer_active/1,
    is_lock_process/2,
    get_remote_waiting_locks/1,
    timer_tick/3,
    debug_log/1,
    on_read_crdt_state/5,
    on_receive_inter_dc_message/4,
    on_complete_crdt_update/4,
    print_state/1, print_systemtime/1, print_vc/1, print_actions/1, print_lock_entries/1]).


% state invariants:
% - if a process has an exclusive lock, no other process has a lock (held state)
% - if a local process is in held or waiting state:
%         for exclusive locks: all locks are locally available
%         for shared locks: the processes own lock is locally available

% Liveness:
% - if a process holds a lock, it is not waiting for any locks that are smaller (=> avoiding deadlocks)
% - each request has a request-time. Requests with lower request time are considered first so that
%   each process should eventually get its lock


-record(pid_state, {
    locks :: orddict:orddict(antidote_locks:lock(), lock_state_with_kind()),
    request_time :: integer(),
    requester :: requester()
}).

-type pid_state() :: #pid_state{}.

-type milliseconds() :: integer().

-record(remote_request, {
    requester :: dcid(),
    pid :: pid(),
    request_time :: milliseconds(),
    lock_item :: antidote_locks:lock_spec_item()
}).

-type remote_request() :: #remote_request{}.

-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    %% all datacenter ids
    all_dc_ids :: [dcid()],
    %% latest snapshot,
    snapshot_time = vectorclock:new() :: snapshot_time(),
    % for each local lock request (requester-pid) the corresponding state
    by_pid = #{} :: #{pid() => pid_state()},
    % for each lock: remote data centers who want this lock
    remote_requests = [] :: [remote_request()],
    % remote requests that already have been fulfilled
    % for every remote request, we store the snapshot time where the lock was last read
    % (might be removed from here if additional permissions come in)
    answered_remote_requests = [] :: orddict:orddict(remote_request(), snapshot_time()),
    % is the retry timer is active or not?
    timer_Active = false :: boolean(),
    % for each exclusive lock we have: the time when we acquired it
    time_acquired = #{} :: #{antidote_locks:lock() => milliseconds()},
    % for each lock we have: the last time that we used it
    last_used = #{} :: #{antidote_locks:lock() => milliseconds()},
    % locks currently being transferred to other datacenters
    % (CRDT state might or might not already be updated)
    locks_in_transfer = [] :: antidote_locks:lock_spec(),
    %%%%%%%%%%%%%%
    % configuration parameters:
    %%%%%%%%%%%%%%
    % minimal time (in ms) for holding an exclusive lock after the last use
    % (this should be enough to start another transaction using the same locks
    %  and thus avoid the need to request the same locks again)
    min_exclusive_lock_duration :: milliseconds(),
    % maximum time for holding a lock after first use
    % if own DC and others request the lock, the own DC is preferred for this duration
    % (higher value increases throughput and latency)
    max_lock_hold_duration :: milliseconds(),
    % artificial request-time delay when remote locks are required
    % (this is to avoid races and to make all DCs give up their locks at about the same time)
    remote_request_delay :: milliseconds()
}).

-opaque state() :: #state{}.



-type requester() :: {pid(), Tag :: term()}.

-type lock_state() :: waiting | held | waiting_remote.

-type lock_state_with_kind() ::
{lock_state(), antidote_locks:lock_kind()}.



-type action() ::
#read_crdt_state{}
| #update_crdt_state{}
| #send_inter_dc_message{}
| #accept_request{}
| #abort_request{}
| #set_timeout{}.


-record(lock_request, {
    requester_pid :: pid(),
    request_time :: milliseconds(),
    locks :: antidote_locks:lock_spec()
}).

-record(ack_locks, {
    locks :: antidote_locks:lock_spec()
}).

-record(locks_transferred, {
    locks :: [{pid(), antidote_locks:lock_spec_item()}],
    snapshot_time :: snapshot_time()
}).

-record(locks_transferred_cont, {
    locks :: [{pid(), antidote_locks:lock_spec_item()}]
}).


-record(handle_remote_requests_cont, {
    locks_to_transfer :: [remote_request()]
}).

-record(handle_remote_requests_cont2, {
    locks_transferred :: [remote_request()]
}).

-record(new_request_cont, {
    requester :: requester(),
    request_time :: milliseconds(),
    snapshot_time :: snapshot_time(),
    all_dcs :: [dcid()],
    lock_spec :: antidote_locks:lock_spec()
}).


-opaque read_crdt_state_onresponse_data() :: #new_request_cont{} | #locks_transferred_cont{} | #handle_remote_requests_cont{}.
-opaque update_crdt_state_onresponse_data() :: #handle_remote_requests_cont2{}.
-opaque inter_dc_message() :: #lock_request{} | #ack_locks{} | #locks_transferred{}.


-type actions() :: [action()].


%-----------------
% Public API:
%-----------------


%% The initial state
-spec initial(dcid(), [dcid()], milliseconds(), milliseconds(), milliseconds()) -> state().
initial(MyDcId, AllDcs, MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay) -> #state{
    dc_id = MyDcId,
    all_dc_ids = AllDcs,
    min_exclusive_lock_duration = MinExclusiveLockDuration,
    max_lock_hold_duration = MaxLockHoldDuration,
    remote_request_delay = RemoteRequestDelay
}.


%% returns the own data center id
-spec my_dc_id(state()) -> dcid().
my_dc_id(State) ->
    State#state.dc_id.


%% Adds a new request to the state.
%% Requester: The process requesting the lock
%% RequestTime: The current time (in ms)
%% SnapshotTime: The database snapshot time this request is based on
%% AllDcIds: List of all data centers in the system
%% LockEntries: The requested locks
-spec new_request(requester(), milliseconds(), snapshot_time(), antidote_locks:lock_spec(), state()) -> {actions(), state()}.
new_request(Requester, RequestTime, SnapshotTime, LockSpec, State) ->
    debug_log({event, new_request, #{
        requester => Requester,
        request_time => print_systemtime(RequestTime),
        snapshot_time => print_vc(SnapshotTime),
        lock_spec => LockSpec
    }}),
    CrdtKeys = antidote_lock_crdt:get_lock_objects_from_spec(LockSpec),
    Actions = [
        #read_crdt_state{snapshot_time = SnapshotTime, objects = CrdtKeys,
            data = #new_request_cont{requester = Requester, request_time = RequestTime, snapshot_time = SnapshotTime, all_dcs = State#state.all_dc_ids, lock_spec = LockSpec}}
    ],
    {Actions, State}.


-spec on_read_crdt_state(milliseconds(), read_crdt_state_onresponse_data(), snapshot_time(), [any()], state()) -> {actions(), state()}.
on_read_crdt_state(CurrentTime, Cont = #new_request_cont{}, ReadTimestamp, CrdtValues, State) ->
    LockEntries = lists:zip(Cont#new_request_cont.lock_spec, antidote_lock_crdt:parse_lock_values(CrdtValues)),
    Requester = Cont#new_request_cont.requester,
    RequestTime = Cont#new_request_cont.request_time,
    AllDcIds = Cont#new_request_cont.all_dcs,
    debug_log({event, new_request_cont, #{
        requester => Requester,
        request_time => print_systemtime(RequestTime),
        snapshot_time => print_vc(ReadTimestamp),
        all_dc_ids => print_dcs(AllDcIds),
        lock_entries => print_lock_entries(LockEntries)
    }}),
    {RequesterPid, _} = Requester,
    MyDcId = my_dc_id(State),
    Locks = [L || {L, _} <- LockEntries],
    MissingLocks = missing_locks(AllDcIds, MyDcId, LockEntries, State#state.locks_in_transfer),
    % if remote requests are necessary, wait longer
    RequestTime2 = case MissingLocks of
        [] -> RequestTime;
        _ -> RequestTime + State#state.remote_request_delay
    end,

    {Actions0, State0} = case maps:is_key(RequesterPid, State#state.by_pid) of
        false -> {[], State};
        true ->
            debug_log({event, process_already_active, #{}}),
            % only one request per process is possible, so cancel the old request
            remove_locks(CurrentTime, RequesterPid, vectorclock:new(), State)
    end,
    State1 = add_process(Requester, RequestTime2, Locks, State0),
    State2 = set_snapshot_time(ReadTimestamp, State1),
    debug_result(case MissingLocks of
        [] ->
            % we have all locks locally
            {Actions3, State3} = next_actions(State2, RequestTime),
            {Actions0 ++ Actions3, State3};
        _ ->
            % tell other data centers that we need locks
            % for shared locks, ask to get own lock back
            % for exclusive locks, ask everyone to give their lock


            % add time and dc to remote requests
            OtherDcs = AllDcIds -- [MyDcId],

            LockRequest = #lock_request{requester_pid = RequesterPid, request_time = RequestTime2, locks = MissingLocks},
            RequestsByDc2 = [{Dc, LockRequest} || Dc <- OtherDcs],

            WaitingLocks = ordsets:from_list([L || {L, _} <- MissingLocks]),
            State3 = set_lock_waiting_state(RequesterPid, WaitingLocks, State2, waiting, waiting_remote),
            {Actions, State4} = next_actions(State3, RequestTime),

            Actions2 = [#send_inter_dc_message{
                receiver = D,
                message = LR
            } || {D, LR} <- RequestsByDc2],
            {Actions0 ++ Actions ++ Actions2, State4}
    end);
on_read_crdt_state(CurrentTime, Cont = #locks_transferred_cont{}, ReadClock, CrdtValues, State) ->
    LockEntries = lists:zip(Cont#locks_transferred_cont.locks, antidote_lock_crdt:parse_lock_values(CrdtValues)),
    debug_log({event, locks_transferred_cont, #{
        current_time => CurrentTime,
        read_clock => print_vc(ReadClock),
        all_dcs => print_dcs(State#state.all_dc_ids),
        lock_entries => print_lock_entries(LockEntries)
    }}),
    LockStates = maps:from_list([{L, S} || {{_, {L, _K}}, S} <- LockEntries]),
    {Actions1, State2} = update_waiting_remote(State#state.all_dc_ids, LockStates, State),
    State3 = set_snapshot_time(ReadClock, State2),

    % mark updated lock requests as unanswered, so they get checked again
    State4 = State3#state{
        answered_remote_requests = orddict:filter(fun(#remote_request{lock_item = {L, _}}, Vc) ->
            not lists:any(fun({{_Pid, {L2, _K2}}, _LV}) -> L == L2 end, LockEntries)
                orelse vectorclock:le(ReadClock, Vc)
        end, State3#state.answered_remote_requests)
    },

    {Actions2, State5} = next_actions(State4, CurrentTime),
    debug_result({Actions1 ++ Actions2, State5});

on_read_crdt_state(CurrentTime, Cont = #handle_remote_requests_cont{}, ReadClock, CrdtValues, State) ->
    ParsedCrdtValues = antidote_lock_crdt:parse_lock_values(CrdtValues),
    RemoteRequests = Cont#handle_remote_requests_cont.locks_to_transfer,
    handle_remote_requests_cont(CurrentTime, RemoteRequests, ParsedCrdtValues, ReadClock, State).

-spec handle_remote_requests_cont(milliseconds(), [remote_request()], [antidote_lock_crdt:value()], snapshot_time(), state()) -> {actions(), state()}.
handle_remote_requests_cont(CurrentTime, RemoteRequests, ParsedCrdtValues, ReadClock, State) ->
    Locks = lists:usort([L || #remote_request{lock_item = {L, _}} <- RemoteRequests]),
    LockValues = maps:from_list(lists:zip(Locks, ParsedCrdtValues)),
    debug_log({event, handle_remote_requests_cont, #{
        current_time => CurrentTime,
        read_clock => print_vc(ReadClock),
        remote_requests => RemoteRequests,
        lock_values => LockValues
    }}),
    MyDcId = my_dc_id(State),
    Transfers = lists:map(
        fun(L) ->
            V = maps:get(L, LockValues),
            LRequests = [R || R <- RemoteRequests, element(1, R#remote_request.lock_item) == L],
            Requesters = lists:usort([R#remote_request.requester || R <- LRequests]),
            K = max_lock_kind([K || #remote_request{lock_item = {_, K}} <- LRequests]),
            Updates =
                lists:flatmap(fun(Requester) ->
                    case K of
                        shared ->
                            [{Requester, Requester} || maps:get(Requester, V, Requester) == MyDcId];
                        exclusive ->
                            [{D, Requester} || D <- State#state.all_dc_ids, maps:get(D, V, D) == MyDcId]
                    end
                end, Requesters),
            {L, Updates}
        end, Locks),
    LockUpdates = lists:flatmap(fun({L, Updates}) -> antidote_lock_crdt:make_lock_updates(L, Updates) end, Transfers),
    Transferred = lists:filter(fun(#remote_request{lock_item = {L, _}}) ->
        lists:any(fun({L2, _}) -> L == L2 end, Transfers)
    end, RemoteRequests),
    Cont = #handle_remote_requests_cont2{
        locks_transferred = Transferred
    },
    case LockUpdates of
        [] ->
            % if there are no updates, directly continue with on_complete_crdt_update
            on_complete_crdt_update(CurrentTime, Cont, ReadClock, State);
        _ ->
            Actions = [
                #update_crdt_state{
                    snapshot_time = ReadClock,
                    updates = LockUpdates,
                    data = Cont}
            ],
            {Actions, State}
    end.


-spec on_complete_crdt_update(milliseconds(), any(), snapshot_time(), state()) -> {actions(), state()}.
on_complete_crdt_update(CurrentTime, Cont = #handle_remote_requests_cont2{}, WriteClock, State) ->
    LocksTransferred = Cont#handle_remote_requests_cont2.locks_transferred,

    debug_log({event, on_complete_crdt_update, #{
        current_time => CurrentTime,
        write_clock => print_vc(WriteClock),
        locks_transferred => LocksTransferred
    }}),

    % notify receivers of transferred locks
    Actions = [
        #send_inter_dc_message{receiver = Dc,
            message = #locks_transferred{
                snapshot_time = WriteClock,
                locks = ordsets:from_list(Locks)
            }}
        || Dc <- State#state.all_dc_ids,
        [] /= (Locks = lists:flatmap(fun(R) ->
            case R#remote_request.requester == Dc of
                true ->
                    [{R#remote_request.pid, R#remote_request.lock_item}];
                false ->
                    []
            end
        end, LocksTransferred))
    ],


    State2 = State#state{
        snapshot_time = vectorclock:max([State#state.snapshot_time, WriteClock]),
        locks_in_transfer = ordsets:new()
    },

    {Actions2, State3} = next_actions(State2, CurrentTime),

    {Actions ++ Actions2, State3}.


-spec on_receive_inter_dc_message(milliseconds(), dcid(), inter_dc_message(), state()) -> {actions(), state()}.
on_receive_inter_dc_message(CurrentTime, SendingDc, #lock_request{locks = Locks, request_time = T, requester_pid = Pid}, State) ->
    debug_log({event, on_receive_inter_dc_message, #{
        ' kind' => lock_request,
        current_time => CurrentTime,
        locks => Locks,
        request_time => T,
        sending_dc => SendingDc,
        requester_pid => Pid
    }}),
    NewRequests = ordsets:from_list([#remote_request{request_time = T, requester = SendingDc, pid = Pid, lock_item = LI} || LI <- Locks]),
    NewState = State#state{
        remote_requests = ordsets:union(NewRequests, State#state.remote_requests),
        answered_remote_requests = antidote_list_utils:orddict_remove_keys(State#state.answered_remote_requests, NewRequests)
    },
    debug_result(next_actions(NewState, CurrentTime));
on_receive_inter_dc_message(_CurrentTime, SendingDc, #locks_transferred{snapshot_time = SnapshotTime, locks = PLocks}, State) ->
    debug_log({event, locks_transferred, #{
        ' kind' => lock_request,
        snapshot_time => SnapshotTime,
        sending_dc => SendingDc,
        locks => PLocks
    }}),
    Locks = [L || {_Pid, {L, _K}} <- PLocks],
    LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
    MergedSnapshotTime = vectorclock:max([SnapshotTime, State#state.snapshot_time]),
    Actions = [
        #read_crdt_state{
            snapshot_time = MergedSnapshotTime,
            objects = LockObjects,
            data = #locks_transferred_cont{
                locks = PLocks
            }
        }
    ],
    NewState = State#state{
        snapshot_time = MergedSnapshotTime
    },
    {Actions, NewState};
on_receive_inter_dc_message(CurrentTime, SendingDc, #ack_locks{locks = Locks}, State) ->
    debug_log({event, ack_locks, #{
        ' kind' => lock_request,
        sending_dc => SendingDc,
        locks => Locks
    }}),
    % remove all remote requests from SendingDc
    NewRemoteRequests = remove_acked_requests(SendingDc, Locks, State#state.remote_requests),
    NewState = State#state{
        remote_requests = NewRemoteRequests,
        answered_remote_requests = orddict:filter(fun(K, _V) ->
            ordsets:is_element(K, NewRemoteRequests) end, State#state.answered_remote_requests)
    },
    debug_result(next_actions(NewState, CurrentTime)).


% checks that we still have access to all the locks the transaction needed.
% This can only be violated if the lock server crashed, so it is sufficient
% to check that we still have an entry for the given process.
-spec check_release_locks(pid(), state()) -> ok | {still_waiting, requester()} | {error, Reason :: any()}.
check_release_locks(FromPid, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        {ok, PS} ->
            StillWaiting = fun({_, {S, _}}) -> S /= held end,
            case lists:any(StillWaiting, PS#pid_state.locks) of
                true -> {still_waiting, PS#pid_state.requester};
                false -> ok
            end;
        error -> {error, 'locks no longer available'}
    end.

% Removes the locks for the given pid and determines who can
% get the locks now.
% Returns the respective actions to perform.
-spec remove_locks(integer(), pid(), snapshot_time(), state()) -> {actions(), state()}.
remove_locks(CurrentTime, FromPid, CommitTime, State) ->
    debug_log({event, remove_locks, #{
        current_time => CurrentTime,
        from_pid => FromPid,
        commit_time => print_vc(CommitTime)
    }}),
    debug_result(case maps:find(FromPid, State#state.by_pid) of
        error ->
            % Pid entry does not exist -> do nothing
            {[], State};
        {ok, PidState} ->
            StateWithoutPid = State#state{
                by_pid = maps:remove(FromPid, State#state.by_pid),
                snapshot_time = merge_snapshot_time(CommitTime, State#state.snapshot_time),
                last_used = update_last_used(CurrentTime, PidState, State#state.last_used)
            },
            next_actions(StateWithoutPid, CurrentTime)
    end).

-spec get_snapshot_time(state()) -> snapshot_time().
get_snapshot_time(State) ->
    State#state.snapshot_time.

-spec set_timer_active(boolean(), state()) -> state().
set_timer_active(B, S) -> S#state{timer_Active = B}.

-spec get_timer_active(state()) -> boolean().
get_timer_active(#state{timer_Active = B}) -> B.




timer_tick(State, Time, _Msg) ->
    debug_log({event, timer_tick, #{
        current_time => print_systemtime(Time)
    }}),
    debug_result(next_actions(State, Time)).


%% For debugging: Transforms data into a form that is
%% more readable when printed by Erlang.
-spec print_state(state()) -> any().
print_state(State) ->
    #{
        dc_id => State#state.dc_id,
        snapshot_time => print_vc(State#state.snapshot_time),
        by_pid => maps:map(fun(_K, V) -> print_pid_state(V) end, State#state.by_pid),
        remote_requests => lists:map(fun print_remote_request/1, State#state.remote_requests),
        remote_requests_answered => lists:map(fun({K, V}) -> {print_remote_request(K), print_vc(V)} end, State#state.answered_remote_requests),
        timer_Active => State#state.timer_Active,
        time_acquired => print_map_to_time(State#state.time_acquired),
        last_used => print_map_to_time(State#state.last_used),
        locks_in_transfer => State#state.locks_in_transfer
    }.

print_remote_request(#remote_request{request_time = T, pid = Pid, lock_item = L, requester = Dc}) ->
    #{' remote_request' => Dc, pid =>  Pid, request_time => print_systemtime(T), lock_item => L}.

print_map_to_time(M) ->
    maps:map(fun(_, V) -> print_systemtime(V) end, M).

print_pid_state(PidState) ->
    #{
        lock => maps:from_list(PidState#pid_state.locks),
        request_time => print_systemtime(PidState#pid_state.request_time)
    }.

print_vc(undefined) ->
    #{};
print_vc(ignore) ->
    #{};
print_vc(Vc) ->
    maps:from_list([{print_dc(K), print_systemtime_micro_seconds(V)} || {K, V} <- vectorclock:to_list(Vc)]).

print_dcs(Dcs) ->
    [print_dc(D) || D <- Dcs].

print_dc({Dc, _}) when is_atom(Dc) ->
    list_to_atom(lists:sublist(atom_to_list(Dc), 4));
print_dc(Dc) ->
    Dc.

-spec print_lock_entries(ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()})) -> term().
print_lock_entries(Items) ->
    maps:map(
        fun(_K, V) ->
            print_lock_crdt(V)
        end,
        maps:from_list(Items)
    ).

-spec print_lock_crdt(antidote_lock_crdt:value()) -> any().
print_lock_crdt(LockValue) ->
    maps:from_list(
        [{print_dc(K), print_dc(V)} || {K, V} <- maps:to_list(LockValue)]
    ).


print_systemtime(Ms) when is_integer(Ms) ->
    {{_Year, _Month, _Day}, {Hour, Minute, Second}} = calendar:system_time_to_local_time(Ms, millisecond),
    lists:flatten(io_lib:format("~p:~p:~p.~p", [Hour, Minute, Second, Ms rem 1000]));
print_systemtime(Other) -> Other.

print_systemtime_micro_seconds(Ms) when is_integer(Ms) ->
    {{_Year, _Month, _Day}, {Hour, Minute, Second}} = calendar:system_time_to_local_time(Ms, micro_seconds),
    lists:flatten(io_lib:format("~p:~p:~p.~p", [Hour, Minute, Second, Ms rem 1000000])).


-spec print_actions(actions()) -> any().
print_actions(Actions) ->
    [print_action(A) || A <- Actions].


-spec print_action(action()) -> any().
print_action(#read_crdt_state{snapshot_time = S, objects = O, data = D}) ->
    #{action => read_crdt_state, snapshot_time => print_vc(S), objects => O, data => D};
print_action(#update_crdt_state{snapshot_time = S, updates = U, data = D}) ->
    #{action => update_crdt_state, snapshot_time => print_vc(S), updates => U, data => D};
print_action(#send_inter_dc_message{message = M, receiver = R}) ->
    #{action => send_inter_dc_message, message => M, receiver => print_dc(R)};
print_action(#accept_request{requester = R, clock = Clock}) ->
    #{action => accept_request, requester => R, clock => Clock};
print_action(#abort_request{requester = R}) ->
    #{action => abort_request, requester => R};
print_action(#set_timeout{timeout = T}) ->
    #{action => set_timeout, timeout => T}.


%% Checks if the given PID is waiting for a lock
-spec is_lock_process(pid(), state()) -> boolean().
is_lock_process(Pid, State) ->
    maps:is_key(Pid, State#state.by_pid).

%%------------------------------------------------------------------------------------------------------------------------
%% Internal functions


% removes all locks requests from SendingDC with the given locks
-spec remove_acked_requests(dcid(), antidote_locks:lock_spec(), [remote_request()]) -> [remote_request()].
remove_acked_requests(SendingDc, Locks, LockRequests) ->
    lists:filter(fun(#remote_request{requester = RequesterDc, lock_item = {L, K}}) ->
        not (RequesterDc == SendingDc
            andalso (ordsets:is_element({L, K}, Locks)
                orelse K == shared andalso ordsets:is_element({L, exclusive}, Locks)))
    end, LockRequests).

%% Adds a new process to the state.
%% The process is initially in the waiting state for all locks.
-spec add_process(requester(), integer(), antidote_locks:lock_spec(), state()) -> state().
add_process(Requester, RequestTime, Locks, State) ->
    {Pid, _} = Requester,
    State#state{
        by_pid = maps:put(Pid, #pid_state{
            locks = [{Lock, {waiting, Kind}} || {Lock, Kind} <- Locks],
            request_time = RequestTime,
            requester = Requester
        }, State#state.by_pid)
    }.


% calculates for which data centers there are still missing locks
-spec missing_locks(list(dcid()), dcid(), [{antidote_locks:lock_spec_item(), antidote_lock_server:lock_crdt_value()}], antidote_locks:lock_spec()) -> antidote_locks:lock_spec().
missing_locks(AllDcIds, MyDcId, Locks, LocksInTransfer) ->
    ordsets:from_list([{L, K} || {{L, K}, LV} <- Locks, not owns_lock(AllDcIds, MyDcId, K, LV) orelse has_conflict({L, K}, LocksInTransfer)]).


has_conflict(LK, LKs) ->
    lists:any(fun(LK2) -> has_conflict1(LK, LK2) end, LKs).

has_conflict1({L, K1}, {L, K2}) ->
    K1 == exclusive orelse K2 == exclusive;
has_conflict1(_, _) ->
    false.

% checks if we own the given lock
-spec owns_lock(list(dcid()), dcid(), antidote_locks:lock_kind(), antidote_lock_server:lock_crdt_value()) -> boolean().
owns_lock(AllDcIds, MyDcId, Kind, LockValue) ->
    case Kind of
        shared ->
            maps:get(MyDcId, LockValue, MyDcId) == MyDcId;
        exclusive ->
            lists:all(fun(Dc) -> maps:get(Dc, LockValue, Dc) == MyDcId end, AllDcIds)
    end.


% sets the given locks to the waiting_remote state
-spec set_lock_waiting_state(pid(), ordsets:ordset(antidote_locks:lock()), state(), lock_state(), lock_state()) -> state().
set_lock_waiting_state(Pid, Locks, State, OldS, NewS) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
            S#pid_state{
                locks = set_lock_waiting_remote_list(Locks, S#pid_state.locks, OldS, NewS)
            }
        end, State#state.by_pid)
    }.

-spec set_lock_waiting_remote_list(ordsets:ordset(antidote_locks:lock()), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), lock_state(), lock_state()) -> orddict:orddict(antidote_locks:lock(), lock_state_with_kind()).
set_lock_waiting_remote_list(LocksToChange, Locks, OldS, NewS) ->
    [{L, case S == OldS andalso ordsets:is_element(L, LocksToChange) of
        true -> {NewS, K};
        false -> {S, K}
    end} || {L, {S, K}} <- Locks].


% Updates the lock state according to the new information about lock values.
% If all locks are received, state is changed from waiting_remote to waiting
-spec update_waiting_remote([dcid()], #{antidote_locks:lock() => antidote_lock_crdt:value()}, state()) -> {actions(), state()}.
update_waiting_remote(AllDcs, LockValues, State) ->
    MyDcId = my_dc_id(State),
    AcquiredLocks = get_acquired_locks(LockValues, MyDcId, AllDcs),
    State2 = State#state{
        by_pid = maps:map(fun(_Pid, S) ->
            S#pid_state{
                locks = update_waiting_remote_list(AllDcs, MyDcId, LockValues, S#pid_state.locks, State#state.locks_in_transfer)
            }
        end, State#state.by_pid)
    },
    Actions = [
        #send_inter_dc_message{receiver = D, message = #ack_locks{locks = AcquiredLocks}}
        || AcquiredLocks /= [], D <- other_dcs(State)
    ],
    {Actions, State2}.


-spec update_waiting_remote_list([dcid()], dcid(), #{antidote_locks:lock() => antidote_lock_crdt:value()}, orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), antidote_locks:lock_spec()) -> orddict:orddict(antidote_locks:lock(), lock_state_with_kind()).
update_waiting_remote_list(AllDcs, MyDcId, LockValues, Locks, LocksInTransfer) ->
    [{L, case maps:find(L, LockValues) of
        {ok, Value} ->
            NewS = case owns_lock(AllDcs, MyDcId, K, Value) and not has_conflict({L, K}, LocksInTransfer) of
                true ->
                    case S of
                        waiting_remote -> waiting;
                        _ -> S
                    end;
                _ -> waiting_remote
            end,

            {NewS, K};
        error -> {S, K}
    end} || {L, {S, K}} <- Locks].


% determines the next actions to perform
-spec next_actions(state(), integer()) -> {actions(), state()}.
next_actions(State, CurrentTime) ->
    {Actions1, State2} = handle_remote_requests(State, CurrentTime),
    {Actions2, State3} = handle_local_requests(State2, CurrentTime),
    {Actions1 ++ Actions2, State3}.


-spec handle_local_requests(state(), milliseconds()) -> {actions(), state()}.
handle_local_requests(State, CurrentTime) ->
    PidStates = State#state.by_pid,
    % locks currently held:
    HeldLocks = ordsets:from_list(
        lists:flatmap(fun(PS) ->
            lists:flatmap(fun
                ({L, {held, K}}) -> [{L, K}];
                (_) -> []
            end, PS#pid_state.locks)
        end, maps:values(PidStates))
    ),

    % requests without waiting-remote or held state (all waiting)
    A = maps:filter(fun(_Pid, PS) ->
        lists:all(fun({_L, {S, _K}}) -> S == waiting end, PS#pid_state.locks)
    end, PidStates),
    % requests not waiting for held locks
    B = maps:filter(fun(_Pid, PS) ->
        lists:all(fun({L, {_, Kind}}) ->
            not ordsets:is_element({L, exclusive}, HeldLocks)
                andalso (Kind == shared orelse not ordsets:is_element({L, shared}, HeldLocks))
        end, PS#pid_state.locks)
    end, A),


    % for each lock in A the minimum request time {time, pid}
    AMinRequestTime = min_request_times(A, shared),
    AMinRequestTimeExclusive = min_request_times(A, exclusive),

    AMinRequestTimeExclusive =
        antidote_list_utils:group_by(
            fun({L, _T}) -> L end,
            fun({_L, T}) -> T end,
            fun({_L, T1}, T2) -> min(T1, T2) end,
            lists:flatmap(fun({Pid, PS}) ->
                lists:flatmap(fun
                    ({L, {_, exclusive}}) ->
                        [{L, {PS#pid_state.request_time, Pid}}];
                    (_) ->
                        []
                end, PS#pid_state.locks)
            end, maps:to_list(A))),

    % remove requests where another process from A also wants the same lock
    % and has a smaller request time
    C = maps:filter(fun(Pid, PS) ->
        lists:all(fun
            ({L, {_, shared}}) ->
                maps:get(L, AMinRequestTimeExclusive, [infty]) >= {PS#pid_state.request_time, Pid};
            ({L, {_, exclusive}}) ->
                maps:get(L, AMinRequestTime) >= {PS#pid_state.request_time, Pid}
        end, PS#pid_state.locks)
    end, B),


    % acquire all locks from C
    Actions = [#accept_request{requester = P#pid_state.requester, clock = State#state.snapshot_time} || P <- maps:values(C)],


    CHeld = maps:map(fun(_Pid, PS) ->
        PS#pid_state{
            locks = lists:map(fun({L, {_, K}}) -> {L, {held, K}} end, PS#pid_state.locks)
        }
    end, C),

    State2 = State#state{
        by_pid = maps:merge(State#state.by_pid, CHeld)
    },
    State3 = maps:fold(fun(_Pid, PS, Acc) ->
        set_lock_acquired_time(CurrentTime, PS#pid_state.locks, Acc)
    end, State2, CHeld),

    {Actions, State3}.


%% Calculates the minimum request time for each lock
%% The time is tupled with pid to get a total order
%% Only includes locks with lock kind >= LockKind
-spec min_request_times(#{pid() => pid_state()}, antidote_locks:lock_kind()) -> #{antidote_locks:lock() => {milliseconds(), pid()}}.
min_request_times(A, LockKind) ->
    antidote_list_utils:group_by(
        fun({L, _T}) -> L end,
        fun({_L, T}) -> T end,
        fun({_L, T1}, T2) -> min(T1, T2) end,
        lists:flatmap(fun({Pid, PS}) ->
            lists:flatmap(fun({L, {_, K}}) ->
                case max_lock_kind(LockKind, K) == K of
                    true ->
                        [{L, {PS#pid_state.request_time, Pid}}];
                    false ->
                        []
                end
            end, PS#pid_state.locks)
        end, maps:to_list(A))).

-spec handle_remote_requests(state(), milliseconds()) -> {actions(), state()}.
handle_remote_requests(State, CurrentTime) ->
    case State#state.locks_in_transfer == [] of
        false ->
            % if we are already transferring locks, do not start a new transfer
            {[], State};
        true ->
            % Select the locks which are eligible to be sent to other DC:
            LocksWaitTime = lists:filtermap(fun(Req) ->
                T = Req#remote_request.request_time,
                {L, K} = Req#remote_request.lock_item,
                Dc = Req#remote_request.requester,
                NoConflicts =
                    % there is no conflicting local request with a smaller time
                    not exists_local_request(L, State#state.dc_id, T, Dc, K, State)
                    % there is no conflicting remote request with a smaller time
                    andalso not exists_conflicting_remote_request(Dc, L, K, T, State),
                % request not already answered
                AlreadyAnswered = orddict:is_key(Req, State#state.answered_remote_requests),

                case NoConflicts andalso not AlreadyAnswered of
                    true ->
                        WaitTime = max(
                            T - CurrentTime,
                            min(
                                maps:get(L, State#state.last_used, 0) + State#state.min_exclusive_lock_duration - CurrentTime,
                                maps:get(L, State#state.time_acquired, 0) + State#state.max_lock_hold_duration - CurrentTime)),
                        {true, {Req, WaitTime}};
                    false ->
                        false
                end

            end, State#state.remote_requests),

            LocksToTransfer = lists:filtermap(fun({Req, T}) ->
                case T =< 0 of
                    true -> {true, Req};
                    false -> false
                end
                end, LocksWaitTime),

            TickTime = lists:min([infty | [T || {_, T} <- LocksWaitTime, T > 0]]),
            TimeoutActions = [#set_timeout{timeout = TickTime} || TickTime /= infty],


            LocksToTransferLSet = ordsets:from_list([LK || #remote_request{lock_item = LK} <- LocksToTransfer]),
            LockObjects = lists:usort(antidote_lock_crdt:get_lock_objects([L || {L, _K} <- LocksToTransferLSet])),


            LockSpec = ordsets:from_list(lists:map(fun(R) -> R#remote_request.lock_item end, LocksToTransfer)),

            % change local requests to waiting_remote
            {RequestAgainLocal, State2} = change_waiting_locks_to_remote(LockSpec, State),

            Actions = [
                #read_crdt_state{snapshot_time = State#state.snapshot_time, objects = LockObjects
                    , data = #handle_remote_requests_cont{
                        locks_to_transfer = LocksToTransfer
                    }}
                || LocksToTransfer /= []
            ] ++ RequestAgainLocal
                ++ TimeoutActions,


            State3 = State2#state{
                answered_remote_requests = orddict:merge(
                    fun(_K, V1, V2) -> vectorclock:max([V1, V2]) end,
                    State2#state.answered_remote_requests,
                    orddict:from_list([{K, State#state.snapshot_time} || K <- LocksToTransfer])
                ),
                locks_in_transfer = LocksToTransferLSet,
                time_acquired = lists:foldl(
                    fun({L, _K}, TA) -> maps:remove(L, TA) end,
                    State2#state.time_acquired,
                    LocksToTransferLSet)
            },
            {Actions, State3}
    end.

exists_conflicting_remote_request(Dc, L, K, T, State) ->
    lists:any(fun(#remote_request{lock_item = {L2, K2}, requester = Dc2, request_time = T2}) ->
        L == L2
            andalso Dc /= Dc2
            andalso not (K == shared andalso K2 == shared)
            andalso {T2, Dc2} =< {T, Dc}
    end, State#state.remote_requests).


% checks if there is a conflicting local request for the given lock
% with time <= T or already held
-spec exists_local_request(antidote_locks:lock(), dcid(), milliseconds(), dcid(), antidote_locks:lock_kind(), state()) -> boolean().
exists_local_request(Lock, MyDcId, T, OtherDcId, Kind, State) ->
    lists:any(
        fun(S) ->
            TimeBefore = {S#pid_state.request_time, MyDcId} =< {T, OtherDcId},
            case orddict:find(Lock, S#pid_state.locks) of
                {ok, {St, shared}} -> Kind == exclusive andalso (St == held orelse TimeBefore);
                {ok, {St, exclusive}} -> St == held orelse TimeBefore;
                error -> false
            end
        end,
        maps:values(State#state.by_pid)
    ).




-spec set_lock_acquired_time(milliseconds(), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), state()) -> state().
set_lock_acquired_time(CurrentTime, PidStatePidStateLocks, State) ->
    EL = orddict:fold(
        fun
            (Key, {_, exclusive}, M) ->
                maps:update_with(Key, fun(X) -> min(X, CurrentTime) end, CurrentTime, M);
            (_, _, M) -> M
        end, State#state.time_acquired, PidStatePidStateLocks),
    State#state{time_acquired = EL}.

-spec update_last_used(milliseconds(), pid_state(), #{antidote_locks:lock() => milliseconds()}) -> #{antidote_locks:lock() => milliseconds()}.
update_last_used(CurrentTime, PidState, LastUsed) ->
    orddict:fold(fun(L, _, Acc) ->
        maps:put(L, CurrentTime, Acc)
    end, LastUsed, PidState#pid_state.locks).

%%-spec remove_lock_acquired_time(orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), state()) -> state().
%%remove_lock_acquired_time(PidStatePidStateLocks, State) ->
%%    EL = orddict:fold(
%%        fun(Key, _, M) ->
%%            maps:remove(Key, M)
%%        end, State#state.exclusive_locks, PidStatePidStateLocks),
%%    State#state{exclusive_locks = EL}.

-spec get_remote_waiting_locks(state()) -> ordsets:ordset(antidote_locks:lock()).
get_remote_waiting_locks(State) ->
    PidStates = maps:values(State#state.by_pid),
    Locks = lists:flatmap(fun(P) -> [L || {L, {waiting_remote, _}} <- P#pid_state.locks] end, PidStates),
    ordsets:from_list(Locks).


% changes lock state from waiting to waiting_remote for all entries conflicting with the given locks
% Produces interdc request actions for the locks that are now waiting
-spec change_waiting_locks_to_remote(antidote_locks:lock_spec(), state()) -> {actions(), state()}.
change_waiting_locks_to_remote(Locks, State) ->
    NewPyPid = maps:map(fun(_Pid, PidState) ->
        PidState#pid_state{
            locks = [
                case LockState == waiting
                    andalso (lists:member({Lock, exclusive}, Locks)
                        orelse LockKind == exclusive andalso lists:member({Lock, shared}, Locks))
                of
                    true -> {Lock, {waiting_remote, LockKind}};
                    false -> L
                end || L = {Lock, {LockState, LockKind}} <- PidState#pid_state.locks]
        }
    end, State#state.by_pid),
    NewState = State#state{
        by_pid = NewPyPid
    },
    % collect locks that changed from waiting to waiting_remote:
    Changes = ordsets:from_list([
        {Pid, NewPidState#pid_state.request_time, LockChanges}
        || {Pid, NewPidState} <- maps:to_list(NewPyPid),
        OldPidState <- [maps:get(Pid, State#state.by_pid)],
        (LockChanges = [{L, Kind} ||
            {{L, {waiting, Kind}}, {L, {waiting_remote, Kind}}} <- lists:zip(OldPidState#pid_state.locks, NewPidState#pid_state.locks)]) /= []
    ]),
    Actions = [
        #send_inter_dc_message{
            receiver = Dc, message = #lock_request{locks = LockChanges, request_time = T, requester_pid = Pid}}
        || Dc <- other_dcs(State), {Pid, T, LockChanges} <- Changes
    ],
    {Actions, NewState}.


%%% computes lock request actions for the locally waiting locks
%%% assuming we send the given locks to other data centers
%%-spec get_local_waiting_requests(lock_request_actions(), state()) -> lock_request_actions().
%%get_local_waiting_requests(LocksToTransferL, State) ->
%%    lists:flatmap(fun({_Dc, {_Pid, _T, {Lock, LockKind}}}) ->
%%        lists:filter(fun(PS) ->
%%            case orddict:find(Lock, PS#pid_state.locks) of
%%                {ok, {_S, K}} when not (LockKind == shared andalso K == shared) ->
%%                    true;
%%                _ ->
%%                    false
%%            end
%%        end, maps:values(State#state.by_pid))
%%    end, LocksToTransferL).


%
%%-spec filter_waited_for_locks([antidote_locks:lock()], state()) -> lock_request_actions_for_dc().
%%filter_waited_for_locks(Locks, State) ->
%%    [{L, {Kind, MinTime}} ||
%%        L <- Locks,
%%        (Kind = max_lock_waiting_kind(L, State)) /= none,
%%        (MinTime = min_lock_request_time(L, State)) < infty].

%%% returns the minimum time of a local request waiting for this lock
%%-spec min_lock_request_time(antidote_locks:lock(), state()) -> integer() | infty.
%%min_lock_request_time(Lock, State) ->
%%    lists:min([infty] ++ [PidState#pid_state.request_time ||
%%        PidState <- maps:values(State#state.by_pid),
%%        {L, {waiting, _}} <- PidState#pid_state.locks,
%%        L == Lock]).


%%% goes through all the processes waiting for the given lock and returns the maximum required lock level
%%-spec max_lock_waiting_kind(antidote_locks:lock(), state()) -> shared | exclusive | none.
%%max_lock_waiting_kind(Lock, State) ->
%%    max_lock_waiting_kind_iter(Lock, maps:iterator(State#state.by_pid), none).

%%max_lock_waiting_kind_iter(Lock, Iter, Res) ->
%%    case maps:next(Iter) of
%%        none -> Res;
%%        {_Pid, PidState, NextIterator} ->
%%            case orddict:find(Lock, PidState#pid_state.locks) of
%%                {ok, {waiting, LockKind}} ->
%%                    % we can return the first entry we find, since the two kinds exclude each other
%%                    max_lock_waiting_kind_iter(Lock, NextIterator, max_lock_kind(LockKind, Res));
%%                _ ->
%%                    max_lock_waiting_kind_iter(Lock, NextIterator, Res)
%%            end
%%    end.


max_lock_kind(Ls) ->
    lists:foldl(fun max_lock_kind/2, none, Ls).

-spec max_lock_kind(shared | exclusive | none, shared | exclusive | none) -> shared | exclusive | none.
max_lock_kind(A, none) -> A;
max_lock_kind(none, A) -> A;
max_lock_kind(exclusive, _) -> exclusive;
max_lock_kind(_, exclusive) -> exclusive;
max_lock_kind(shared, shared) -> shared.


-spec set_snapshot_time(snapshot_time(), state()) -> state().
set_snapshot_time(Time, State) ->
    State#state{snapshot_time = Time}.



-spec merge_snapshot_time(snapshot_time(), snapshot_time()) -> snapshot_time().
merge_snapshot_time(V1, V2) -> vectorclock:max([V1, V2]).




-spec get_acquired_locks(#{antidote_locks:lock() => antidote_lock_crdt:value()}, dcid(), [dcid()]) -> antidote_locks:lock_spec().
get_acquired_locks(LockValues, MyDcId, AllDcs) ->
    ordsets:from_list([{L, K} || {L, LV} <- maps:to_list(LockValues), (K = lock_level(LV, MyDcId, AllDcs)) /= none]).


-spec lock_level(antidote_lock_crdt:value(), dcid(), [dcid()]) -> antidote_locks:lock_kind() | none.
lock_level(LockValue, MyDcId, AllDcIds) ->
    case lists:all(fun(Dc) -> maps:get(Dc, LockValue, Dc) == MyDcId end, AllDcIds) of
        true -> exclusive;
        false ->
            case maps:get(MyDcId, LockValue, MyDcId) == MyDcId of
                true -> shared;
                false -> none
            end
    end.

debug_log(Term) ->
    Log = case disk_log:open([{name, antidote_lock_server}]) of
        {ok, L} -> L;
        {repaired, L, _, _} -> L
    end,
    ok = disk_log:log(Log, {erlang:system_time(millisecond), Term}),
    ok = disk_log:close(Log).

debug_result({Actions, State}) ->
    debug_log({actions, print_actions(Actions)}),
    debug_log({state, print_state(State)}),
    {Actions, State}.


-spec other_dcs(state()) -> [dcid()].
other_dcs(State) ->
    State#state.all_dc_ids -- [State#state.dc_id].



-ifdef(TEST).
% run with: rebar3 eunit --module=antidote_lock_server_state

initial(Dc) ->
    initial(Dc, [dc1, dc2, dc3], 0, 0, 1).

max_lock_kind_test() ->
    ?assertEqual(none, max_lock_kind(none, none)),
    ?assertEqual(shared, max_lock_kind(none, shared)),
    ?assertEqual(exclusive, max_lock_kind(none, exclusive)),
    ?assertEqual(shared, max_lock_kind(shared, none)),
    ?assertEqual(shared, max_lock_kind(shared, shared)),
    ?assertEqual(exclusive, max_lock_kind(shared, exclusive)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, none)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, shared)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, exclusive)),
    ok.


unparse_lock_values(Vs) ->
    [unparse_lock_value(V) || V <- Vs].

unparse_lock_value(M) ->
    [{{K, antidote_crdt_register_mv}, [V]} || {K, V} <- maps:to_list(M)].


new_test_request(Requester, RequestTime, SnapshotTime, LockSpecWithLockValues, State) ->
    {LockSpec, LockValues} = lists:unzip(LockSpecWithLockValues),
    {Actions1, State1} = new_request(Requester, RequestTime, SnapshotTime, LockSpec, State),
    [#read_crdt_state{data = Data}] = Actions1,
    on_read_crdt_state(RequestTime, Data, SnapshotTime, unparse_lock_values(LockValues), State1).

% 99, #{}, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1
on_remote_locks_received(Time, RequesterPid, SendingDc, SnapshotTime, LockSpecWithLockValues, State) ->
    {LockSpec, LockValues} = lists:unzip(LockSpecWithLockValues),
    {Actions1, State1} = on_receive_inter_dc_message(Time, SendingDc, #locks_transferred{
        snapshot_time = SnapshotTime,
        locks = [{RequesterPid, L} || L <- LockSpec]
    }, State),
    [#read_crdt_state{data = Data}] = Actions1,
    on_read_crdt_state(Time, Data, SnapshotTime, unparse_lock_values(LockValues), State1).




shared_lock_ok_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_test_request(R1, 10, #{}, [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], S1),
    ?assertEqual([#accept_request{requester = R1, clock = #{}}], Actions),
    ok.



shared_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_test_request(R1, 10, undefined, [{{lock1, shared}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}}], S1),
    Msg = #lock_request{requester_pid = p1, request_time = 11, locks = [{lock1, shared}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions),
    ok.

shared_lock_fail2_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_test_request(R1, 10, undefined, [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], S1),
    Msg = #lock_request{requester_pid = p1, request_time = 11, locks = [{lock1, shared}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions),
    ok.




shared_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions1, S1} = new_test_request(R1, 10, undefined, [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], SInit),
    Msg = #lock_request{requester_pid = p1, request_time = 11, locks = [{lock1, shared}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions1),

    % later we receive the lock from dc3 and we can get the lock
    {Actions2, _S2} = on_remote_locks_received(99, p1, dc3, #{}, [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual([
        #accept_request{requester = R1, clock = #{}},
        #send_inter_dc_message{receiver = dc2, message = #ack_locks{locks = [{lock1, shared}]}},
        #send_inter_dc_message{receiver = dc3, message = #ack_locks{locks = [{lock1, shared}]}}
    ], lists:sort(Actions2)),
    ok.




exclusive_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data centers 2 and 3 have the locks, so we need to request it
    {Actions1, S1} = new_test_request(R1, 10, undefined, [{{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}], SInit),
    Msg = #lock_request{requester_pid = p1, request_time = 11, locks = [{lock1, exclusive}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions1),
%%    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock1} => {exclusive, 11}}, dc3 => #{{dc1, lock1} => {exclusive, 11}}}}, Actions1),

    % later we receive the lock from dc2, which is not enough to acquire the lock
    {Actions2, S2} = on_remote_locks_received(99, p1, dc2, #{}, [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual([], Actions2),
%%    ?assertEqual(#actions{}, Actions2),

    % when we have all the locks, we can acquire the lock
    {Actions3, _S3} = on_remote_locks_received(99, p1, dc3, #{}, [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S2),
    ?assertEqual([
        #accept_request{requester = R1, clock = #{}},
        #send_inter_dc_message{receiver = dc2, message = #ack_locks{locks = [{lock1, exclusive}]}},
        #send_inter_dc_message{receiver = dc3, message = #ack_locks{locks = [{lock1, exclusive}]}}
    ], lists:sort(Actions3)),
%%    ?assertEqual(#actions{replies = [R1], ack_locks = [{lock1, exclusive}]}, Actions3),
    ok.



exclusive_lock_missing_local2_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data centers 2 and 3 have the locks, so we need to request it
    Locks = [
        {{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}},
        {{lock2, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}
    ],
    {Actions1, S1} = new_test_request(R1, 10, #{}, Locks, SInit),
    Msg = #lock_request{requester_pid = p1, request_time = 11, locks = [{lock1, exclusive}, {lock2, exclusive}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions1),
    ?assertEqual([lock1, lock2], get_remote_waiting_locks(S1)),

    % first we only get lock 2
    {Actions2, S2} = on_remote_locks_received(99, p1, dc2, #{}, [{{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = #ack_locks{locks = [{lock2, exclusive}]}},
        #send_inter_dc_message{receiver = dc3, message = #ack_locks{locks = [{lock2, exclusive}]}}
    ], Actions2),
    ?assertEqual([lock1], get_remote_waiting_locks(S2)),

    % when we have all the locks, we can acquire the lock
    ReceivedLocks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}],
    {Actions3, _S3} = on_remote_locks_received(99, p1, dc3, #{}, ReceivedLocks, S2),
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = #ack_locks{locks = [{lock1, exclusive}, {lock2, exclusive}]}},
        #send_inter_dc_message{receiver = dc3, message = #ack_locks{locks = [{lock1, exclusive}, {lock2, exclusive}]}},
        #accept_request{requester = R1, clock = #{}}
    ], Actions3),
    ok.


on_read_crdt_state_test() ->
    {Actions, _State} = on_read_crdt_state(24,
        #handle_remote_requests_cont{
            locks_to_transfer = [{remote_request, r1, 2, 5, {lock2, exclusive}},
                {remote_request, r1, 4, 5, {lock1, exclusive}},
                {remote_request, r1, 4, 5, {lock2, exclusive}}]},
        #{},
        [[], []],
        #state{
            dc_id = r2,
            all_dc_ids = [r1, r2, r3],
            snapshot_time = #{},
            by_pid = #{1 => {pid_state, [{lock2, {waiting_remote, exclusive}}, {lock3, {waiting, shared}}], 5, {1, tag}}},
            remote_requests = [{remote_request, r1, 2, 5, {lock2, exclusive}}, {remote_request, r1, 4, 5, {lock1, exclusive}}, {remote_request, r1, 4, 5, {lock2, exclusive}}, {remote_request, r3, 3, 5, {lock3, exclusive}}],
            answered_remote_requests = [
                {{remote_request, r1, 2, 5, {lock2, exclusive}}, #{}},
                {{remote_request, r1, 4, 5, {lock1, exclusive}}, #{}},
                {{remote_request, r1, 4, 5, {lock2, exclusive}}, #{}}],
            timer_Active = false,
            time_acquired = #{},
            last_used = #{},
            min_exclusive_lock_duration = 10,
            max_lock_hold_duration = 100,
            remote_request_delay = 5}),
    [#update_crdt_state{updates = Updates}] = Actions,
    Objects = [O || {O, _, _} <- Updates],

    ?assertEqual([], Objects -- lists:usort(Objects)),
    ok.


change_waiting_locks_to_remote_test() ->
    SInit = initial(dc1),

    R1 = {p1, t1},

    % assume we have all the locks except for lock5:
    Locks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock3, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock4, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock5, exclusive}, #{dc1 => dc3, dc2 => dc3, dc3 => dc3}}
    ],
    {Actions1, S1} = new_test_request(R1, 210, #{}, Locks, SInit),
    Msg = #lock_request{requester_pid = p1, request_time = 211, locks = [{lock5, exclusive}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions1),
    ?assertEqual([lock5], get_remote_waiting_locks(S1)),

    % receive remote request from dc2
    {_Actions2, S2} = on_receive_inter_dc_message(211, dc2, #lock_request{locks = [{lock1, shared}, {lock2, exclusive}, {lock3, shared}, {lock4, exclusive}], request_time = 0, requester_pid = p2}, S1),
    % everything except for lock1 (Shared-shared) should be changed to waiting-remote
    ?assertEqual([lock2, lock3, lock4, lock5], get_remote_waiting_locks(S2)),


    ok.

has_conflict_test() ->
    InTransfer = [{lock1, shared}, {lock2, exclusive}, {lock3, shared}, {lock4, exclusive}],
    ?assertEqual(false, has_conflict({lock1, shared}, InTransfer)),
    ?assertEqual(true, has_conflict({lock2, shared}, InTransfer)),
    ?assertEqual(true, has_conflict({lock3, exclusive}, InTransfer)),
    ?assertEqual(true, has_conflict({lock4, exclusive}, InTransfer)),
    ?assertEqual(false, has_conflict({lock5, exclusive}, InTransfer)),
    ok.

request_lock_in_transfer_test() ->
    SInit = initial(dc1),

    % receive remote request from dc2
    {_Actions1, S1} = on_receive_inter_dc_message(211, dc2, #lock_request{locks = [{lock1, shared}, {lock2, exclusive}, {lock3, shared}, {lock4, exclusive}], request_time = 0, requester_pid = p2}, SInit),


    R1 = {p1, t1},

    % assume we have all the locks except for lock5:
    Locks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock3, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock4, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock5, exclusive}, #{dc1 => dc3, dc2 => dc3, dc3 => dc3}}
    ],
    {Actions2, S2} = new_test_request(R1, 210, #{}, Locks, S1),

    % everything except for lock1 (Shared-shared) should be changed to waiting-remote
    ?assertEqual([lock2, lock3, lock4, lock5], get_remote_waiting_locks(S2)),

    Msg = #lock_request{requester_pid = p1, request_time = 211, locks = [{lock2, shared}, {lock3, exclusive}, {lock4, exclusive}, {lock5, exclusive}]},
    ?assertEqual([
        #send_inter_dc_message{receiver = dc2, message = Msg},
        #send_inter_dc_message{receiver = dc3, message = Msg}
    ], Actions2),


    ok.


remote_request_then_transfer_test() ->
    SInit = initial(dc1),

    % receive remote request from dc2
    {Actions1, S1} = on_receive_inter_dc_message(0, dc2, #lock_request{locks = [{lock1, exclusive}], request_time = 0, requester_pid = p2}, SInit),
    % then we need to read lock1
    [#read_crdt_state{data = Data}] = Actions1,
    {Actions2, S2} = on_read_crdt_state(1, Data, #{}, unparse_lock_values([#{dc1 => dc1, dc2 => dc1, dc3 => dc3}]), S1),
    % send all locks we have so far
    ExpectedUpdates = antidote_lock_crdt:make_lock_updates(lock1, [{dc1, dc2}, {dc2, dc2}]),
    [#update_crdt_state{data = Data2, updates = ExpectedUpdates}] = Actions2,

    {Actions3, S3} = on_complete_crdt_update(2, Data2, #{}, S2),
    [#send_inter_dc_message{message = #locks_transferred{}}] = Actions3,

    % next, we get one more missing lock
    {Actions4, S4} = on_receive_inter_dc_message(3, dc3, #locks_transferred{snapshot_time = #{}, locks = [{p1, {lock1, exclusive}}]}, S3),
    [#read_crdt_state{data = Data4}] = Actions4,


    {Actions5, S5} = on_read_crdt_state(4, Data4, #{dc1 => 1}, unparse_lock_values([#{dc1 => dc2, dc2 => dc2, dc3 => dc1}]), S4),
    [#read_crdt_state{data = Data5}] = Actions5,

    {Actions6, _S6} = on_read_crdt_state(5, Data5, #{}, unparse_lock_values([#{dc1 => dc2, dc2 => dc2, dc3 => dc1}]), S5),

    ExpectedUpdates6 = antidote_lock_crdt:make_lock_updates(lock1, [{dc3, dc2}]),
    [#update_crdt_state{updates = ExpectedUpdates6}] = Actions6,

    ok.

-endif.

