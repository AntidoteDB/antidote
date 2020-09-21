-module(lock_server_state_SUITE).
-include_lib("antidote/include/antidote.hrl").

% run with:
% rebar3 ct --suite lock_server_state_SUITE --dir test/singledc

-export([all/0, explore/1, suite/0, explore/0, many_requests/1]).

all() -> [
    explore,
    many_requests
].

suite() ->
    [{timetrap, {minutes, 150}}].

-record(pid_state, {
    spec :: antidote_locks:lock_spec(),
    requested_time :: integer(),
    status :: waiting | held
}).

-record(replica_state, {
    lock_server_state :: antidote_lock_server_state:state(),
    pid_states = #{} :: #{pid() => #pid_state{}},
    time = 0 :: integer(),
    snapshot_time = #{} :: snapshot_time()
}).

-type future_action() :: {Time :: integer(), Dc :: dcid(), Action :: antidote_lock_server_state:action()}.

-record(state, {
    replica_states :: #{dcid() => #replica_state{}},
    time = 0 :: integer(),
    future_actions = [] :: [future_action()],
    max_pid = 0 :: integer(),
    crdt_effects = [] :: [{snapshot_time(), [{bound_object(), antidote_crdt:effect()}]}],
    cmds = [] :: [any()]
}).


explore() ->
    [{timetrap, {minutes, 150}}].


explore(_Config) ->
    dorer:check(#{max_shrink_time => {1000, second}, n => 100}, fun() ->
        State1 = my_run_commands(initial_state()),
        State = complete_run(State1),
        log_commands(State),
        dorer:log("Final State: ~n ~p", [print_state(State)]),
        check_liveness(lists:reverse(State#state.cmds))
    end).

many_requests(_Config) ->
    N = 100,
    State1 = initial_state(100, 500, 50),
    InitialRequests = [{request, list_to_atom("pid_" ++ integer_to_list(P) ++ "_" ++ atom_to_list(R)), R, [{lock1, exclusive}]}
        || P <- lists:seq(1, 1), R <- replicas()],
    State2 = lists:foldl(fun(Cmd = {request, _Pid, _R, _}, Acc) ->
        io:format("Time ~p: RUN ~w~n", [Acc#state.time, Cmd]),
        Acc2 = next_state(Acc, Cmd),
        Acc2#state{time = Acc2#state.time + 1}
    end, State1, InitialRequests),


    % should take less than 100ms per request on average
    FinalState = run_many_requests(State2, N),
    case FinalState#state.time / N < 100 of
        true -> ok;
        false -> throw({took_too_long, FinalState#state.time / N})
    end.

run_many_requests(State, N) ->
    AcceptedRequests = length([ok || {action, {_, _, {accept_request, _, _}}} <- State#state.cmds]),
    case AcceptedRequests > N of
        true -> State;
        false ->

            case length(State#state.cmds) < 5000 of
                true -> ok;
                false ->
                    log_commands(State),
                    throw('run_many_requests: probably contains looping actions')
            end,
            NextCommands = begin
                FutureActions1 = filter_future_actions(State#state.future_actions),
                % some actions need more time
                FutureActions = lists:filter(fun
                    ({T, _, {release_locks, _}}) ->
                        % 50ms until locks are released:
                        T + 50 < State#state.time;
                    ({T, R, {read_crdt_state, SnapshotTime, _Objects, _Data}}) ->
                        Delay = case vectorclock:le(SnapshotTime, (maps:get(R, State#state.replica_states))#replica_state.snapshot_time) of
                            true -> 1;
                            false ->
                                % 100ms to read lock values when we need to wait for VC
                                100
                        end,
                        T + Delay < State#state.time;
                    ({T, _, _}) -> T + 1 < State#state.time;
                    (_) -> true
                end, FutureActions1),
                if
                    % execute actions as soon as possible
                    FutureActions /= [] ->
                        [{action, hd(FutureActions)}];
                    true ->
                        []
                end
            end,



            NextState = lists:foldl(fun(Cmd, Acc) ->
                Acc2 = next_state_l(Acc, Cmd),
                case Cmd of
                    {action, {_, Dc, {release_locks, Pid}}} ->
                        ct:pal("~p) release ~p at ~p", [Acc#state.time, AcceptedRequests, Dc]),
                        % add another request when lock was just released:
                        next_state_l(Acc2, {request, Pid, Dc, [{lock1, exclusive}]});
                    _ ->
                        Acc2

                end
            end, State, NextCommands),
            case State == NextState of
                true -> State;
                false -> run_many_requests(NextState, N)
            end
    end.




log_commands(State) ->
    lists:foreach(fun(Cmd) ->
        dorer:log("RUN ~w", [Cmd])
    end, lists:reverse(State#state.cmds)).

print_state(State) ->
    #{
        time => State#state.time,
        future_actions => State#state.future_actions,
        replica_states => maps:map(
            fun(_K, V) ->
                #{
                    lock_server_state => antidote_lock_server_state:print_state(V#replica_state.lock_server_state),
                    pid_states => V#replica_state.pid_states,
                    time => V#replica_state.time,
                    snapshot_time => V#replica_state.snapshot_time
                }
            end,
            State#state.replica_states),
        crdt_states => maps:map(fun(_K, V) ->
            Snapshot = V#replica_state.snapshot_time,
            Objects = [O || {_S, Effs} <- State#state.crdt_effects, {O, _Eff} <- Effs],
            CrdtStates = calculate_crdt_states(Snapshot, Objects, State),
            ReadResults = [{Key, antidote_crdt:value(Type, CrdtState)} || {{Key, Type, _}, CrdtState} <- CrdtStates],
            maps:from_list(ReadResults)
        end, State#state.replica_states)
    }.



my_run_commands(State) ->
    case length(State#state.cmds) < 1000 andalso dorer:gen(command, dorer_generators:has_more()) of
        false -> State;
        true ->
            case gen_command(State) of
                done -> State;
                Cmd ->
                    dorer:log("Time ~p: RUN ~w", [State#state.time, Cmd]),
                    NextState = next_state(State#state{cmds = [Cmd | State#state.cmds]}, Cmd),
                    dorer:log("State:~n ~p", [print_state(NextState)]),
                    check_invariant(NextState),
                    my_run_commands(NextState)
            end
    end.

complete_run(State) ->
    complete_run(State, State#state.time).

% completes the run by executing
complete_run(State, LastAction) ->
    case length(State#state.cmds) < 5000 of
        true -> ok;
        false ->
            log_commands(State),
            throw('complete_run: probably contains looping actions')
    end,
    case State#state.time < min(10000, LastAction + 500) of
        false -> State;
        true ->
            case complete_run_next_command(State) of
                done -> State;
                Cmd ->
                    dorer:log("_Time ~p: RUN ~w", [State#state.time, Cmd]),
                    NextState = next_state(State#state{cmds = [Cmd | State#state.cmds]}, Cmd),
                    dorer:log("_State:~n ~p", [print_state(NextState)]),
                    check_invariant(NextState),
                    LastAction2 = case Cmd of
                        {tick, _, _, _} -> LastAction;
                        _ -> State#state.time
                    end,
                    complete_run(NextState, LastAction2)
            end
    end.

complete_run_next_command(State) ->
    FutureActions = filter_future_actions(State#state.future_actions),
    if
    % execute actions as soon as possible
        FutureActions /= [] ->
            {action, hd(FutureActions)};
    % otherwise we are done
        true ->
            done
    end.


replicas() -> [r1, r2, r3].

replica() -> dorer_generators:no_shrink(dorer_generators:oneof(replicas())).

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    initial_state(10, 100, 5).

initial_state(MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay) ->
    #state{
        replica_states = maps:from_list([{R, initial_state_r(R, MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay)} || R <- replicas()])
    }.

initial_state_r(R, MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay) ->
    #replica_state{
        lock_server_state = antidote_lock_server_state:initial(R, replicas(), MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay)
    }.

gen_command(State) ->
    NeedsActions = needs_action(State),
    if
        NeedsActions /= [] ->
            hd(NeedsActions);
        true ->
            P1 = if
                State#state.time < 100 -> 20;
                true -> 0
            end,
            FutureActions = filter_future_actions(State#state.future_actions),
            if
                FutureActions == [] andalso P1 == 0 ->
                    done;
                true ->
                    dorer:gen([command, normal],
                        dorer_generators:frequency_gen([
                            {P1, {request, State#state.max_pid + 1, replica(), lock_spec()}},
                            {10 * length(FutureActions), dorer_generators:oneof([{action, A} || A <- FutureActions])}
                        ]))
            end
    end.

needs_action(State) ->
    FutureActions = filter_future_actions(State#state.future_actions),

    lists:map(fun(A) -> {action, A} end,
        lists:filter(fun({T, _Dc, _A}) ->
            T + 50 < State#state.time
        end, FutureActions)).

% only execute reads, if there is no update action planned for the same DC
-spec filter_future_actions([future_action()]) -> [future_action()].
filter_future_actions(Actions1) ->
    UpdatesBeforeReads = fun(Actions) ->
        lists:filter(fun
            ({_, Dc, {read_crdt_state, _SnapshotTime, _Objects, _Data}}) ->
                % check if there is no update scheduled on the same DC
                not lists:any(fun
                    ({_, Dc2, {update_crdt_state, _SnapshotTime2, _Updates, _Data2}}) ->
                        Dc == Dc2;
                    (_) ->
                        false
                end, Actions);
            (_) ->
                true
        end, Actions)
    end,
    InterDCFifo = fun
        InterDCFifo([]) -> [];
        InterDCFifo([A = {_, Sender, {send_inter_dc_message, Receiver, _}} | Rest]) ->
            [A | InterDCFifo(lists:filter(fun
                ({_, Sender2, {send_inter_dc_message, Receiver2, _}}) -> Sender /= Sender2 orelse Receiver /= Receiver2;
                (_) -> true
            end, Rest))]
        ;
        InterDCFifo([A | As]) -> [A | InterDCFifo(As)]
    end,
    UpdatesBeforeReads(InterDCFifo(Actions1)).



lock_spec() ->
    N = 3,
    Locks = [list_to_atom("lock" ++ integer_to_list(I)) || I <- lists:seq(1, N)],
    dorer_generators:transform(
        dorer_generators:map(dorer_generators:oneof(Locks), lock_level()),
        fun(M) ->
            case maps:to_list(M) of
                [] -> [{lock1, shared}];
                L -> L
            end
        end).


lock_level() ->
    dorer_generators:oneof([shared, exclusive]).



check_invariant(State) ->
    % TODO check should be stricter and also consider the versions assigned by the lock server
    HeldLocks = lists:flatmap(
        fun(R) ->
            [{{R, P}, {L, K}} ||
                {P, S} <- maps:to_list((maps:get(R, State#state.replica_states))#replica_state.pid_states),
                S#pid_state.status == held,
                {L, K} <- S#pid_state.spec]
        end,
        replicas()),
    lists:foreach(fun
        (Lock1 = {Id, {L, exclusive}}) ->
            lists:foreach(fun(Lock2 = {Id2, {L2, _K2}}) ->
                case Id2 /= Id andalso L == L2 of
                    false -> ok;
                    true ->
                        log_commands(State),
                        dorer:log("Invariant violation in state:~n ~p", [print_state(State)]),
                        throw({'Safety violation: Lock held by two processes: ', Lock1, Lock2})
                end

            end, HeldLocks);
        (_) ->
            ok
    end, HeldLocks).


next_state_l(State, Cmd) -> next_state(State#state{cmds = [Cmd | State#state.cmds]}, Cmd).

%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, {request, Pid, Replica, LockSpec}) ->
    Rs = maps:get(Replica, State#state.replica_states),
    Requester = {Pid, tag},
    {Actions, NewRs} = antidote_lock_server_state:new_request(Requester, State#state.time, Rs#replica_state.snapshot_time, LockSpec, Rs#replica_state.lock_server_state),
    State2 = State#state{
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            pid_states = maps:put(Pid, #pid_state{
                status = waiting,
                requested_time = State#state.time,
                spec = LockSpec
            }, Rs#replica_state.pid_states)
        }, State#state.replica_states),
        max_pid = max(Pid, State#state.max_pid)
    },
    State3 = add_actions(State2, Replica, Actions),
    State3;
next_state(State, {action, {T1, Dc1, Action1}}) ->
    case pick_most_similar({T1, Dc1, Action1}, State#state.future_actions) of
        error ->
            State;
        {ok, {T, Dc, Action}} ->
            State2 = State#state{
                future_actions = State#state.future_actions -- [{T, Dc, Action}]
            },
            run_action(State2, Dc, Action)
    end;
next_state(_, Other) ->
    throw({unhandled_next_state_action, Other}).

run_tick(Replica, State, DeltaTime, Msg) ->
    Rs = maps:get(Replica, State#state.replica_states),
    NewTime = State#state.time + DeltaTime,
    {Actions, NewRs} = antidote_lock_server_state:timer_tick(Rs#replica_state.lock_server_state, NewTime, Msg),
    State2 = State#state{
        time = NewTime,
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            time = NewTime
        }, State#state.replica_states)
    },
    add_actions(State2, Replica, Actions).



run_action(State, Dc, {read_crdt_state, SnapshotTime, Objects, Data}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),

    % TODO it is not necessary to read exactly from snapshottime
    ReadSnapshot = vectorclock:max([SnapshotTime, ReplicaState#replica_state.snapshot_time]),

    % collect all updates <= SnapshotTime
    CrdtStates = calculate_crdt_states(ReadSnapshot, Objects, State),
    ReadResults = [antidote_crdt:value(Type, CrdtState) || {{_, Type, _}, CrdtState} <- CrdtStates],

    LockServerState = ReplicaState#replica_state.lock_server_state,
    dorer:log("ReadSnapshot = ~p", [ReadSnapshot]),
    dorer:log("ReadResults = ~p", [ReadResults]),
    {Actions, LockServerState2} = antidote_lock_server_state:on_read_crdt_state(State#state.time, Data, ReadSnapshot, ReadResults, LockServerState),
    ReplicaState2 = ReplicaState#replica_state{
        lock_server_state = LockServerState2,
        snapshot_time = ReadSnapshot
    },
    State2 = State#state{
        replica_states = maps:put(Dc, ReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {send_inter_dc_message, Receiver, Message}) ->
    % deliver interdc message
    ReplicaState = maps:get(Receiver, State#state.replica_states),
    LockServerState = ReplicaState#replica_state.lock_server_state,
    {Actions, LockServerState2} = antidote_lock_server_state:on_receive_inter_dc_message(State#state.time, Dc, Message, LockServerState),
    ReplicaState2 = ReplicaState#replica_state{
        lock_server_state = LockServerState2
    },
    State2 = State#state{
        replica_states = maps:put(Receiver, ReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Receiver, Actions);
run_action(State, Dc, {update_crdt_state, SnapshotTime, Updates, Data}) ->
    % [{bound_object(), op_name(), op_param()}]
    UpdatedObjects = [O || {O, _, _} <- Updates],
    case lists:usort(UpdatedObjects) == lists:sort(UpdatedObjects) of
        true -> ok;
        false -> throw({'List of updates contains duplicates', UpdatedObjects, Updates})
    end,

    ReplicaState = maps:get(Dc, State#state.replica_states),
    UpdateSnapshot = vectorclock:max([ReplicaState#replica_state.snapshot_time, SnapshotTime]),

    CrdtStates = calculate_crdt_states(UpdateSnapshot, UpdatedObjects, State),
    Effects = lists:map(
        fun({{_Key, CrdtState}, {Key, Op, Args}}) ->
            {_, Type, _} = Key,
            {ok, Effect} = antidote_crdt:downstream(Type, {Op, Args}, CrdtState),
            {Key, Effect}
        end,
        lists:zip(CrdtStates, Updates)
    ),
    NewUpdateSnapshot = vectorclock:set(Dc, vectorclock:get(Dc, UpdateSnapshot) + 1, UpdateSnapshot),
    LockServerState = ReplicaState#replica_state.lock_server_state,
    {Actions, NewLockServerState} = antidote_lock_server_state:on_complete_crdt_update(State#state.time, Data, NewUpdateSnapshot, LockServerState),

    NewReplicaState = ReplicaState#replica_state{
        snapshot_time = NewUpdateSnapshot,
        lock_server_state = NewLockServerState
    },
    State2 = State#state{
        crdt_effects = [{NewUpdateSnapshot, Effects} | State#state.crdt_effects],
        replica_states = maps:put(Dc, NewReplicaState, State#state.replica_states)
    },

    add_actions(State2, Dc, Actions);
run_action(State, Dc, {accept_request, {Pid, _Tag}, Vc}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),
    PidState = maps:get(Pid, ReplicaState#replica_state.pid_states),
    NewPidState = PidState#pid_state{
        status = held
    },
    NewReplicaState = ReplicaState#replica_state{
        pid_states = maps:put(Pid, NewPidState, ReplicaState#replica_state.pid_states),
        snapshot_time = vectorclock:max([Vc, ReplicaState#replica_state.snapshot_time])
    },

    State2 = State#state{
        replica_states = maps:put(Dc, NewReplicaState, State#state.replica_states)
    },
    Actions = [
        {release_locks, Pid}
    ],
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {release_locks, Pid}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),

    NewSnapshotTime = vectorclock:set(Dc, vectorclock:get(Dc, ReplicaState#replica_state.snapshot_time), ReplicaState#replica_state.snapshot_time),
    {Actions, NewLockServerState} = antidote_lock_server_state:remove_locks(State#state.time, Pid, NewSnapshotTime, ReplicaState#replica_state.lock_server_state),


    NewReplicaState2 = ReplicaState#replica_state{
        lock_server_state = NewLockServerState,
        pid_states = maps:remove(Pid, ReplicaState#replica_state.pid_states),
        snapshot_time = NewSnapshotTime
    },

    State2 = State#state{
        replica_states = maps:put(Dc, NewReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {set_timeout, T, M}) ->
    run_tick(Dc, State, T, M).

calculate_crdt_states(ReadSnapshot, Objects, State) ->
    Effects = lists:filter(fun({Clock, _}) -> vectorclock:le(Clock, ReadSnapshot) end, State#state.crdt_effects),
    OrderedEffects = antidote_list_utils:topsort(fun({C1, _}, {C2, _}) -> vectorclock:lt(C1, C2) end, Effects),
    CrdtStates = lists:map(fun(Key = {_, Type, _}) ->
        Initial = antidote_crdt:new(Type),
        CrdtState = lists:foldl(fun(Eff, Acc) ->
            {ok, NewAcc} = antidote_crdt:update(Type, Eff, Acc),
            NewAcc
        end, Initial, [Eff || {_, Effs} <- OrderedEffects, {K, Eff} <- Effs, K == Key]),
        {Key, CrdtState}
    end, Objects),
    CrdtStates.


%%make_lock_update(exclusive, From, To, Crdt) ->
%%    maps:from_list([{D, To} || D <- replicas(), maps:get(D, Crdt, D) == From]);
%%make_lock_update(shared, From, To, Crdt) ->
%%    maps:from_list([{D, To} || D <- [To], maps:get(D, Crdt, D) == From]).
%%
%%apply_lock_updates(CrdtStates, []) ->
%%    CrdtStates;
%%apply_lock_updates(CrdtStates, [{L, Upd} | Rest]) ->
%%    CrdtStates2 = maps:update_with(L, fun(V) -> maps:merge(V, Upd) end, Upd, CrdtStates),
%%    apply_lock_updates(CrdtStates2, Rest).

add_actions(State, Dc, Actions) ->
    T = State#state.time,
    NewActions = [{T, Dc, A} || A <- Actions],
    State#state{
        future_actions = State#state.future_actions ++ NewActions
    }.

%%-record(actions, {
%%    % locks to send to other DCs
%%    hand_over = #{} :: #{dcid() => antidote_locks:lock_spec()},
%%    % new lock requests to send to other DCs
%%    lock_request = #{} :: lock_request_actions()
%% #{dcid() => lock_request_actions_for_dc()}.,
%% #{{dcid(), antidote_locks:lock()} => {MaxKind :: antidote_locks:lock_kind(), MinRequestTime :: milliseconds()}}
%%    % local requesting processes to reply to
%%    replies = [] :: [requester()],
%%    % acknowledge that all parts of a lock have been received
%%    ack_locks = [] :: [antidote_locks:lock_spec_item()]
%%}).



check_liveness([]) ->
    true;
check_liveness([Req = {request, Pid, _R, _Locks} | Rest]) ->
    case find_reply(Pid, Rest, 1000000) of
        true -> ok;
        false ->
            throw({'liveness violation, no response for request ', Req})
    end,
    check_liveness(Rest);
check_liveness([_ | Rest]) ->
    check_liveness(Rest).

find_reply(_, _, Time) when Time < 0 ->
    false;
find_reply(_, [], _) ->
    true;
find_reply(Pid, [{action, {_T, _Dc, Action}} | Rest], Time) ->
    case Action of
        {accept_request, {Pid, _}} ->
            true;
        _ ->
            find_reply(Pid, Rest, Time)
    end;
find_reply(Pid, [{tick, _R, T, _M} | Rest], Time) ->
    find_reply(Pid, Rest, Time - T);
find_reply(Pid, [Other | Rest], Time) ->
    % sanity check
    case Other of
        {request, _, _, _} -> ok;
        Other -> throw({unhandled_case2, Other})
    end,
    find_reply(Pid, Rest, Time).


pick_most_similar(_Elem, []) -> error;
pick_most_similar(Elem, List) ->
    WithSimilarity = [{similarity(Elem, X), X} || X <- List],
    {_, Res} = lists:max(WithSimilarity),
    {ok, Res}.


similarity(X, X) -> 1;
similarity(X, Y) when is_atom(X) andalso is_atom(Y) ->
    0.1;
similarity(X, Y) when is_list(X) andalso is_list(Y) ->
    case {X, Y} of
        {[], _} -> 0.1;
        {_, []} -> 0.1;
        {[A | As], [A | Bs]} ->
            L = max(length(As), length(Bs)),
            1 / (1 + L) + similarity(As, Bs) * L / (1 + L);
        {[A | As], Xs} ->
            L = max(1 + length(As), length(Xs)),
            {ok, Sim} = pick_most_similar(A, Xs),
            similarity(A, Sim) / L + similarity(As, Xs -- [Sim]) * (L - 1) / L
    end;
similarity(X, Y) when is_tuple(X) andalso is_tuple(Y) ->
    0.5 + similarity(tuple_to_list(X), tuple_to_list(Y)) * 0.5;
similarity(X, Y) when is_map(X) andalso is_map(Y) ->
    0.5 + similarity(maps:to_list(X), maps:to_list(Y)) * 0.5;
similarity(_, _) -> 0.
