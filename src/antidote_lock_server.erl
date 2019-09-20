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

%% @doc A lock server is running on each datacenter.
%% It is globally registered under the name 'antidote_lock_server'.

-module(antidote_lock_server).
%%
-include("antidote.hrl").
-include("antidote_message_types.hrl").
-include("antidote_locks.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behavior(gen_server).


%% API
-export([start_link/0, request_locks/2, release_locks/2, on_interdc_reply/2, on_interdc_request/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

% how long (in milliseconds) can a local server prefer local requests over remote requests?
% (higher values should give higher throughput but also higher latency if remote lock requests are necessary)
-define(INTER_DC_LOCK_REQUEST_DELAY, 100).
% how long to wait for locks before requesting them again
-define(INTER_DC_RETRY_DELAY, 1500).

% minimum time for holding an exclusive lock after acquiring it
% (higher values should give higher throughput and higher tail latencies)
-define(MIN_EXCLUSIVE_LOCK_DURATION, 250).

-define(MAX_LOCK_HOLD_DURATION, 1000).

% how long (in milliseconds) may a transaction take to acquire the necessary locks?
-define(LOCK_REQUEST_TIMEOUT, 20000).
-define(LOCK_REQUEST_RETRIES, 3).

% how long (in milliseconds) can a transaction be alive when it is holding locks?
-define(MAX_TRANSACTION_TIME, 20000).

-define(SERVER, ?MODULE).

% there is one lock-part per datacenter.
% the map stores lock-part to current owner
-export_type([lock_crdt_value/0]).

-type lock_crdt_value() :: #{dcid() => dcid()}.
-type requester() :: {pid(), Tag :: term()}.

%% Messages sent to the server:

-record(request_locks, {
    client_clock :: snapshot_time(),
    locks :: antidote_locks:lock_spec()
}).

-record(release_locks, {
    commit_time :: snapshot_time(),
    locks :: antidote_locks:lock_spec()
}).

% internal messages sent to the server:

%%-record(interdc_request, {
%%    message :: antidote_lock_server_state:inter_dc_message()
%%}).

% state of the server:

-record(state, {
    s :: antidote_lock_server_state:state(),
    read_write_process :: pid()
}).

-type state() :: #state{}.

-record(on_complete_crdt_update, {
    cont :: any(),
    clock :: snapshot_time()
}).

-record(on_read_crdt_state, {
    cont :: any(),
    clock :: snapshot_time(),
    values :: [any()]
}).

-record(update_crdt, {
    self :: pid(),
    clock :: snapshot_time(),
    updates :: [{bound_object(), op_name(), op_param()}],
    cont :: any()
}).

-record(read_crdt, {
    self :: pid(),
    clock :: snapshot_time(),
    objects :: [bound_object()],
    cont :: any()
}).

-record(interdc_message, {
    sender :: dcid(),
    body :: antidote_lock_server_state:inter_dc_message()
}).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Starts the server
-spec(start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    % globally register this server so that we only have one
    % lock manager for the whole data center.
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

-spec request_locks(snapshot_time(), antidote_locks:lock_spec()) -> {ok, snapshot_time()} | {error, any()}.
request_locks(ClientClock, Locks) ->
    request(#request_locks{client_clock = ClientClock, locks = Locks}, ?LOCK_REQUEST_TIMEOUT, ?LOCK_REQUEST_RETRIES).

-spec release_locks(snapshot_time(), antidote_locks:lock_spec()) -> ok | {error, any()}.
release_locks(CommitTime, Locks) ->
    request(#release_locks{commit_time = CommitTime, locks = Locks}, infinity, 0).

% sends a request to the global gen-server instance, starting it if necessary
request(Req, Timeout, NumTries) ->
    try
        gen_server:call({global, ?SERVER}, Req, Timeout)
    catch
        exit:{noproc, _} when NumTries > 0 ->
            % if there is no lock server running, start one and try again
            % we register this as a transient process directly under the antidote_sup:
            Res = supervisor:start_child(antidote_sup, #{
                id => lock_server,
                start => {?MODULE, start_link, []},
                % using a transient process, because it will be started on demand and we need
                % to avoid conflicts with other shards who might als try to start a server
                restart => transient
            }),
            case Res of
                {error, Reason} ->
                    logger:error("Could not start antidote_lock_server:~n  ~p", [Reason]);
                {ok, _} -> ok
            end,
            request(Req, Timeout, NumTries - 1);
        Err:Reason:ST ->
            logger:error("Could not handle antidote_lock_server request:~n  ~p~n ~p~n ~p~n ~p", [Req, Err, Reason, ST]),
            case NumTries > 0 of
                true -> request(Req, Timeout, NumTries - 1);
                false -> {error, Reason}
            end
    end.

% called in inter_dc_query_response
-spec on_interdc_request(antidote_lock_server_state:inter_dc_message()) -> ok.
on_interdc_request(Request) ->
    logger:notice("on_interdc_request: ~p", [Request]),
    spawn_link(fun() ->
        request(Request, infinity, 3)
    end),
    ok.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % we want to be notified if a transaction holding locks crashes
    process_flag(trap_exit, true),
    MyDcId = dc_meta_data_utilities:get_my_dc_id(),
    AllDcs = dc_meta_data_utilities:get_dcs(),
    Self = self(),
%%    spawn_link(fun() ->
%%        check_lock_state_process(Self)
%%    end),
    S = antidote_lock_server_state:initial(MyDcId, AllDcs, ?MIN_EXCLUSIVE_LOCK_DURATION, ?MAX_LOCK_HOLD_DURATION, ?INTER_DC_LOCK_REQUEST_DELAY),
    {ok, #state{
        s = S,
        read_write_process = spawn_link(fun() -> read_write_process(Self) end)
    }}.

%%check_lock_state_process(Pid) ->
%%    timer:sleep(100),
%%    {ok, Locks} = gen_server:call(Pid, get_remote_waiting_locks),
%%
%%    LockObjects = antidote_lock_crdt:get_lock_objects([{L, shared} || L <- Locks]),
%%    case LockObjects of
%%        [] ->
%%            timer:sleep(100);
%%        _ ->
%%
%%            case antidote:read_objects(ignore, [], LockObjects) of
%%                {error, Reason} ->
%%                    % this could happen if the shards containing the locks are down
%%                    % if we cannot read the locks we fail immediately since waiting
%%                    % would probably take too much time
%%                    logger:error("check_lock_state_process: Could not obtain locks:~n~p", [Reason]),
%%                    ok;
%%                {ok, LockValuesRaw, ReadClock} ->
%%                    LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
%%                    LockEntries = lists:zip(Locks, LockValues),
%%%%                    logger:notice("check_lock_state_process, LockEntries = ~n~p", [LockEntries]),
%%                    gen_server:cast(Pid, #on_receive_remote_locks2{lock_entries = LockEntries, read_clock = ReadClock})
%%            end
%%    end,
%%
%%    %repeat
%%    check_lock_state_process(Pid).


handle_call(Req, From, State) ->
%%    logger:notice("handle_call~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {_Time, Res} = timer:tc(fun() -> handle_call2(Req, From, State) end),
%%    logger:notice("handle_call done in ~pµs", [Time]),
    Res.

handle_cast(Req, State) ->
%%    logger:notice("handle_cast~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {_Time, Res} = timer:tc(fun() -> handle_cast2(Req, State) end),
%%    logger:notice("handle_cast done in ~pµs", [Time]),
    Res.

handle_info(Req, State) ->
%%    logger:notice("handle_info~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {_Time, Res} = timer:tc(fun() -> handle_info2(Req, State) end),
%%    logger:notice("handle_info done in ~pµs", [Time]),
    Res.

handle_call2(#request_locks{client_clock = ClientClock, locks = Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State);
handle_call2(#release_locks{commit_time = CommitTime}, From, State) ->
    {FromPid, _Tag} = From,
    handle_release_locks(FromPid, CommitTime, State);
handle_call2(#on_complete_crdt_update{clock = Clock, cont = Cont}, _From, State) ->
    S = State#state.s,
    CurrentTime = erlang:system_time(millisecond),
    {Actions, S2} = antidote_lock_server_state:on_complete_crdt_update(CurrentTime, Cont, Clock, S),
    NewState = State#state{s = S2},
    run_actions(Actions, NewState),
    {reply, ok, NewState};
handle_call2(#on_read_crdt_state{clock = Clock, cont = Cont, values = Values}, _From, State) ->
    S = State#state.s,
    CurrentTime = erlang:system_time(millisecond),
    {Actions, S2} = antidote_lock_server_state:on_read_crdt_state(CurrentTime, Cont, Clock, Values, S),
    NewState = State#state{s = S2},
    run_actions(Actions, NewState),
    {reply, ok, NewState};
handle_call2(#interdc_message{sender = Sender, body = Msg}, _From, State) ->
    S = State#state.s,
    CurrentTime = erlang:system_time(millisecond),
    {Actions, S2} = antidote_lock_server_state:on_receive_inter_dc_message(CurrentTime, Sender, Msg, S),
    NewState = State#state{s = S2},
    run_actions(Actions, NewState),
    {reply, ok, NewState};
handle_call2(get_remote_waiting_locks, _From, State) ->
    {reply, {ok, antidote_lock_server_state:get_remote_waiting_locks(State)}, State}.

handle_cast2(Req, State) ->
    logger:error("Unhandled cast request: ~p", [Req]),
    {noreply, State}.



handle_info2({tick, Msg}, State) ->
    Time = erlang:system_time(millisecond),
    logger:notice("tick at ~p", [antidote_lock_server_state:print_systemtime(Time)]),
    S = State#state.s,
    {Actions, S2} = antidote_lock_server_state:timer_tick(S, Time, Msg),
    State2 = State#state{s = S2},
    run_actions(Actions, State2),
    {noreply, State2};

handle_info2({'EXIT', FromPid, Reason}, State) ->
    % when a process crashes, its locks are released
    case Reason of
        normal -> ok;
        _ ->
            logger:notice("process exited ~p ~n  Reason = ~p", [FromPid, Reason])
    end,
    {reply, _, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    {noreply, NewState};
handle_info2({transaction_timeout, FromPid}, State) ->
    {reply, Res, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    case Res of
        ok ->
            % kill the transaction process if it still has the locks
            logger:error("transaction_timeout: Killing process ~p", [FromPid]),
            % TODO better to send a message that the clocksi_interactive_coord can understand
            erlang:exit(FromPid, kill);
        _ -> ok
    end,
    {noreply, NewState};
handle_info2(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================






-spec handle_request_locks(snapshot_time(), antidote_locks:lock_spec(), requester(), state()) -> Result
    when Result :: {reply, Resp, state()} | {noreply, state()},
    Resp :: {could_not_obtain_logs, any()}.
handle_request_locks(ClientClock, Locks, From, State) ->
    logger:notice("handle_request_locks~n ClientClock = ~p~n Locks= ~p", [ClientClock, Locks]),
    % link the requester:
    % if we crash, then the transaction using the locks should crash as well
    % if the transaction crashes, we want to know about that to release the lock
    {FromPid, _} = From,
    link(FromPid),
    % send a message so that we can kill the transaction if it takes too long
    {ok, _} = timer:send_after(?MAX_TRANSACTION_TIME, {transaction_timeout, FromPid}),
    CurrentTime = erlang:system_time(millisecond),
    {Actions, S2} = antidote_lock_server_state:new_request(From, CurrentTime, ClientClock, Locks, State#state.s),
    State2 = State#state{s = S2},
    run_actions(Actions, State2),
    {noreply, State2}.

-spec handle_release_locks(pid(), snapshot_time(), state()) ->
    {reply, Resp, state()}
    when Resp :: ok | {lock_error, reason()} | 'unrelated process'.
handle_release_locks(FromPid, CommitTime, State) ->
    S = State#state.s,
    case antidote_lock_server_state:is_lock_process(FromPid, S) of
        false -> {reply, 'unrelated process', State};
        true ->
            CheckResult = antidote_lock_server_state:check_release_locks(FromPid, S),
            {Actions, S2} = antidote_lock_server_state:remove_locks(erlang:system_time(millisecond), FromPid, CommitTime, S),
            NewState = State#state{s = S2},
            run_actions(Actions, NewState),
            case CheckResult of
                {error, Reason} ->
                    {reply, {lock_error, Reason}, NewState};
                {still_waiting, Requester} ->
                    gen_server:reply(Requester, {error, could_not_acquire_locks}),
                    {reply, {lock_error, still_waiting}, NewState};
                ok ->
                    {reply, ok, NewState}
            end
    end.

%%-spec send_interdc_lock_requests(dcid(), antidote_lock_server_state:lock_request_actions()) -> ok.
%%send_interdc_lock_requests(MyDcId, RequestsByDc) ->
%%    logger:notice("send_interdc_lock_requests~n RequestsByDc = ~p", [antidote_lock_server_state:print_lock_request_actions(RequestsByDc)]),
%%    lists:foreach(fun({OtherDcID, {Pid, Time, RLock}}) ->
%%        RLocks = [RLock],
%%        ReqMsg = #request_locks_remote{timestamp = Time, locks = ordsets:from_list(RLocks), pid = Pid, my_dc_id = MyDcId},
%%        send_interdc_lock_request(OtherDcID, ReqMsg, 3)
%%    end, RequestsByDc).


%%-spec send_interdc_lock_request(dcid(), #request_locks_remote{} | #on_receive_remote_locks{} | #ack{}, integer()) -> ok.
%%send_interdc_lock_request(OtherDcID, ReqMsg, Retries) ->
%%    {LocalPartition, _} = log_utilities:get_key_partition(locks),
%%    PDCID = {OtherDcID, LocalPartition},
%%    logger:notice("send_interdc_lock_request to ~p:~n~p", [OtherDcID, ReqMsg]),
%%    case inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interdc_reply/2) of
%%        ok ->
%%%%            logger:notice("send_interdc_lock_request ok, to ~p,~n~p", [OtherDcID, ReqMsg]),
%%            ok;
%%        Err when Retries > 0 ->
%%            logger:warning("send_interdc_lock_request failed ~p ~p ~p ~p", [Err, OtherDcID, ReqMsg, Retries]),
%%            send_interdc_lock_request(OtherDcID, ReqMsg, Retries - 1);
%%        Err ->
%%            logger:error("send_interdc_lock_request failed ~p ~p ~p", [Err, OtherDcID, ReqMsg]),
%%            ok
%%    end.

on_interdc_reply(_BinaryResp, _RequestCacheEntry) ->
    % Nothing to do here, messages are handled asynchronous and response should always be 'ok'
    ok.


%%handle_request_locks_remote(#request_locks_remote{locks = Locks, timestamp = Timestamp, pid = Pid, my_dc_id = RequesterDcId}, _From, State) ->
%%    NewState = lists:foldl(fun(LockItem, S1) ->
%%        {Actions, S2} = antidote_lock_server_state:new_remote_request(
%%            erlang:system_time(millisecond),
%%            {Pid, Timestamp, LockItem},
%%            S1),
%%        run_actions(Actions, S2),
%%        logger:notice("handle_request_locks_remote~n  State = ~p~n  Locks = ~p~n  Timestamp = ~p~n  RequesterDcId = ~p~n  Actions = ~p", [antidote_lock_server_state:print_state(S1), Locks, antidote_lock_server_state:print_systemtime(Timestamp), RequesterDcId, antidote_lock_server_state:print_actions(Actions)]),
%%        S2
%%    end, State, Locks),
%%    {reply, ok, NewState}.


%%-spec handoff_locks_to_other_dcs(antidote_locks:lock_spec(), dcid(), dcid(), [dcid()], snapshot_time(), integer()) -> {ok, snapshot_time()} | {error, reason()}.
%%handoff_locks_to_other_dcs(Locks, RequesterDcId, MyDcId, AllDcIds, Snapshot, Retries) ->
%%    case antidote:start_transaction(Snapshot, []) of
%%        {error, Reason} ->
%%            logger:error("Could not start transaction:~n  ~p", [Reason]),
%%            {error, Reason};
%%        {ok, TxId} ->
%%            LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
%%            {ok, LockValuesRaw} = antidote:read_objects(LockObjects, TxId),
%%            LockValues = [antidote_lock_crdt:parse_lock_value(L) || L <- LockValuesRaw],
%%            LocksWithValues = lists:zip(Locks, LockValues),
%%            Updates = lists:flatmap(fun({{Lock, Kind}, LockValue1}) ->
%%                LockValue = maps:merge(#{MyDcId => MyDcId}, LockValue1),
%%                LocksToUpdate = case Kind of
%%                    shared ->
%%                        case maps:find(RequesterDcId, LockValue) of
%%                            {ok, MyDcId} ->
%%                                [{RequesterDcId, RequesterDcId}];
%%                            _ ->
%%                                []
%%                        end;
%%                    exclusive ->
%%                        [{K, RequesterDcId} || {K, V} <- maps:to_list(LockValue), V == MyDcId]
%%                end,
%%                antidote_lock_crdt:make_lock_updates(Lock, LocksToUpdate)
%%            end, LocksWithValues),
%%            ok = antidote:update_objects(Updates, TxId),
%%            case antidote:commit_transaction(TxId) of
%%                {error, Reason} when Retries > 0 ->
%%                    logger:error("Handoff transaction to ~p failed with reason ~p, retrying ~p more times", [RequesterDcId, Reason, Retries]),
%%                    handoff_locks_to_other_dcs(Locks, RequesterDcId, MyDcId, AllDcIds, Snapshot, Retries - 1);
%%                {error, Reason} ->
%%                    logger:error("Handoff transaction to ~p failed with reason ~p", [RequesterDcId, Reason]),
%%                    {error, Reason};
%%                {ok, Time} when Updates == [] ->
%%                    logger:notice("Handoff transaction to ~p SUCCESS~n no updates~n LockValues = ~p", [RequesterDcId, Updates, LocksWithValues]),
%%                    {ok, Time};
%%                {ok, Time} ->
%%                    logger:notice("Handoff transaction to ~p SUCCESS~n Updates = ~p~n LockValues = ~p", [RequesterDcId, Updates, LocksWithValues]),
%%                    antidote_lock_server_state:debug_log({event, updates_transferred, #{
%%                        commit_time => antidote_lock_server_state:print_vc(Time),
%%                        locks_with_values => LocksWithValues,
%%                        updates => Updates
%%                    }}),
%%                    send_interdc_lock_request(RequesterDcId, #on_receive_remote_locks{
%%                        snapshot_time = Time,
%%                        locks         = Locks
%%                    }, 3),
%%                    {ok, Time}
%%            end
%%    end.




-spec run_actions(antidote_lock_server_state:actions(), state()) -> ok.
run_actions(Actions, State) ->
    lists:foreach(fun(A) -> run_action(A, State) end, Actions).

-spec run_action(antidote_lock_server_state:action(), state()) -> ok.
run_action(#read_crdt_state{snapshot_time = Clock, data = Cont, objects = Objects}, State) ->
    antidote_lock_server_state:debug_log({event, read_crdt_send, #{
        clock => Clock,
        objects => Objects,
        cont => Cont
    }}),
    State#state.read_write_process ! #read_crdt{self = self(), clock = Clock, objects = Objects, cont = Cont};
run_action(#update_crdt_state{updates = Updates, snapshot_time = Clock, data = Cont}, State) ->
    antidote_lock_server_state:debug_log({event, update_crdt_send, #{
        clock => Clock,
        updates => Updates,
        cont => Cont
    }}),
    State#state.read_write_process ! #update_crdt{self = self(), clock = Clock, updates = Updates, cont = Cont};
run_action(#send_inter_dc_message{receiver = Receiver, message = Message}, _State) ->
    % TODO should this be done async?
    send_interdc_lock_request(Receiver, Message, 3);
run_action(#accept_request{requester = From, clock = Clock}, _State) ->
    gen_server:reply(From, {ok, Clock});
run_action(#abort_request{requester = From}, _State) ->
    gen_server:reply(From, {error, no_locks});
run_action(#set_timeout{timeout = T, message = M}, _State) ->
    {ok, _Ref} = timer:send_after(T, {tick, M}),
    ok.


-spec send_interdc_lock_request(dcid(), antidote_lock_server_state:inter_dc_message(), integer()) -> ok.
send_interdc_lock_request(OtherDcID, ReqMsg, Retries) ->
    {LocalPartition, _} = log_utilities:get_key_partition(locks),
    PDCID = {OtherDcID, LocalPartition},
    logger:notice("send_interdc_lock_request to ~p:~n~p", [OtherDcID, ReqMsg]),
    Msg = term_to_binary(#interdc_message{sender = dc_utilities:get_my_dc_id(), body = ReqMsg}),
    case inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, Msg, fun antidote_lock_server:on_interdc_reply/2) of
        ok ->
%%            logger:notice("send_interdc_lock_request ok, to ~p,~n~p", [OtherDcID, ReqMsg]),
            ok;
        Err when Retries > 0 ->
            logger:warning("send_interdc_lock_request failed ~p ~p ~p ~p", [Err, OtherDcID, ReqMsg, Retries]),
            send_interdc_lock_request(OtherDcID, ReqMsg, Retries - 1);
        Err ->
            logger:error("send_interdc_lock_request failed ~p ~p ~p", [Err, OtherDcID, ReqMsg]),
            ok
    end.


%%handle_on_receive_remote_locks(Locks, SnapshotTime, State) ->
%%    logger:info("handle_on_receive_remote_locks~n Locks = ~p~n", [Locks]),
%%    antidote_lock_server_state:debug_log({event, handle_on_receive_remote_locks, #{
%%        commit_time => antidote_lock_server_state:print_vc(SnapshotTime),
%%        locks => Locks
%%    }}),
%%    Self = self(),
%%    spawn_link(fun() ->
%%        LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
%%        case antidote:read_objects(SnapshotTime, [], LockObjects) of
%%            {error, Reason} ->
%%                logger:error("handle_on_receive_remote_locks, could not read objects ~p~n~p", [Reason, LockObjects]),
%%                % ignore
%%                ok;
%%            {ok, LockValuesRaw, ReadClock} ->
%%                LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
%%                LockEntries = lists:zip(Locks, LockValues),
%%%%                logger:notice("handle_on_receive_remote_locks, new lock entries:~n~p", [LockEntries]),
%%                antidote_lock_server_state:debug_log({event, handle_on_receive_remote_locks, #{
%%                    commit_time => antidote_lock_server_state:print_vc(SnapshotTime),
%%                    read_clock => antidote_lock_server_state:print_vc(ReadClock),
%%                    locks => Locks,
%%                    LockEntries => LockEntries
%%                }}),
%%                gen_server:cast(Self, #on_receive_remote_locks2{lock_entries = LockEntries, read_clock = ReadClock})
%%        end
%%    end),
%%    {reply, ok, State}.
%%
%%handle_on_receive_remote_locks2(LockEntries, ReadClock, State) ->
%%    logger:notice("handle_on_receive_remote_locks2, new lock entries:~n~p", [LockEntries]),
%%    AllDcIds = dc_meta_data_utilities:get_dcs(),
%%    {Actions, NewState} = antidote_lock_server_state:on_remote_locks_received(erlang:system_time(millisecond), ReadClock, AllDcIds, LockEntries, State),
%%%%    logger:notice("handle_on_receive_remote_locks~n  LockEntries = ~p~n  State = ~p~n  NewState = ~p~n  Actions = ~p", [LockEntries, antidote_lock_server_state:print_state(State), antidote_lock_server_state:print_state(NewState), antidote_lock_server_state:print_actions(Actions)]),
%%    run_actions(Actions, NewState),
%%    {noreply, NewState}.


% process for performing reads and updates.
% Ensures that updates are executed before reads, so that we read the latest values
read_write_process(Self) ->
    receive
        #update_crdt{self = Self, clock = Clock, updates = Updates, cont = Cont} ->
            update_crdt(Self, Clock, Updates, Cont)
    after 0 ->
        receive
            #update_crdt{self = Self, clock = Clock, updates = Updates, cont = Cont} ->
                update_crdt(Self, Clock, Updates, Cont);
            #read_crdt{self = Self, clock = Clock, objects = Objects, cont = Cont} ->
                read_crdt(Self, Clock, Objects, Cont)
        end
    end.




update_crdt(Self, Clock, Updates, Cont) ->
    antidote_lock_server_state:debug_log({event, update_crdt_start, #{
        clock => Clock,
        updates => Updates,
        cont => Cont
    }}),
    {ok, WriteClock} = antidote:update_objects(Clock, [], Updates),
    antidote_lock_server_state:debug_log({event, update_crdt_done, #{
        clock => Clock,
        updates => Updates,
        cont => Cont
    }}),
    gen_server:call(Self, #on_complete_crdt_update{cont = Cont, clock = WriteClock}),
    read_write_process(Self).

read_crdt(Self, Clock, Objects, Cont) ->
    antidote_lock_server_state:debug_log({event, read_crdt_start, #{
        clock => Clock,
        objects => Objects,
        cont => Cont
    }}),
    {ok, Values, ReadClock} = antidote:read_objects(Clock, [], Objects),
    antidote_lock_server_state:debug_log({event, read_crdt_done, #{
        clock => Clock,
        objects => Objects,
        values => Values,
        cont => Cont
    }}),
    gen_server:call(Self, #on_read_crdt_state{cont = Cont, clock = ReadClock, values = Values}),
    read_write_process(Self).



-ifdef(TEST).

simple_test() ->
    ok.

-endif.

