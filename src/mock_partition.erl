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
%% @doc A mocked file that emulates the behavior of several antidote
%%      components which relies on riak-core backend, e.g.
%%      clocksi_vnode, dc_utilities and log_utilities. For simplicity,
%%      the reply of some functions depend on the key being updated.
%%      The detailed usage can be checked within each function, which is
%%      self-explanatory.

-module(mock_partition).

-behavior(gen_statem).

-include("antidote.hrl").

%% API
-export([
    start_link/0,
    append/3,
    asyn_append/4,
    get_my_dc_id/0,
    get_clock_of_dc/2,
    get_preflist_from_key/1,
    read_data_item/5,
    generate_downstream_op/7,
    get_key_partition/1,
    get_logid_from_key/1,
    update_data_item/5,
    prepare/2,
    value/1,
    set_clock_of_dc/3,
    abort/2,
    commit/3,
    single_commit/2,
    get_stable_snapshot/0,
    inc/2,
    inc/1,
    dec/1
]).

%% States
-export([execute_op/3]).

%% statem Callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3,
    code_change/4
]).

-record(state, {
        key :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_statem:start_link(?MODULE, [], []).

append(_Node, _LogId, _LogRecord) ->
    {ok, {0, node}}.

asyn_append(_Node, _LogId, _LogRecord, ReplyTo) ->
    case ReplyTo of
        ignore -> ok;
        {_, _, Pid} -> gen_statem:cast(Pid, {ok, 0})
    end,
    ok.

%% @doc Initialize the state.
init([]) ->
    {ok, execute_op, #state{}}.

%% Functions that always return the same value no matter the input.
get_my_dc_id() ->
    mock_dc.

value(_) ->
    mock_value.

set_clock_of_dc(_, _, Clock) ->
    Clock.

get_clock_of_dc(_DcId, _SnapshotTime) ->
    0.

get_key_partition(_Key) ->
    {ok, Pid} = mock_partition:start_link(),
    Pid.

get_preflist_from_key(_Key) ->
    {ok, Pid} = mock_partition:start_link(),
    [Pid].

get_stable_snapshot() ->
    {ok, dict:new()}.

get_logid_from_key(_Key) ->
    self().

abort(UpdatedPartitions, _Transactions) ->
    Self = self(),
    lists:foreach(fun({Fsm, Rest}) -> gen_statem:cast(Fsm, {ack_abort, Self, Rest}) end, UpdatedPartitions).

single_commit(UpdatedPartitions, _Transaction) ->
    Self = self(),
    lists:foreach(fun({Fsm, Rest}) -> gen_statem:cast(Fsm, {prepare, Self, Rest}) end, UpdatedPartitions).

commit(_UpdatedPartitions, _Transaction, _CommitTime) ->
    ok.

%% Functions that will return different value depending on Key.
read_data_item(_IndexNode, _Transaction, Key, _Type, _Ws) ->
    case Key of
        read_fail ->
            {error, mock_read_fail};
        counter ->
            Counter = antidote_crdt_counter_pn:new(),
            {ok, Counter1} = antidote_crdt_counter_pn:update(1, Counter),
            {ok, Counter2} = antidote_crdt_counter_pn:update(1, Counter1),
            {ok, Counter2};
        set ->
            Set = antidote_crdt_set_go:new(),
            {ok, Set1} = antidote_crdt_set_go:update([a], Set),
            {ok, Set1};
        _ ->
            {ok, mock_value}
    end.

generate_downstream_op(_Transaction, _IndexNode, Key, _Type, _Param, _Ws, _Rs) ->
    case Key of
        downstream_fail ->
            {error, mock_downstream_fail};
        _ ->
            {ok, mock_downsteam}
    end.


update_data_item(FsmRef, _Transaction, Key, _Type, _DownstreamRecord) ->
    gen_statem:call(FsmRef, {update_data_item, Key}).

prepare(UpdatedPartitions, _Transaction) ->
    Self = self(),
    lists:foreach(fun({Fsm, Rest}) -> gen_statem:cast(Fsm, {prepare, Self, Rest}) end, UpdatedPartitions).

inc(_, _) -> ok.
dec(_) -> ok.
inc(_) -> ok.

%%%===================================================================
%%% STATE FUNCTIONS
%%%===================================================================

%% We spawn a new mock_partition for each update request, therefore
%% a mock will only receive a single update so only need to store a
%% single updated key. In contrast, clocksi_vnode may receive multiple
%% update request for a single transaction.
execute_op({call, From}, {update_data_item, Key}, State) ->
    Result = case Key of
                fail_update -> {error, mock_downstream_fail};
                _ -> ok
             end,
    %% current responses (from logging_vnode)
%%    gen_fsm:send_event(From, Result),
    %% how it should be sent
    gen_statem:cast(From, Result),
    {next_state, execute_op, State#state{key=Key}};

execute_op(cast, {prepare, From, [{Key, _, _}|_]}, State) ->
    Result = case Key of
                single_commit -> {committed, 10};
                success -> {prepared, 10};
                timeout -> timeout;
                _ -> abort
            end,
    gen_statem:cast(From, Result),
%%    gen_fsm:send_event(From, Result),
    {next_state, execute_op, State};

execute_op(cast, {ack_abort, From, _}, State) ->
%%    gen_fsm:send_event(From, ack_abort),
    gen_statem:cast(From, ack_abort),
    {stop, normal, State}.

%%%===================================================================
%%% statem CALLBACK FUNCTIONS
%%%=====================================================================

callback_mode() -> state_functions.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.

