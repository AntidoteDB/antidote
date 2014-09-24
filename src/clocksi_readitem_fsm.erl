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
-module(clocksi_readitem_fsm).

-behavior(gen_fsm).

-include("floppy.hrl").

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([check_clock/2,
         waiting1/2,
         waiting2/2,
         return/2]).

%% Spawn

-record(state, {type,
                key,
                transaction,
                tx_coordinator,
                vnode,
                updates,
                pending_txs}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, Tx, Key, Type, Updates) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 Tx, Key, Type, Updates], []).

now_milisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, Transaction, Key, Type, Updates]) ->
    SD = #state{vnode=Vnode,
                type=Type,
                key=Key,
                tx_coordinator=Coordinator,
                transaction=Transaction,
                updates=Updates,
                pending_txs=[]},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behinf, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
check_clock(timeout, SD0=#state{transaction=Transaction}) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = now_milisec(erlang:now()),
    case (T_TS) > Time of
        true ->
            timer:sleep(T_TS - Time),
            {next_state, waiting1, SD0, 0};
        false ->
            {next_state, waiting1, SD0, 0}
    end.

waiting1(timeout, SDO=#state{key=Key, transaction=Transaction}) ->
    case vectorclock:get_clock_by_key(Key) of
        {ok, LocalClock} ->
            SnapshotTime = Transaction#transaction.vec_snapshot_time,
            case vectorclock:is_greater_than(LocalClock, SnapshotTime) of
                false ->
                    clocksi_downstream_generator_vnode:trigger(Key, {dummytx, [], vectorclock:from_list([]), 0}),
                    %% TODO change this, add a heartbeat to increase vectorclock if
                    %%     there are no pending txns in downstream generator
                    {next_state, waiting2, SDO, 1};
                true ->
                    {next_state, return, SDO, 0}
            end;
        _ ->
            {next_state, waiting1, SDO, 5}
    end;
waiting1(_SomeMessage, SDO) ->
    {next_state, waiting1, SDO,0}.

waiting2(timeout, SDO=#state{key=Key, transaction=Transaction}) ->
    case  vectorclock:get_clock_by_key(Key) of
        {ok, LocalClock} ->
            SnapshotTime = Transaction#transaction.vec_snapshot_time,
            case vectorclock:is_greater_than(LocalClock, SnapshotTime) of
                false ->
                    {next_state, waiting2, SDO, 1};
                true ->
                    {next_state, return, SDO, 0}
            end;
        _ ->
            {next_state, waiting2, SDO, 5}
    end;
waiting2(_SomeMessage, SDO) ->
    {next_state, waiting2, SDO,0}.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(timeout, SD0=#state{key=Key,
                           tx_coordinator=Coordinator,
                           transaction=Transaction,
                           type=Type,
                           updates=Updates}) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    case materializer_vnode:read(Key, Type, VecSnapshotTime) of
        {ok, Snapshot} ->
            Updates2=filter_updates_per_key(Updates, Key),
            Snapshot2=clocksi_materializer:update_snapshot_eager
                        (Type, Snapshot, Updates2),
            Reply=Type:value(Snapshot2);
        {error, Reason} ->
            Reply={error, Reason}
    end,
    riak_core_vnode:reply(Coordinator, Reply),
    {stop, normal, SD0};
return(_SomeMessage, SDO) ->
    {next_state, return, SDO,0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Internal functions

filter_updates_per_key(Updates, Key) ->
    int_filter_updates_key(Updates, Key, []).

int_filter_updates_key([], _Key, Updates2) ->
    Updates2;

int_filter_updates_key([Next|Rest], Key, Updates2) ->
    {_, {KeyPrime, _Type, Op}} = Next,
    case KeyPrime==Key of
        true ->
            int_filter_updates_key(Rest, Key, lists:append(Updates2, [Op]));
        false ->
            int_filter_updates_key(Rest, Key, Updates2)
    end.
