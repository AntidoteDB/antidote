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
-module(clocksi_batch_read_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/4]).

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
         return/2]).

%% Spawn

-record(state, {reads,
                transaction,
                tx_coordinator,
                vnode,
                pending_txs}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, Tx, Reads) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 Tx, Reads], []).


%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, Transaction, Reads]) ->
    SD = #state{vnode=Vnode,
                tx_coordinator=Coordinator,
                transaction=Transaction,
                reads=Reads,
                pending_txs=[]},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
check_clock(timeout, SD0=#state{transaction=Transaction}) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = clocksi_vnode:now_microsec(erlang:now()),
    case T_TS > Time of
        true ->
	    SleepMiliSec = (T_TS - Time) div 1000 + 1,
            timer:sleep(SleepMiliSec),
            {next_state, waiting1, SD0, 0};
        false ->
            {next_state, waiting1, SD0, 0}
    end.

waiting1(timeout, SDO=#state{transaction=Transaction, vnode=Vnode}) ->
    LocalClock = get_stable_time(Vnode),
    TxId = Transaction#transaction.txn_id,
    SnapshotTime = TxId#tx_id.snapshot_time,
    case LocalClock > SnapshotTime of
        false ->
            {next_state, waiting1, SDO, 1};
        true ->
            {next_state, return, SDO, 0}
    end;

waiting1(_SomeMessage, SDO) ->
    {next_state, waiting1, SDO,0}.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(timeout, SD0=#state{tx_coordinator=Coordinator,
                           transaction=Transaction,
                           vnode=Vnode,
                           reads=Reads}) ->
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    TxId = Transaction#transaction.txn_id,
    case materializer_vnode:multi_read(Vnode, Reads, VecSnapshotTime, TxId) of
        {ok, PartitionReadSet} ->
			Reply = {batch_read_result, PartitionReadSet};
        {error, Reason} ->
            Reply={error, Reason}
    end,
    riak_core_vnode:reply(Coordinator, Reply),
    {stop, normal, SD0};
    
    
    
    
    
return(_SomeMessage, SDO) ->
    {next_state, return, SDO,0}.

handle_info(_Info, StateName, StateData) ->
    {next_state,StateName,StateData,1}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Internal functions

get_stable_time(Vnode) ->
    case riak_core_vnode_master:sync_command(
           Vnode, {get_active_txns}, ?CLOCKSI_MASTER) of
        {ok, Active_txns} ->
            lists:foldl(fun({_,{_TxId, Snapshot_time}}, Min_time) ->
                                case Min_time > Snapshot_time of
                                    true ->
                                        Snapshot_time;
                                    false ->
                                        Min_time
                                end
                        end,
                        clocksi_vnode:now_microsec(erlang:now()),
                        Active_txns);
        _ -> 0
    end.
