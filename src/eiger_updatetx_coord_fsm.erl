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

-module(eiger_updatetx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([scatter_updates/2,
         gather_prepare/2,
         send_commit/2,
         wait_for_commit/3,
         gather_commit/2]).

-record(state, {
          from,
          vnode,
          updates,
          debug,
          transaction,
          scattered_updates,
          commit_time,
          ack,
          dc,
          servers}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, From, Updates, Debug) ->
    gen_fsm:start_link(?MODULE, [Vnode, From, Updates, Debug], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([Vnode, From, Updates, Debug]) ->
    DcId = dc_utilities:get_my_dc_id(),
    {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    SD = #state{
            dc=DcId,
            vnode=Vnode,
            from=From,
            debug=Debug,
            tx_id=TransactionId,
            updates=Updates
           },
    {ok, scatter_updates, SD, 0}.

scatter_updates(timeout, SD0=#state{vnode=Vnode, updates=Updates, tx_id=TxId}) ->
    ScatteredUpdates = lists:foldl(fun(Update, Dict0) ->
                                    {Key, _Type, _Param} = Update,
                                    Preflist = log_utilities:get_preflist_from_key(Key),
                                    IndexNode = hd(Preflist),
                                    dict:append(IndexNode, Update, Dict0)
                                   end, dict:new(), Updates),
    {ok, Clock} = eiger_vnode:get_clock(Vnode), 
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    Keys = [Key || {Key, _Type, _Param} <- ListUpdates],
                    eiger_vnode:prepare(IndexNode, TxId, Clock, Keys)
                  end, dict:to_list(ScatteredUpdates)),
    Servers = length(dict:to_list(ScatteredUpdates)),
    {next_state, gather_prepare, SD0#state{scattered_updates=ScatteredUpdates, servers=Servers, ack=0, commit_time=0}}.

gather_prepare({prepared, Clock}, SD0=#state{vnode=Vnode, servers=Servers, ack=Ack0, from=From, debug=Debug, commit_time=CommitTime0}) ->
    ok = eiger_vnode:update_clock(Vnode, Clock),
    CommitTime = max(CommitTime0, Clock),
    Ack = Ack0 + 1,
    case Ack of
        Servers ->
            case Debug of
                debug ->
                    riak_core_vnode:reply(From, {ok, self()}),
                    {next_state, wait_for_commit, SD0#state{ack=0, commit_time=CommitTime}};
                undefined ->
                    {next_state, send_commit, SD0#state{ack=0, commit_time=CommitTime},0}
            end;
        _ ->
            {next_state, gather_prepare, SD0#state{ack=Ack, commit_time=CommitTime}}
    end.

wait_for_commit(commit, Sender, SD0) ->
    {next_state, send_commit, SD0#state{from=Sender}, 0}.

send_commit(timeout, SD0=#state{scattered_updates=ScatteredUpdates, tx_id=TxId, commit_time=CommitTime, updates=Updates, tx_id=TxId, dc=DcId}) ->
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    eiger_vnode:commit(IndexNode, TxId, ListUpdates, {DcId, CommitTime}, length(Updates))
                  end, dict:to_list(ScatteredUpdates)),
    {next_state, gather_commit, SD0}.

gather_commit({committed, Clock}, SD0=#state{vnode=Vnode, scattered_updates=_ScatteredUpdates, servers=Servers, ack=Ack0, from=From, commit_time=CommitTime, debug=Debug}) ->
    ok = eiger_vnode:update_clock(Vnode, Clock),
    Ack = Ack0 + 1,
    case Ack of
        Servers ->
            case Debug of
                debug ->
                    gen_fsm:reply(From, {ok, CommitTime});
                undefined ->
                    riak_core_vnode:reply(From, {ok, CommitTime})
            end,
            {stop, normal, SD0#state{ack=Ack}};
        _ ->
            {next_state, gather_commit, SD0#state{ack=Ack}}
    end. 

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
