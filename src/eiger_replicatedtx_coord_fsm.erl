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

-module(eiger_replicatedtx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_dependencies/2,
         gather_deps/2,
         scatter_updates/2,
         gather_prepare/2,
         send_commit/2,
         gather_commit/2]).

-record(state, {
          tx_id,
          commit_time,
          snapshot_vector,
          deps,
          transaction,
          nacks,
          scattered_deps,
          scattered_updates,
          updates}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Transaction) ->
    gen_fsm:start_link(?MODULE, [Transaction], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([Tx]) ->
    {TxId, CommitTime, ST, Deps, Ops} = Tx,
    SD = #state{
            tx_id=TxId,
            commit_time=CommitTime,
            snapshot_vector=ST,
            deps=Deps,
            transaction=Tx,
            updates=Ops
           },
    {ok, check_dependencies, SD, 0}.

check_dependencies(timeout, SD0=#state{deps=Deps}) ->
    ScatteredDeps = lists:fold(fun(Dep, Dict0) ->
                                {Key, _CommitTime} = Dep,
                                Preflist = log_utilities:get_preflist_from_key(Key),
                                IndexNode = hd(Preflist),
                                dict:append(IndexNode, Dep, Dict0)
                               end, dict:new(), Deps),
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListDeps} = Slice,
                    eiger_vnode:check_deps(IndexNode, ListDeps)
                  end, dict:to_list(ScatteredDeps)),
    NAcks = length(dict:to_list(ScatteredDeps)),
    {next_state, gather_deps, SD0#state{scattered_deps=ScatteredDeps, nacks=NAcks}}.

gather_deps(deps_checked, SD0=#state{nacks=NAcks0}) ->
    NAcks = NAcks0 - 1,
    case NAcks of
        0 ->
            {next_state, scatter_updates, SD0#state{nacks=NAcks}};
        _ ->
            {next_state, gather_deps, SD0#state{nacks=NAcks}}
    end.
    
scatter_updates(timeout, SD0=#state{updates=Updates, transaction=Transaction}) ->
    ScatteredUpdates = lists:foldl(fun(Update, Dict0) ->
                                    Record = Update#operation.payload,
                                    {Key, _Type, _Param} = Record#log_record.op_payload,
                                    Preflist = log_utilities:get_preflist_from_key(Key),
                                    IndexNode = hd(Preflist),
                                    dict:append(IndexNode, Update, Dict0)
                                   end, dict:new(), Updates),
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    Keys = [Key || {Key, _Type, _Param} <- ListUpdates],
                    eiger_vnode:prepare(IndexNode, Transaction, no_clock, Keys)
                  end, dict:to_list(ScatteredUpdates)),
    NAcks = length(dict:to_list(ScatteredUpdates)),
    {next_state, gather_prepare, SD0#state{scattered_updates=ScatteredUpdates, nacks=NAcks}}.

gather_prepare({prepared, _Clock}, SD0=#state{nacks=NAcks0}) ->
    NAcks = NAcks0 - 1,
    case NAcks of
        0 ->
            {next_state, send_commit, SD0#state{nacks=NAcks},0};
        _ ->
            {next_state, gather_prepare, SD0#state{nacks=NAcks}}
    end.

send_commit(timeout, SD0=#state{scattered_updates=ScatteredUpdates, transaction=Transaction, commit_time=CommitTime}) ->
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    eiger_vnode:commit_replicated(IndexNode, Transaction, ListUpdates, CommitTime)
                  end, dict:to_list(ScatteredUpdates)),
    {next_state, gather_commit, SD0}.

gather_commit(committed, SD0=#state{nacks=NAcks0}) ->
    NAcks = NAcks0 + 1,
    case NAcks of
        0 ->
            {stop, normal, SD0#state{nacks=NAcks}};
        _ ->
            {next_state, gather_commit, SD0#state{nacks=NAcks}}
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
