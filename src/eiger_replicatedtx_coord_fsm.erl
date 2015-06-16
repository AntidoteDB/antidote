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
         compose_tx/2,
         gather_deps/2,
         scatter_updates/2,
         gather_prepare/2,
         send_commit/2,
         gather_commit/2]).

-record(state, {
          tx_id,
          commit_time,
          prepare_op,
          commit_op,
          snapshot_vector,
          deps,
          transaction,
          nacks,
          scattered_deps,
          scattered_updates,
          scattered_keys,
          total_ops,
          to_complete,
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
    {TxId, CommitTime, ST, Deps, Ops, TotalOps} = Tx,
    Updates = lists:sublist(Ops, length(Ops) - 2),
    SD = #state{
            commit_op=lists:last(Ops),
            prepare_op=hd(lists:sublist(Ops, length(Ops) - 1, 1)),
            tx_id=TxId,
            commit_time=CommitTime,
            snapshot_vector=ST,
            deps=Deps,
            transaction=Tx,
            updates=Updates,
            total_ops=TotalOps,
            to_complete=TotalOps - length(Updates)
           },
    {ok, compose_tx, SD}.

compose_tx({notify, NewUpdates0}, SD0=#state{updates=Updates0, to_complete=ToComplete0}) ->
    NewUpdates = lists:sublist(NewUpdates0, length(NewUpdates0) - 2),
    Updates = Updates0 ++ NewUpdates,
    ToComplete = ToComplete0 - length(NewUpdates),
    case ToComplete of
        0 ->
            {next_state, check_dependencies, SD0#state{to_complete=ToComplete, updates=Updates}, 0};
        _ ->
            {next_state, compose_tx, SD0#state{to_complete=ToComplete, updates=Updates}}
    end.

check_dependencies(timeout, SD0=#state{deps=Deps}) ->
    ScatteredDeps = lists:foldl(fun(Dep, Dict0) ->
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
    case NAcks of
        0 ->
            {next_state, scatter_updates, SD0#state{scattered_deps=ScatteredDeps, nacks=NAcks}, 0};
        _ ->
            {next_state, gather_deps, SD0#state{scattered_deps=ScatteredDeps, nacks=NAcks}}
    end.

gather_deps(deps_checked, SD0=#state{nacks=NAcks0}) ->
    NAcks = NAcks0 - 1,
    case NAcks of
        0 ->
            {next_state, scatter_updates, SD0#state{nacks=NAcks}, 0};
        _ ->
            {next_state, gather_deps, SD0#state{nacks=NAcks}}
    end.
    
scatter_updates(timeout, SD0=#state{updates=Updates, tx_id=TxId}) ->
    {ScatteredUpdates, ScatteredKeys} = lists:foldl(fun(Update, {SU0, SK0}) ->
                                                        Record = Update#operation.payload,
                                                        {Key, _Type, _Param} = Record#log_record.op_payload,
                                                        Preflist = log_utilities:get_preflist_from_key(Key),
                                                        IndexNode = hd(Preflist),
                                                        {dict:append(IndexNode, Update, SU0), dict:append(IndexNode, Key, SK0)}
                                                    end, {dict:new(), dict:new()}, Updates),
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListKeys} = Slice,
                    eiger_vnode:prepare_replicated(IndexNode, TxId, ListKeys)
                  end, dict:to_list(ScatteredKeys)),
    NAcks = length(dict:to_list(ScatteredKeys)),
    case NAcks of
        0 ->
            {next_state, send_commit, SD0#state{scattered_updates=ScatteredUpdates, scattered_keys=ScatteredKeys, nacks=NAcks}, 0};
        _ ->
            {next_state, gather_prepare, SD0#state{scattered_updates=ScatteredUpdates, scattered_keys=ScatteredKeys, nacks=NAcks}}
    end.

gather_prepare({prepared, _Clock}, SD0=#state{nacks=NAcks0}) ->
    NAcks = NAcks0 - 1,
    case NAcks of
        0 ->
            {next_state, send_commit, SD0#state{nacks=NAcks},0};
        _ ->
            {next_state, gather_prepare, SD0#state{nacks=NAcks}}
    end.

send_commit(timeout, SD0=#state{scattered_updates=ScatteredUpdates, transaction=Transaction, prepare_op=PrepareOp, commit_op=CommitOp}) ->
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates0} = Slice,
                    ListUpdates = ListUpdates0 ++ [PrepareOp] ++ [CommitOp],
                    eiger_vnode:commit_replicated(IndexNode, Transaction, ListUpdates)
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
