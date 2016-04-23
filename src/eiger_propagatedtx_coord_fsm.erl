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

-module(eiger_propagatedtx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([gather/2,
         idle/2,
         prepare/2,
         gather_prepare/2,
         send_commit/2,
         gather_commit/2]).

-record(state, {
          tx_id,
          vnode,
          n_partitions,
          scattered_updates,
          n_partitions_deps,
          commit_clock,
          ack,
          timestamp,
          notifies,
          deps_ack
          }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode) ->
    gen_fsm:start_link(?MODULE, [Vnode], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([Vnode]) ->
    {ok, idle, #state{vnode=Vnode}}.

idle({new_tx, TxId, TimeStamp, Deps, Ops}, S0) ->
    {ListDeps, NPartitions} = Deps,
    NewList = case ListDeps of
                [_H|_T] ->
                    ListDeps;
                _ ->
                    []
              end,
    Aggregate = length(Ops),
    MergedOps = lists:flatten(Ops),
    DepsPartition = lists:foldl(fun(Dependency, Dict)->
                                    %lager:info("Dependency: ~p", [Dependency]),
                                    {Key, _TimeStamp} = Dependency,
                                    Preflist = log_utilities:get_preflist_from_key(Key),
                                    IndexNode = hd(Preflist),
                                    dict:append(IndexNode, Dependency, Dict)
                                end, dict:new(), NewList),
    lists:foreach(fun({Partition, Slice}) ->
                    eiger_vnode:check_deps(Partition, Slice)
                  end, dict:to_list(DepsPartition)),
    ScatteredUpdates = lists:foldl(fun(Operation, Acc) ->
                                    Logrecord = Operation#operation.payload,
                                    case Logrecord#log_record.op_type of
                                        update ->
                                            {Key, _Value, Param} = Logrecord#log_record.op_payload,
                                            Preflist = log_utilities:get_preflist_from_key(Key),
                                            IndexNode = hd(Preflist),
                                            dict:append(IndexNode, Param, Acc);
                                        _ ->
                                            Acc
                                    end
                                   end, dict:new(), MergedOps),

    S1 = S0#state{tx_id=TxId,
                  n_partitions=NPartitions,
                  scattered_updates=ScatteredUpdates,
                  n_partitions_deps=length(dict:fetch_keys(DepsPartition)),
                  notifies=Aggregate,
                  deps_ack=0,
                  timestamp=TimeStamp
                 },

    case NPartitions of
        Aggregate ->
            case ListDeps of
                [] ->
                    {next_state, prepare, S1, 0};
                _ ->
                    {next_state, gather, S1}
            end;
        _ ->
            {next_state, gather, S1}
    end.

gather(timeout, SD0) ->
    {next_state, gather, SD0};

gather({notify, Ops, _Partition}, SD0=#state{scattered_updates=ScatteredUpdates0, notifies=Notifies0, n_partitions=NPartitions, deps_ack=DepsAck, n_partitions_deps=NPDeps}) ->
    Notifies1 = Notifies0 + 1,
    %lager:info("Received a notify. Received: ~p, total: ~p. Deps total: ~p, received: ~p", [Notifies1, NPartitions, NPDeps, DepsAck]),
    ScatteredUpdates1 = lists:foldl(fun(Operation, Acc) ->
                                        Logrecord = Operation#operation.payload,
                                        case Logrecord#log_record.op_type of
                                            update ->
                                                {Key, _Value, Param} = Logrecord#log_record.op_payload,
                                                Preflist = log_utilities:get_preflist_from_key(Key),
                                                IndexNode = hd(Preflist),
                                                dict:append(IndexNode, Param, Acc);
                                            _ ->
                                                Acc
                                        end
                                    end, ScatteredUpdates0, Ops),
    case Notifies1 of
        NPartitions ->
            case DepsAck of
                NPDeps ->
                    %lager:info("Lets prepare"),
                    {next_state, prepare, SD0#state{scattered_updates=ScatteredUpdates1, notifies=Notifies1}, 0};
                _ ->
                    {next_state, gather, SD0#state{scattered_updates=ScatteredUpdates1, notifies=Notifies1}, 0}
            end;
        _ ->
            {next_state, gather, SD0#state{scattered_updates=ScatteredUpdates1, notifies=Notifies1}, 0}
    end;

gather(deps_checked, SD0=#state{notifies=Notifies, n_partitions=NPartitions, deps_ack=DepsAck0, n_partitions_deps=NPDeps}) ->
    %lager:info("Received deps_checked"),
    DepsAck1 = DepsAck0 + 1,
    case Notifies of
        NPartitions ->
            case DepsAck1 of
                NPDeps ->
                    {next_state, prepare, SD0#state{deps_ack=DepsAck1}, 0};
                _ ->
                    {next_state, gather, SD0#state{deps_ack=DepsAck1}, 0}
            end;
        _ ->
            {next_state, gather, SD0#state{deps_ack=DepsAck1}, 0}
    end.

prepare(timeout, SD0=#state{tx_id=TxId, vnode=_Vnode, scattered_updates=ScatteredUpdates0, timestamp=TimeStamp}) ->
    %lager:info("About to prepare"),
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    Keys = [Key || {Key, _Type, _Param} <- ListUpdates],
                    eiger_vnode:remote_prepare(IndexNode, TxId, TimeStamp,  Keys)
                  end, dict:to_list(ScatteredUpdates0)),
    {next_state, gather_prepare, SD0#state{ack=0, commit_clock=0, n_partitions=length(dict:fetch_keys(ScatteredUpdates0))}}.

gather_prepare({prepared, Clock, Keys, Partition}, SD0=#state{vnode=Vnode, n_partitions=NPartitions, ack=Ack0, commit_clock=CommitClock0, scattered_updates=ScatteredUpdates0}) ->
    %lager:info("Prepared: received= ~p, total= ~p", [Ack0+1, NPartitions]),
    ok = eiger_vnode:update_clock(Vnode, Clock),
    Updates0 = dict:fetch(Partition, ScatteredUpdates0),
    Updates1 = lists:foldl(fun(Update, Acc) ->
                            {Key, _Type, _Param} = Update,
                            %lager:info("Is ~p contained in ~p", [Key, Keys]),
                            case contains(Key, Keys) of
                                true -> Acc ++ [Update];
                                false -> Acc
                            end
                           end, [], Updates0),
    %lager:info("Updates to commit: ~p", [Updates1]),
    case Updates1 of
        [] ->
            ScatteredUpdates1 = dict:erase(Partition, ScatteredUpdates0);
        _List ->
            ScatteredUpdates1 = dict:store(Partition, Updates1, ScatteredUpdates0)
    end,
    CommitClock = max(CommitClock0, Clock),
    Ack = Ack0 + 1,
    case Ack of
        NPartitions ->
            {next_state, send_commit, SD0#state{ack=0, commit_clock=CommitClock, scattered_updates=ScatteredUpdates1},0};
        _ ->
            {next_state, gather_prepare, SD0#state{ack=Ack, commit_clock=CommitClock, scattered_updates=ScatteredUpdates1}}
    end.

send_commit(timeout, SD0=#state{scattered_updates=ScatteredUpdates, tx_id=TxId, commit_clock=CommitClock, timestamp=TimeStamp}) ->
    Transaction = #transaction{vec_snapshot_time=null,
                               txn_id=TxId},
    lists:foreach(fun(Slice) ->
                    {IndexNode, ListUpdates} = Slice,
                    eiger_vnode:commit(IndexNode, Transaction , ListUpdates, nodeps, TimeStamp, CommitClock, 1)
                  end, dict:to_list(ScatteredUpdates)),
    {next_state, gather_commit, SD0}.

gather_commit({committed, Clock}, SD0=#state{vnode=Vnode, n_partitions=NPartitions, ack=Ack0, tx_id=TxId}) ->
    %lager:info("Committed: received= ~p, total= ~p", [Ack0+1, NPartitions]),
    ok = eiger_vnode:update_clock(Vnode, Clock),
    Ack1 = Ack0 + 1,
    case Ack1 of
        NPartitions ->
            eiger_vnode:clean_propagated_tx_fsm(Vnode, TxId, self()),
            {next_state, idle, SD0#state{ack=Ack1}};
        _ ->
            {next_state, gather_commit, SD0#state{ack=Ack1}}
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

contains(_Key, []) ->
    false;

contains(Key, [H|T]) ->
    case H  of
        Key -> true;
        _ -> contains(Key, T)
    end.
