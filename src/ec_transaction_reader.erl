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
-module(ec_tx_id_reader).

-include("antidote.hrl").

-record(state, {partition :: partition_id(),
                logid :: log_id(),
                last_read_opid :: empty | op_id(),
                pending_operations :: dict(),
               }).

-export([init/2,
         get_next_tx_ids/1,
         get_update_ops_from_tx_id/1,
         get_prev_stable_time/1]).


%% @doc Returns an iterator to read tx_ids from a partition
%%  tx_ids can be read using get_next_tx_ids
-spec init(Partition::partition_id()) -> {ok, #state{}}.
init(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    [Preflist] = lists:filtermap(fun([H|_T]) ->
                                         case H of
                                             {Partition,_} ->
                                                 true;
                                             _ ->
                                                 false
                                         end
                                 end, GrossPreflists),
    LogId = [ P || {P, _Node} <- Preflist],
    {ok, #state{partition = Partition,
                logid = LogId,
                last_read_opid = empty,
                pending_operations = dict:new()}}.

%% @doc get_next_tx_ids takes the iterator returned by init
%%  it returns new iterator and a list of committed tx_ids in
%%  commit time order. TxIds which are not committed will not be returned
%% TODO: Deal with tx_ids committed in other DCs
-spec get_next_tx_ids(State::#state{}) -> {#state{}, [tx_id()]}.
get_next_tx_ids(State=#state{partition = Partition,
                                   logid = LogId,
                                   pending_operations = Pending,
                                   pending_commit_records = PendingCommitRecords,
                                   prev_stable_time = PrevStableTime,
                                   last_read_opid = Last_read_opid}
                     ) ->
    Node = {Partition, node()},
    %% No tx_ids will commit in future with commit time < stable_time
    %% So it is safe to read all tx_ids committed before stable_time
    {ok, NewOps} = read_next_ops(Node, LogId, Last_read_opid),
    case NewOps of
        [] -> Newlast_read_opid = Last_read_opid;
        _ -> Newlast_read_opid = get_last_opid(NewOps)
    end,

    {PendingOperations, Commitrecords} =
        add_to_pending_operations(Pending, PendingCommitRecords, NewOps),

    Txns = get_sorted_commit_records(Commitrecords),
    %% "Before" contains all tx_ids committed before stable_time
    {Before, After} = lists:splitwith(
                        fun(Logrecord) ->
                                {{_Dcid, CommitTime}, _} = Logrecord#log_record.op_payload,
                                CommitTime < Stable_time
                        end,
                        Txns),
    {NewPendingOps, ListTxIds} =
        lists:foldl(
          fun(_Logrecord=#log_record{tx_id=TxId},
              {PendingOps, TxIds}) ->
                  {ok, Ops} = get_ops_of_txn(TxId,PendingOps),
                  Txn = construct_tx_id(Ops),
                  NewTxIds = TxIds ++ [Txn],
                  NewPending = remove_txn_from_pending(TxId, PendingOps),
                  {NewPending, NewTxIds}
          end, {PendingOperations, []},
          Before),
    NewState = State#state{pending_operations = NewPendingOps,
                           pending_commit_records = After,
                           prev_stable_time = Stable_time,
                           last_read_opid = Newlast_read_opid},
    {NewState, ListTxIds}.

%% @doc returns all update operations in a txn in #ec_payload{} format
-spec get_update_ops_from_tx_id(TxId::tx_id()) ->
                                             [#ec_payload{}].
get_update_ops_from_tx_id(TxId) ->
    {_TxId, {DcId, CommitTime}, VecSnapshotTime, Ops} = TxId,
    Downstreamrecord =
        fun(_Operation=#operation{payload=Logrecord}) ->
                case Logrecord#log_record.op_type of
                    update ->
                        {Key, Type, Op} = Logrecord#log_record.op_payload,
                        _NewRecord = #ec_payload{
                                        key = Key,
                                        type = Type,
                                        op_param = Op,
                                        snapshot_time = VecSnapshotTime,
                                        commit_time = {DcId, CommitTime},
                                        txid =  Logrecord#log_record.tx_id
                                       };
                    _ ->
                        nothing
                end
        end,

    lists:foldl( fun(Op, ListsOps) ->
                         case Downstreamrecord(Op) of
                             nothing ->
                                 ListsOps;
                             Record ->
                                 ListsOps ++ [Record]
                         end
                 end, [], Ops).

-spec get_prev_stable_time(Reader::#state{}) -> non_neg_integer().
get_prev_stable_time(Reader) ->
    Reader#state.prev_stable_time.


%% ---- Internal functions ----- %%

%% @doc construct_tx_id: Returns a structure of type tx_id()
%% from a list of update operations and prepare/commit records
-spec construct_tx_id(Ops::[#operation{}]) -> tx_id().
construct_tx_id(Ops) ->
    Commitoperation = lists:last(Ops),
    Commitrecord = Commitoperation#operation.payload,
    {CommitTime, VecSnapshotTime} = Commitrecord#log_record.op_payload,
    TxId = Commitrecord#log_record.tx_id,
    {TxId, CommitTime, VecSnapshotTime, Ops}.

read_next_ops(Node, LogId, Last_read_opid) ->
    case Last_read_opid of
        empty ->
            logging_vnode:read(Node, LogId);
        _ ->
            logging_vnode:read_from(Node, LogId, Last_read_opid)
    end.

get_ops_of_txn(TxId, PendingOps) ->
    dict:find(TxId, PendingOps).

remove_txn_from_pending(TxId, PendingOps) ->
    dict:erase(TxId, PendingOps).

get_last_opid(Ops) ->
    Last = lists:last(Ops),
    {_Logid, Operation} = Last,
    Operation#operation.op_number.

%%@doc return all txnIds in sorted order committed befor stabletime
get_sorted_commit_records(Commitrecords) ->
    %%sort txns
    CompareFun = fun(Commitrecord1, Commitrecord2) ->
                         {{_DcId, CommitTime1},_} = Commitrecord1#log_record.op_payload,
                         {{_DcId, CommitTime2},_} = Commitrecord2#log_record.op_payload,
                         CommitTime1 =< CommitTime2
                 end,
    lists:sort(CompareFun, Commitrecords).

%% @doc Return smallest snapshot time of active tx_ids.
%%      No new updates with smaller timestamp will occur in future.
get_stable_time(Node, Prev_stable_time) ->
    case riak_core_vnode_master:sync_command(
           Node, {get_active_txns}, ?EC_MASTER) of
        {ok, Active_txns} ->
            lists:foldl(fun({_,{_TxId, Snapshot_time}}, Min_time) ->
                                case Min_time > Snapshot_time of
                                    true ->
                                        Snapshot_time;
                                    false ->
                                        Min_time
                                end
                        end,
                        ec_vnode:now_microsec(erlang:now()),
                        Active_txns);
        _ -> Prev_stable_time
    end.


%%@doc Add updates in writeset ot Pending operations to process downstream
add_to_pending_operations(Pending, Commitrecords, Ops) ->
    case Ops of
        [] ->
            {Pending,Commitrecords};
        _ ->
            lists:foldl(
              fun(Op, {ListPending, ListCommits}) ->
                      {_Logid, Operation} = Op,
                      Logrecord = Operation#operation.payload,
                      TxId = Logrecord#log_record.tx_id,
                      case Logrecord#log_record.op_type of
                          commit ->
                              %{{Dc,_CT},_ST} = Logrecord#log_record.op_payload,
                              %case Dc of
                                  %DcId ->
                                      NewCommit = ListCommits ++ [Logrecord],
                                      NewPending =
                                          dict:append(
                                            TxId, Operation, ListPending);
                                  %_ ->
                                      %NewCommit=ListCommits,
                                      %NewPending = dict:erase(TxId, ListPending)
                              %end;
                          abort ->
                              NewCommit = ListCommits,
                              NewPending = dict:erase(TxId, ListPending);
                          _ ->
                              NewPending =
                                  dict:append(TxId, Operation, ListPending),
                              NewCommit=ListCommits
                      end,
                      {NewPending, NewCommit}
              end,
              {Pending, Commitrecords}, Ops)
    end.
