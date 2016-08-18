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

%% Transaction buffer, used to check for message loss through operation log id gaps.

-module(inter_dc_sub_buf).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% Expected time to wait until the logging_vnode is started
-define(LOG_STARTUP_WAIT, 1000).

%% API
-export([
  new_state/2,
  process/2]).

%%%% API --------------------------------------------------------------------+

-spec new_state(pdcid(),partition_id()) -> #inter_dc_sub_buf{}.
new_state(PDCID,LocalPartition) -> #inter_dc_sub_buf{
  local_partition = LocalPartition,
  state_name = normal,
  pdcid = PDCID,
  last_observed_opid = init,
  queue = queue:new()
}.

-spec process({txn, #interdc_txn{}} | {log_reader_resp, [#interdc_txn{}]}, #inter_dc_sub_buf{}) -> #inter_dc_sub_buf{}.
process({txn, Txn=#interdc_txn{dcid=DCID, partition=Partition}},
	State = #inter_dc_sub_buf{last_observed_opid = init, pdcid = {DCID, Partition}, local_partition = MyPartition}) ->
    %% If this is the first txn received (i.e. if last_observed_opid = init) then check the log
    %% to see if there was a previous op received (i.e. in the case of fail and restart) so that
    %% you can check for duplicates or lost messages
    %% Operations are orderd for each {DCID,Partition} by the origin DC
    %% Assuming the partitioning scheme might be different at the local DC we have to check all local partitions
    %% for the max updates from the sending DC, Partition pair
    LocalPartitions = dc_meta_data_utilities:get_my_partitions_list(),
    OpIds = 
	lists:foldl(fun(LocalPartition,Acc) ->
			    Result = 
				try
				    logging_vnode:request_op_id(dc_utilities:partition_to_indexnode(LocalPartition),
								DCID, Partition)
				catch
				    _:Reason ->
					lager:info("Error loading last opid from log: ~w, will retry", [Reason]),
					error
				end,
			    case Result of
				{ok, OpId} when is_list(Acc) ->
				    [OpId | Acc];
				_ ->
				    error
			    end
		    end, [], LocalPartitions),
    case OpIds of
	_ when is_list(OpIds) ->
	    MaxOpId = lists:max(OpIds),
	    lager:info("Loaded opid ~p from log for dc ~p, partition, ~p, at local partition ~p", [MaxOpId, DCID, Partition, MyPartition]),
	    process({txn, Txn}, State#inter_dc_sub_buf{last_observed_opid=MaxOpId});
	error ->
	    riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, {txn, Txn}),
	    State
    end;
process({txn, Txn}, State = #inter_dc_sub_buf{state_name = normal}) -> process_queue(push(Txn, State));
process({txn, Txn}, State = #inter_dc_sub_buf{state_name = buffering}) ->
  lager:info("Buffering txn in ~p", [State#inter_dc_sub_buf.pdcid]),
  push(Txn, State);

process({log_reader_resp, Txns}, State = #inter_dc_sub_buf{queue = Queue, state_name = buffering, local_partition = LocalPartition}) ->
    ok = lists:foreach(fun(Txn) -> deliver(Txn,LocalPartition) end, Txns),
    NewLast = case queue:peek(Queue) of
		  empty -> State#inter_dc_sub_buf.last_observed_opid;
		  {value, Txn} -> get_prev_op_id(Txn)
	      end,
    NewState = State#inter_dc_sub_buf{last_observed_opid = NewLast},
    process_queue(NewState).

%%%% Methods ----------------------------------------------------------------+
%% Returns the id of the last operation of the previous transaction
-spec get_prev_op_id(#interdc_txn{}) -> non_neg_integer().
get_prev_op_id(Txn) ->
    case ?IS_PARTIAL() of
	false ->
	    Txn#interdc_txn.prev_log_opid#op_number.local;
	true ->
	    DCID = dc_meta_data_utilities:get_my_dc_id(),
	    %%lager:info("the list of prev op ids ~p and the op list", [Txn#interdc_txn.prev_log_opid_dc]),
	    case lists:keyfind(DCID,1,Txn#interdc_txn.prev_log_opid_dc) of
		{DCID, Time} ->
		    Time#op_number.local;
		false ->
		    %% The DCs are not yet connected, so this should just be a new ping
		    %% fail if not
		    %% true = inter_dc_txn:is_ping(Txn),
		    0
	    end
    end.

%% Returns the id of the last operation from this transaction
-spec get_last_op_id(#interdc_txn{},non_neg_integer()) -> non_neg_integer().
get_last_op_id(Txn,PrevLast) ->
    {Id, DCIDList} = inter_dc_txn:last_log_opid(Txn),
    case ?IS_PARTIAL() of
	false ->
	    Id#op_number.local;
	true ->
	    DCID = dc_meta_data_utilities:get_my_dc_id(),
	    case lists:keyfind(DCID,1,DCIDList) of
		{DCID, Time} ->
		    Time#op_number.local;
		false ->
		    %% The DCs are not yet connected, so this should just be a new ping
		    %% fail if not
		    %% true = inter_dc_txn:is_ping(Txn),
		    PrevLast
	    end
    end.

-spec process_queue(#inter_dc_sub_buf{}) -> #inter_dc_sub_buf{}.
process_queue(State = #inter_dc_sub_buf{queue = Queue, last_observed_opid = Last, local_partition = LocalPartition}) ->
  case queue:peek(Queue) of
    empty -> State#inter_dc_sub_buf{state_name = normal};
    {value, Txn} ->
      TxnLast = get_prev_op_id(Txn),
      case cmp(TxnLast, Last) of

      %% If the received transaction is immediately after the last observed one
        eq ->
          deliver(Txn,LocalPartition),
          Max = get_last_op_id(Txn,Last),
          process_queue(State#inter_dc_sub_buf{queue = queue:drop(Queue), last_observed_opid = Max});

      %% If the transaction seems to come after an unknown transaction, ask the remote log
        gt ->
          lager:info("Whoops, lost message. New is ~p, last was ~p. Asking the remote DC ~p, is a ping ~p",
		     [TxnLast, Last, State#inter_dc_sub_buf.pdcid, inter_dc_txn:is_ping(Txn)]),
          case query(State#inter_dc_sub_buf.pdcid, State#inter_dc_sub_buf.last_observed_opid + 1, TxnLast) of
            ok ->
              State#inter_dc_sub_buf{state_name = buffering};
            _ ->
              lager:warning("Failed to send log query to DC, will retry on next ping message"),
              State#inter_dc_sub_buf{state_name = normal}
          end;

      %% If the transaction has an old value, drop it.
        lt ->
	      lager:warning("Dropping duplicate message ~w, last time was ~w", [Txn, Last]),
	      process_queue(State#inter_dc_sub_buf{queue = queue:drop(Queue)})
      end
  end.

-spec deliver(#interdc_txn{},partition_id()) -> ok.
deliver(Txn,LocalPartition) ->
    PartTxnList = inter_dc_txn:to_local_txn_partition_list(LocalPartition,Txn),
    lists:foreach(fun({Partition,ATxn}) ->
			  inter_dc_dep_vnode:handle_transaction(ATxn,Partition,LocalPartition)
		  end, PartTxnList).

%% TODO: consider dropping messages if the queue grows too large.
%% The lost messages would be then fetched again by the log_reader.
-spec push(#interdc_txn{}, #inter_dc_sub_buf{}) -> #inter_dc_sub_buf{}.
push(Txn, State) -> State#inter_dc_sub_buf{queue = queue:in(Txn, State#inter_dc_sub_buf.queue)}.

%% Instructs the log reader to ask the remote DC for a given range of operations.
%% Instead of a simple request/response with blocking, the result is delivered
%% asynchronously to inter_dc_sub_vnode.
-spec query(pdcid(), log_opid(), log_opid()) -> ok | unknown_dc.
query({DCID,Partition}, From, To) ->
    BinaryRequest = term_to_binary({read_log, Partition, From, To}),
    inter_dc_query:perform_request(?LOG_READ_MSG, {DCID, Partition}, BinaryRequest, fun inter_dc_sub_vnode:deliver_log_reader_resp/2,
				   infinity, none, self()).

cmp(A, B) when A > B -> gt;
cmp(A, B) when B > A -> lt;
cmp(_, _) -> eq.
