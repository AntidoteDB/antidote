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

%% API
-export([
  new_state/1,
  process/2]).

%% State
-record(state, {
  state_name :: normal | buffering,
  pdcid :: pdcid(),
  last_observed_opid :: dict(),
  queue :: queue()
}).

%%%% API --------------------------------------------------------------------+

%% TODO: Fetch last observed ID from durable storage (maybe log?). This way, in case of a node crash, the queue can be fetched again.
-spec new_state(pdcid()) -> #state{}.
new_state(PDCID) -> #state{
		       state_name = normal,
		       pdcid = PDCID,
		       last_observed_opid = dict:new(),
		       queue = queue:new()
		      }.

-spec process({txn, #interdc_txn{}} | {log_reader_resp, [#interdc_txn{}]}, #state{}) -> #state{}.
process({txn, Txn}, State = #state{state_name = normal}) -> process_queue(push(Txn, State));
process({txn, Txn}, State = #state{state_name = buffering}) ->
  lager:info("Buffering txn in ~p", [State#state.pdcid]),
  push(Txn, State);

process({log_reader_resp, Txns}, State = #state{queue = Queue, state_name = buffering}) ->
    ok = lists:foreach(fun deliver/1, Txns),
    NewLast = case queue:peek(Queue) of
		  empty -> State#state.last_observed_opid;
		  {value, Txn} -> Txn#interdc_txn.prev_log_opid
	      end,
    NewState = State#state{last_observed_opid = NewLast},
    process_queue(NewState).

%%%% Methods ----------------------------------------------------------------+
process_queue(State = #state{queue = Queue, last_observed_opid = LastDict, pdcid = {DCID,_}}) ->
    case queue:peek(Queue) of
	empty -> State#state{state_name = normal};
	{value, Txn} ->
	    DestPartition = Txn#interdc_txn.dest_partition,
	    FromPartition = Txn#interdc_txn.partition,
	    LastOpDict = case dict:find(DestPartition, LastDict) of
			     {ok, Dict} ->
				 Dict;
			     error ->
				 dict:new()
			 end,
	    case check_missing(Txn, LastOpDict) of
		%% If the received transaction is immediately after the last observed one
		[] ->
		    deliver(Txn),
		    Max = inter_dc_txn:last_log_opid(Txn),
		    process_queue(State#state{queue = queue:drop(Queue),
					      last_observed_opid = dict:store(DestPartition, dict:store(FromPartition, Max, LastOpDict), LastDict)});
		%% If the transaction has an old value, drop it.
		{lt, _} ->
		    lager:warning("Dropping duplicate message"),
		    process_queue(State#state{queue = queue:drop(Queue)});
		%% If the transaction seems to come after an unknown transaction, ask the remote log
		MissingList ->
		    Result = 
			lists:foldl(fun({gt, {FromPart, DestPart}, LastOp, TxnLast}, Acc) ->
					    lager:info("Whoops, lost message. Asking the remote DC ~p ~p here ~p", [DCID, FromPart, DestPart]),
					    case inter_dc_log_reader_query:query({DCID,FromPart}, DestPart, LastOp, TxnLast) of
						ok ->
						    Acc;
						_ ->
						    lager:warning("Failed to send log query to DC, will retry on next ping message"),
						    normal
					    end
				    end, buffering, MissingList),
		    State#state{state_name = Result}
	    end
    end.

-spec deliver(#interdc_txn{}) -> ok.
deliver(Txn) -> inter_dc_dep_vnode:handle_transaction(Txn).

%% TODO: consider dropping messages if the queue grows too large.
%% The lost messages would be then fetched again by the log_reader.
-spec push(#interdc_txn{}, #state{}) -> #state{}.
push(Txn, State) -> State#state{queue = queue:in(Txn, State#state.queue)}.

check_missing(Txn = #interdc_txn{operations=[], dcid = DCID, dest_partition = DestPart,
				 partition = FromPartition, timestamp = {Timestamp, DCOpIds}}, LastOpDict) ->
    %% This is a heartbeat
    %% Need to check that we have received all messages from all sender partitions
    %% with this node as the destination
    %% The DCIds in the Txn is a dict with key {senderPartition, destinationPartition} and value Id
    SenderPartitions = replication_check:get_dc_partitions_dict(DCID),
    %% Heartbeat, be sure you have all messages from all partitions from the sending DC
    dict:fold(fun(SenderPartition, _, Acc) ->
		      LastOpId = dict:fetch({SenderPartition,DestPart},DCOpIds),
		      case get_missing(LastOpId, Partition, DestPart, LastOpDict, Acc) of
			  {lt, _} ->
			      %% Could have already got more up to date messages than the heartbeat
			      %% so just continue
			      Acc;
			  NewAcc ->
			      NewAcc
		      end
	      end, [], SenderPartitions);
check_missing(Txn = #interdc_txn{prev_log_opid = TxnLast, dest_partition = DestPart, partition = Partition}, LastOpDict) ->
    %% Normal Txn, be sure you have all messages from the sending partition
    get_missing(TxnLast, Partition, DestPart, LastOpDict, []).

get_missing(TxnLast, Partition, DestPart, LastOpDict, Acc) ->
    LastOp = case dict:find(Partition, LastOpDict) of
		 {ok, OpId} ->
		     OpId;
		 error ->
		     0
	     end,
    case cmp(TxnLast,LastOpId) of
	eq ->
	    Acc;
	gt ->
	    [{gt, {Partition, DestPart}, LastOp + 1, TxnLast} | Acc];
	lt ->
	    {lt, Acc}
    end.

cmp(A, B) when A > B -> gt;
cmp(A, B) when B > A -> lt;
cmp(_, _) -> eq.
