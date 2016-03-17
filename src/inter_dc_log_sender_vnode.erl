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

%% Each logging_vnode informs this vnode about every new appended operation.
%% This vnode assembles operations into transactions, and sends the transactions to appropriate destinations.
%% If no transaction is sent in 10 seconds, heartbeat messages are sent instead.

-module(inter_dc_log_sender_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


%% API
-export([
  send/2]).

%% VNode methods
-export([
  init/1,
  start_vnode/1,
  handle_command/3,
  handle_coverage/4,
  handle_exit/3,
  handoff_starting/2,
  handoff_cancelled/1,
  handoff_finished/2,
  handle_handoff_command/3,
  handle_handoff_data/2,
  encode_handoff_item/2,
  is_empty/1,
  terminate/2,
  delete/1]).

%% Vnode state
-record(state, {
	  partition :: partition_id(),
	  buffer, %% log_tx_assembler:state
	  last_log_id :: dict(),
	  dcit :: dcid(),
	  timer :: any()
	 }).

%%%% API --------------------------------------------------------------------+

%% Send the new operation to the log_sender.
%% The transaction will be buffered until all the operations in a transaction are collected,
%% and then the transaction will be broadcasted via interDC.
%% WARNING: only LOCALLY COMMITED operations (not from remote DCs) should be sent to log_sender_vnode.
-spec send(partition_id(), #operation{}, non_neg_integer() | none) -> ok.
send(Partition, Operation, PrepTime) -> dc_utilities:call_vnode(Partition, inter_dc_log_sender_vnode_master, {log_event, Operation, PrepTime}).

%%%% VNode methods ----------------------------------------------------------+

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    PartList = dc_utilities:get_all_partitions(),
    {ok, #state{
	    partition = Partition,
	    partition_index = partition_index(Partition, PartitionList, 1),
	    buffer = log_txn_assembler:new_state(),
	    last_log_id = dict:new(),
	    dcid = dc_utilities:get_my_dcid(),
	    dc_part_dict = dict:new(),
	    timer = none
	   }}.

partition_index(Partition, [Partition, _], Index) ->
    Index;
partition_index(Partition, [_, Rest], Index) ->
    partition_index(Partition, Rest, Index + 1).

%% Start the timer
handle_command({start_timer}, _Sender, State) ->
    {reply, ok, set_timer(true, State)};

%% Handle the new operation
handle_command({log_event, Operation, PrepTime}, _Sender, State) ->
    %% Use the txn_assembler to check if the complete transaction was collected.
    {Result, NewBufState} = log_txn_assembler:process(Operation, State#state.buffer),
    State1 = State#state{buffer = NewBufState, dc_partitions=DCPartDict},
    State2 = case Result of
		 %% If the transaction was collected
		 {ok, Ops} ->
		     {Txns, NewIdDict, NewDCPartDict} =
			 inter_dc_txn:ops_to_dc_transactions(Ops, State1#state.partition, State1#state.partition_index,
							     State1#state.dcid, State1#state.last_log_id, State1#state.dc_part_dict),
		     NextState = broadcast(State1#state{last_log_id = NewIdDict, dc_part_dict = NewDCPartDict}, Txns),

		     %% Store the same safe sent time for all partitions
		     %% Store the ids for each partition
		     %% The ids are the number of messages sent on each connection,
		     %% These are used when sending the safe time so that it is sure that
		     %% no message was lost
		     ok = safe_time_functions:put_in_meta_data(PrepTime, NewIdDict),
		     NextState;
		 %% If the transaction is not yet complete
		 none -> State1
	     end,
    {noreply, State2};

handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

%% Handle the ping request, managed by the timer (1s by default)
handle_command(ping, _Sender, State#state{partition = Partition, last_log_id = NewIdDict}) ->
    %% THERE IS A CONCURRENCY BUG HERE
    %% because could get the stable time larger than a transaction that is in
    %% your message queue to be sent
    %% can be fixed by not advancing stable time in ClocksiVnode until the "log_event"
    %% operation has completed here
    ok = safe_time_functions:put_in_meta_data(get_stable_time(Partition), NewIdDict),
    case shouldPing() of
	true ->
	    {ok, Ids} = safe_time_functions:get_stable_external_ids(),
	    PingTxn = inter_dc_txn:ping(State#state.partition, State#state.last_log_id, Ids),
	    {noreply, set_timer(broadcast(State, PingTxn))};
	false ->
	    {noreply, set_timer(State)}
    end.

handle_coverage(_Req, _KeySpaces, _Sender, State) -> 
    {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> 
    {noreply, State}.
handoff_starting(_TargetNode, State) -> 
    {true, State}.
handoff_cancelled(State) ->
    {ok, set_timer(State)}.
handoff_finished(_TargetNode, State) -> 
    {ok, State}.
handle_handoff_command( _Message , _Sender, State) -> 
    {noreply, State}.
handle_handoff_data(_Data, State) -> 
    {reply, ok, State}.
encode_handoff_item(Key, Operation) -> 
    term_to_binary({Key, Operation}).
is_empty(State) -> 
    {true, State}.
delete(State) -> 
    {ok, State}.
terminate(_Reason, State) ->
    _ = del_timer(State),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%

%% Cancels the ping timer, if one is set.
-spec del_timer(#state{}) -> #state{}.
del_timer(State = #state{timer = none}) -> State;
del_timer(State = #state{timer = Timer}) ->
  _ = erlang:cancel_timer(Timer),
  State#state{timer = none}.

%% Cancels the previous ping timer and sets a new one.
-spec set_timer(#state{}) -> #state{}.
set_timer(State) ->
    set_timer(false,State).

-spec set_timer(boolean(), #state{}) -> #state{}.
set_timer(First, State = #state{partition = Partition}) ->
    case First of
	true ->
	    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
	    Node = riak_core_ring:index_owner(Ring, Partition),
	    MyNode = node(),
	    case Node of
		MyNode ->
		    State1 = del_timer(State),
		    State1#state{timer = riak_core_vnode:send_command_after(?HEARTBEAT_PERIOD, ping)};
		_Other ->
		    State
	    end;
	false ->
	    State1 = del_timer(State),
	    State1#state{timer = riak_core_vnode:send_command_after(?HEARTBEAT_PERIOD, ping)}
    end.
		
%% Broadcasts the transaction via local publisher.
-spec broadcast(#state{}, #interdc_txn{}) -> #state{}.
broadcast(State, Txns) ->
    inter_dc_pub:broadcast(Txns),
    State.

%% @doc Return smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
-spec get_stable_time(partition_id()) -> non_neg_integer().
get_stable_time(Partition) ->
    {ok, Time} = clocksi_vnode:get_min_prepared(Partition),
    Time.

-spec store_time_in_meta_data(non_neg_integer(), partition_id()) -> ok.
store_time_in_meta_data(Time, Partition) ->
    DCs = replication_check:get_dc_ids(false),
    NewClock = lists:foldl(fun(DCID, Acc) ->
				dict:store(DCID, Time, Acc)
			end, dict:new(), DCs),
    ok = meta_data_sender:put_meta_dict(stableExternal, Partition, NewClock).

%% -spec time_to_dc_dict(non_neg_integer()) -> dict().
%% time_to_dc_dict(Time) ->
