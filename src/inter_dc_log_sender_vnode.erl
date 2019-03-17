%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
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
     send/2,
     update_last_log_id/2,
     start_timer/1,
     send_stable_time/2]).

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
  delete/1,
  handle_overload_command/3,
  handle_overload_info/2]).

%% Vnode state
-record(state, {
  partition :: partition_id(),
  buffer, %% log_tx_assembler:state
  last_log_id :: #op_number{},
  timer :: any()
}).

%%%% API --------------------------------------------------------------------+

%% Send the new operation to the log_sender.
%% The transaction will be buffered until all the operations in a transaction are collected,
%% and then the transaction will be broadcasted via interDC.
%% WARNING: only LOCALLY COMMITED operations (not from remote DCs) should be sent to log_sender_vnode.
-spec send(partition_id(), #log_record{}) -> ok.
send(Partition, LogRecord) -> dc_utilities:call_vnode(Partition, inter_dc_log_sender_vnode_master, {log_event, LogRecord}).

%% Start the heartbeat timer
-spec start_timer(partition_id()) -> ok.
start_timer(Partition) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_vnode_master, {start_timer}).

%% After restarting from failure, load the operation id of the last operation sent by this DC
%% Otherwise the stable time won't advance as the receiving DC will be thinking it is getting old messages
-spec update_last_log_id(partition_id(), #op_number{}) -> ok.
update_last_log_id(Partition, OpId) -> dc_utilities:call_vnode_sync(Partition, inter_dc_log_sender_vnode_master, {update_last_log_id, OpId}).

%% Send the stable time to this vnode, no transaction in the future will commit with a smaller time
-spec send_stable_time(partition_id(), non_neg_integer()) -> ok.
send_stable_time(Partition, Time) ->
    dc_utilities:call_local_vnode(Partition, inter_dc_log_sender_vnode_master, {stable_time, Time}).

%%%% VNode methods ----------------------------------------------------------+

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
  {ok, #state{
    partition = Partition,
    buffer = log_txn_assembler:new_state(),
    last_log_id = #op_number{},
    timer = none
  }}.

%% Start the timer
handle_command({start_timer}, _Sender, State) ->
    {reply, ok, set_timer(true, State)};

handle_command({update_last_log_id, OpId}, _Sender, State = #state{partition = Partition}) ->
    logger:debug("Updating last log id at partition ~w to: ~w", [Partition, OpId]),
    {reply, ok, State#state{last_log_id = OpId}};

%% Handle the new operation
%% -spec handle_command({log_event, #log_record{}}, pid(), #state{}) -> {noreply, #state{}}.
handle_command({log_event, LogRecord}, _Sender, State) ->
  %% Use the txn_assembler to check if the complete transaction was collected.
  {Result, NewBufState} = log_txn_assembler:process(LogRecord, State#state.buffer),
  State1 = State#state{buffer = NewBufState},
  State2 = case Result of
    %% If the transaction was collected
    {ok, Ops} ->
      Txn = inter_dc_txn:from_ops(Ops, State1#state.partition, State#state.last_log_id),
      broadcast(State1, Txn);
    %% If the transaction is not yet complete
    none -> State1
  end,
  {noreply, State2};

handle_command({stable_time, Time}, _Sender, State) ->
    PingTxn = inter_dc_txn:ping(State#state.partition, State#state.last_log_id, Time),
    {noreply, set_timer(broadcast(State, PingTxn))};

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

%% Handle the ping request, managed by the timer (1s by default)
handle_command(ping, _Sender, State) ->
    get_stable_time(State#state.partition),
    {noreply, State}.

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
handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
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
    set_timer(false, State).

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
broadcast(State, Txn) ->
  inter_dc_pub:broadcast(Txn),
  Id = inter_dc_txn:last_log_opid(Txn),
  State#state{last_log_id = Id}.

%% @doc Sends an async request to get the smallest snapshot time of active transactions.
%%      No new updates with smaller timestamp will occur in future.
-spec get_stable_time(partition_id()) -> ok.
get_stable_time(Partition) ->
    ok = clocksi_vnode:send_min_prepared(Partition).
