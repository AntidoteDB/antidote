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
%% @doc The coordinator for a given Clock SI interactive tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible ec_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(ec_interactive_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute_op/3, finish_op/3, prepare/2, prepare_2pc/2, committing/2, committing_2pc/3,
         receive_committed/2,
         receive_prepared/2, receive_batch_read/2, single_committing/2, abort/2,
         reply_to_client/2]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    tx_id: tx_id id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: tx_id prepare time.
%%    commit_time: tx_id commit time.
%%    state: state of the tx_id: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
          from,
          tx_id :: txid(),
          updated_partitions :: list(),
          num_to_ack :: integer(),
          buffer=dict:new() :: dict(),
          batch_read_set=[] :: list(),
          commit_protocol :: term(),
          state:: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From) ->
    gen_fsm:start_link(?MODULE, [From], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From]) ->
    TxId = #tx_id{time_id=ec_vnode:now_microsec(erlang:now()), server_pid=self()},
    SD = #state{
            tx_id = TxId,
            updated_partitions=[]},
    From ! {ok, TxId},
    {ok, execute_op, SD}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op({Op_type, Args}, Sender,
           SD0=#state{tx_id=TxId,
                      buffer=Buffer0,
                      updated_partitions=UpdatedPartitions0}) ->
    case Op_type of
        prepare ->
            case Args of
            two_phase ->
                {next_state, prepare_2pc, SD0#state{from=Sender, commit_protocol=Args}, 0};
            _ ->
                {next_state, prepare, SD0#state{from=Sender, commit_protocol=Args}, 0}
            end;
        read ->
            {Key, Type}=Args,
            Preflist = log_utilities:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            Updates = case dict:find(IndexNode, Buffer0) of
                        {ok, Dict0} ->
                            case dict:find(Key, Dict0) of
                                {ok, List} ->
                                    List;
                                error ->
                                    []
                            end;
                        error ->
                            []
                       end,
            case ec_vnode:read_data_item(IndexNode, TxId,
                                              Key, Type, Updates) of
                {error, Reason} ->
                    {reply, {error, Reason}, abort, SD0};
                {ok, Snapshot} ->
                    ReadResult = Type:value(Snapshot),
                    {reply, {ok, ReadResult}, execute_op, SD0}
            end;
            
        batch_read ->
        	{ReadBuffer, ReadPartitions}=Args,
        	lists:foreach(fun(Vnode) ->
                            Reads = dict:fetch(Vnode, ReadBuffer),
                            ec_vnode:batch_read(Vnode, TxId, dict:to_list(Reads))
                          end, ReadPartitions),
            {next_state, receive_batch_read, SD0#state{num_to_ack=length(ReadPartitions), from=Sender}};
        
        update ->
            {Key, Type, Param}=Args,
            Preflist = log_utilities:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            case dict:find(IndexNode, Buffer0) of
                {ok, Dict0} ->
                    Dict1 = dict:append(Key, {Type, Param}, Dict0),
                    UpdatedPartitions = UpdatedPartitions0;
                error ->
                    Dict0 = dict:new(),
                    Dict1 = dict:append(Key, {Type, Param}, Dict0),
                    UpdatedPartitions = UpdatedPartitions0 ++ [IndexNode]
            end,
            Buffer = dict:store(IndexNode, Dict1, Buffer0),
            {reply, ok, execute_op, SD0#state{updated_partitions=UpdatedPartitions, buffer=Buffer}}
    end.
    
    
%% @doc in this state, the fsm waits for the read results of each batch-read partition
%%      and gathers all the results for forwarding them.
receive_batch_read({batch_read_result, PartitionReadSet},
                 S0=#state{num_to_ack=NumToAck,
                 			from=From,
                           batch_read_set=BatchReadSet}) ->
    BatchReadSet1 = lists:append(BatchReadSet, PartitionReadSet),
    case NumToAck of 1 ->            
			%{reply, {ok, BatchReadSet1}, execute_op, S0};
			gen_fsm:reply(From, {ok, BatchReadSet1}),
			{next_state, execute_op, S0#state{num_to_ack= NumToAck-1, batch_read_set=BatchReadSet1}};
        _ ->
            {next_state, receive_batch_read,
             S0#state{num_to_ack= NumToAck-1, batch_read_set=BatchReadSet1}}
    end;
    
    
receive_batch_read({error, Reason}, S0=#state{from=From}) ->
	gen_fsm:reply(From, {error, Reason}),
    {next_state, abort, S0, 0}.
    
      

%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(timeout, SD0=#state{
                        tx_id = TxId,
                        buffer = Buffer,
                        updated_partitions=Updated_partitions, from=_From}) ->
    case length(Updated_partitions) of
        0->
            {next_state, reply_to_client,
            SD0#state{state=committed}, 0};
        1-> 
            Updates = dict:fetch(hd(Updated_partitions), Buffer),
            ec_vnode:pre_prepare(Updated_partitions, TxId, dict:to_list(Updates), single),
            {next_state, single_committing,
            SD0#state{state=committing, num_to_ack=1}};
        _->
            lists:foreach(fun(Partition) ->
                            Updates = dict:fetch(Partition, Buffer),
                            ec_vnode:pre_prepare(Partition, TxId, dict:to_list(Updates), multi)
                          end, Updated_partitions),
            Num_to_ack=length(Updated_partitions),
            {next_state, receive_prepared,
            SD0#state{num_to_ack=Num_to_ack, state=prepared}}
    end.
%% @doc state called when 2pc is forced independently of the number of partitions
%%      involved in the txs.
prepare_2pc(timeout, SD0=#state{
                        tx_id = TxId,
                        buffer = Buffer,
                        updated_partitions=Updated_partitions, from=From}) ->
    case length(Updated_partitions) of
        0->
            gen_fsm:reply(From, {ok, TxId}),
            {next_state, committing_2pc,
            SD0#state{state=committing}};
        _->
            lists:foreach(fun(Partition) ->
                            Updates = dict:fetch(Partition, Buffer),
                            ec_vnode:pre_prepare(Partition, TxId, dict:to_list(Updates), multi)
                          end, Updated_partitions),
            Num_to_ack=length(Updated_partitions),
            {next_state, receive_prepared,
            SD0#state{num_to_ack=Num_to_ack, state=prepared}}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared(prepared,
                 S0=#state{num_to_ack=NumToAck,
                           commit_protocol=CommitProtocol,
                           from=From}) ->
    case NumToAck of 1 ->
            case CommitProtocol of
                two_phase ->
                    gen_fsm:reply(From, ok),
                    {next_state, committing_2pc,
                     S0#state{state=committing}};
                _ ->
                    {next_state, committing,
                     S0#state{state=committing}, 0}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing(committed, S0=#state{from=_From}) ->
    {next_state, reply_to_client, S0#state{state=committed}, 0};
    
single_committing(abort, S0) ->
    {next_state, abort, S0, 0}.

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state expects other process to sen the commit message to 
%%      start the commit phase.
committing_2pc(commit, Sender, SD0=#state{tx_id = TxId,
                              updated_partitions=Updated_partitions}) ->
    
    NumToAck=length(Updated_partitions),
        case NumToAck of
        0 ->
                            {next_state, reply_to_client,
                             SD0#state{state=committed, from=Sender},0};
                    _ ->            ec_vnode:commit(Updated_partitions, TxId),
            {next_state, receive_committed,
             SD0#state{num_to_ack=NumToAck, from=Sender, state=committing}}
    end.

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, SD0=#state{tx_id = TxId,
                              updated_partitions=Updated_partitions}) ->
    NumToAck=length(Updated_partitions),
    case NumToAck of
        0 ->
            {next_state, reply_to_client,
             SD0#state{state=committed},0};
        _ ->
            ec_vnode:commit(Updated_partitions, TxId),
            {next_state, receive_committed,
             SD0#state{num_to_ack=NumToAck, state=committing}}
    end.

%% @doc the fsm waits for acks indicating that each partition has successfully

%% @doc the fsm waits for acks indicating that each partition has successfully
%%committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            {next_state, reply_to_client, S0#state{state=committed}, 0};
        _ ->
           {next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.

%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the tx_id aborts.
abort(timeout, SD0=#state{tx_id = TxId,
                          updated_partitions=UpdatedPartitions}) ->
    ec_vnode:abort(UpdatedPartitions, TxId),
    {next_state, reply_to_client, SD0#state{state=aborted},0};

abort(abort, SD0=#state{tx_id = TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ec_vnode:abort(UpdatedPartitions, TxId),
    {next_state, reply_to_client, SD0#state{state=aborted},0}.

%% @doc when the tx_id has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
reply_to_client(timeout, SD=#state{from=From, tx_id=TxId,
                                   state=TxState}) ->
    if undefined =/= From ->
            Reply = case TxState of
                        committed ->
                            {ok, TxId};
                        aborted->
                            {aborted, TxId};
                        Reason->
                            {TxId, Reason}
                    end,
            gen_fsm:reply(From,Reply);
       true -> ok
    end,
    {stop, normal, SD}.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
