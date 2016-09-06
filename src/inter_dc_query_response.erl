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

-module(inter_dc_query_response).
-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([start_link/1,
	 get_entries/2,
	 perform_external_read/2,
	 request_permissions/2,
	 generate_server_name/1]).
-export([init/1,
	 handle_cast/2,
	 handle_call/3,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-record(state, {
	  id :: non_neg_integer()}).

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_link(non_neg_integer()) -> {ok,pid()} | ignore | {error,term()}.
start_link(Num) ->
    gen_server:start_link({local,generate_server_name(Num)}, ?MODULE, [Num], []).

-spec get_entries(binary(),#inter_dc_query_state{}) -> ok.
get_entries(BinaryQuery,QueryState) ->
    ok = gen_server:cast(generate_server_name(random:uniform(?INTER_DC_QUERY_CONCURRENCY)), {get_entries,BinaryQuery,QueryState}).

-spec perform_external_read(binary(),#inter_dc_query_state{}) -> ok.
perform_external_read(BinaryQuery,QueryState) ->
    ok = gen_server:cast(generate_server_name(random:uniform(?INTER_DC_QUERY_CONCURRENCY)), {perform_external_read,BinaryQuery,QueryState}).

-spec request_permissions(binary(),#inter_dc_query_state{}) -> ok.
request_permissions(BinaryRequest,QueryState) ->
    ok = gen_server:cast(generate_server_name(random:uniform(?INTER_DC_QUERY_CONCURRENCY)), {request_permissions,BinaryRequest,QueryState}).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Num]) ->
    {ok, #state{id=Num}}.

handle_cast({get_entries,BinaryQuery,QueryState}, State) ->
    {read_log,Partition, From, To, FirstOpId, LastOpId, DCID} = binary_to_term(BinaryQuery),
    Entries = get_entries_internal(Partition,From,To,FirstOpId,LastOpId,DCID),
    BinaryResp = term_to_binary({{dc_meta_data_utilities:get_my_dc_id(),Partition},Entries}),
    BinaryPartition = inter_dc_txn:partition_to_bin(Partition),
    FullResponse = <<BinaryPartition/binary,BinaryResp/binary>>,
    ok = inter_dc_query_receive_socket:send_response(FullResponse,QueryState),
    {noreply, State};

handle_cast({perform_external_read,BinaryQuery,QueryState}, State) ->
    {external_read, Key, Type, Transaction, Property} = binary_to_term(BinaryQuery),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    lager:info("The external read req ~p", [binary_to_term(BinaryQuery)]),
    {ok,Snapshot} = clocksi_readitem_fsm:read_data_item(IndexNode,Key,Type,Transaction,[Property]),
    BinaryRep = term_to_binary({external_read_rep, Key, Type, Snapshot}),
    ok = inter_dc_query_receive_socket:send_response(BinaryRep,QueryState),
    {noreply, State};    

handle_cast({request_permissions,BinaryRequest,QueryState}, State) ->
    {request_permissions, Operation, _Partition, _From, _To} = binary_to_term(BinaryRequest),
    BinaryResp = BinaryRequest,
    ok = bcounter_mgr:process_transfer(Operation),
    ok = inter_dc_query_receive_socket:send_response(BinaryResp,QueryState),
    {noreply, State};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

-spec get_entries_internal(partition_id(), log_opid(), log_opid(), log_opid() | undefined, log_opid() | undefined, dcid()) -> [#interdc_txn{}].
get_entries_internal(Partition, From, To, FirstOpId, LastOpId, OtherDC) ->
    %% TODO: Trim the query correctly for partial replication
    Node = case lists:member(Partition,dc_utilities:get_my_partitions()) of
	       true -> node();
	       false ->
		   log_utilities:get_my_node(Partition)
	   end,
    Logs = log_read_range(Partition, Node, From, To),
    Asm = log_txn_assembler:new_state(),
    {OpLists, _} = log_txn_assembler:process_all(Logs, Asm),
    Txns = lists:map(fun(TxnOps) -> inter_dc_txn:from_ops(TxnOps, Partition, none, none) end, OpLists),
    %% This is done in order to ensure that we only send the transactions we committed.
    %% We can remove this once the read_log_range is reimplemented.
    LocalTxns = lists:filter(fun inter_dc_txn:is_local/1, Txns),
    case ?IS_PARTIAL() of
	false ->
	    LocalTxns;
	true ->
	    BucketTxns = lists:foldl(fun(Txn,Acc) ->
					     Acc ++ inter_dc_txn:to_per_bucket(Txn)
				     end, [], LocalTxns),
	    %% Filter out only the replicated operations
	    %% And the ones with the update op ids in the non-inclusive range from FirstOpId to LastOpId
	    lists:filter(fun(Txn = #interdc_txn{log_records = [FirstOp|_RestOps]}) ->
				 Payload = FirstOp#log_record.log_operation,
				 update = Payload#log_operation.op_type, %% sanity check
				 Bucket = 
				     case Payload#log_operation.log_payload#update_log_payload.key of
					 {_Key, Buck} ->
					     {bucket, Buck};
					 _ ->
					     undefined
				     end,
				 case partial_repli_utils:is_replicated_at_dc(Bucket, OtherDC) of
				     false -> false;
				     true ->
					 {_Id, DCIDList} = inter_dc_txn:last_log_opid(Txn),
					 {OtherDC, Time} = lists:keyfind(OtherDC,1,DCIDList),
					 (Time#op_number.local > FirstOpId) and (Time#op_number.local =< LastOpId)
				 end
			 end, BucketTxns)
    end.

%% TODO: reimplement this method efficiently once the log provides efficient access by partition and DC (Santiago, here!)
%% TODO: also fix the method to provide complete snapshots if the log was trimmed
-spec log_read_range(partition_id(), node(), log_opid(), log_opid()) -> [#log_record{}].
log_read_range(Partition, Node, From, To) ->
  {ok, RawOpList} = logging_vnode:read({Partition, Node}, [Partition]),
  OpList = lists:map(fun({_Partition, Op}) -> Op end, RawOpList),
  filter_operations(OpList, From, To).

-spec filter_operations([#log_record{}], log_opid(), log_opid()) -> [#log_record{}].
filter_operations(Ops, Min, Max) ->
  F = fun(Op) ->
    Num = Op#log_record.op_number#op_number.local,
    (Num >= Min) and (Max >= Num)
  end,
  lists:filter(F, Ops).

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_server_name(Id) ->
    list_to_atom("inter_dc_query_response" ++ integer_to_list(Id)).
