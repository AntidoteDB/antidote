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

%% @doc : This module has functions to process updates from remote DCs

-module(inter_dc_repl_update).

-include("../include/inter_dc_repl.hrl").
-include("../include/antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init_state/1, enqueue_update/2, process_queue/1]).

init_state(Partition) ->
    {ok, #recvr_state{lastRecvd = orddict:new(), %% stores last OpId received
                      lastCommitted = orddict:new(),
                      recQ = orddict:new(),
                      partition=Partition,
		      partition_vclock = vectorclock:new()}
    }.

%% @doc enqueue_update: Put transaction into queue for processing later
-spec enqueue_update(Transaction::clocksi_transaction_reader:transaction(),
                     #recvr_state{}) ->
                            {ok, #recvr_state{}}.
enqueue_update({Transaction, From},
               State = #recvr_state{recQ = RecQ}) ->
    {_,{FromDC, _CommitTime},_,_} = Transaction,
    RecQNew = enqueue(FromDC, {Transaction, From}, RecQ),
    {ok, State#recvr_state{recQ = RecQNew}}.

%% Process one transaction from Q for each DC each Q.
%% This method must be called repeatedly
%% inorder to process all updates
process_queue(State=#recvr_state{recQ = RecQ}) ->
    {Ret, NewState} = orddict:fold(
			fun(K, V, {DepSat, Acc}) ->
				case process_q_dc(K, V, Acc) of				    
				    {ok, NS} ->
					{DepSat, NS};
				    {dep_not_sat, NS} ->
					{dep_not_sat, NS}
				end
			end, {ok, State}, RecQ),
    {Ret, NewState}.

%% private functions


%% Takes one transction from DC queue, checks whether its depV is satisfied
%% and apply the update locally.
process_q_dc(Dc, DcQ, StateData=#recvr_state{lastCommitted = _LastCTS,
					     recQ = RecQ,
                                             partition = Partition,
					     partition_vclock = LC}) ->
    case queue:is_empty(DcQ) of
        false ->
            {Transaction,_From} = queue:get(DcQ),
            {_TxId, CommitTime, VecSnapshotTime, Ops} = Transaction,

	    Operation = hd(Ops),
	    Logrecord = Operation#operation.payload,
	    %%Payload = Logrecord#log_record.op_payload,
	    Op_type = Logrecord#log_record.op_type,
	    case Op_type of
		safe_update ->
		    %% TODO: Before calling update_safe_clock,
		    %% should wait until all updates up to this time have been processed locally
		    %% (they all have been recieved, but not yet processed yet) otherwise some new
		    %% transactions might be blocked temporarily
		    {Dc, Ts} = CommitTime,
		    {ok, _} = vectorclock:update_safe_clock_local(Partition, Dc, Ts - 1),
		    DcQNew = queue:drop(DcQ),
		    RecQNew = set(Dc, DcQNew, RecQ),
		    {ok, StateData#recvr_state{recQ = RecQNew}};
		_ ->
		    %% Tyler: Sets the time of the sending DC to 0 because
		    %% ops are recieved in order by partition
		    SnapshotTime = vectorclock:set_clock_of_dc(
				     Dc, 0, VecSnapshotTime),
		    LocalDc = dc_utilities:get_my_dc_id(),
		    {Dc, Ts} = CommitTime,
		    %% Check for dependency of operations and write to log
		    %% Gets safe_clock from the partition (instead of partition clock)
		    %% {ok, LC} = vectorclock:get_safe_time(),
		    %% Sets the time of the local DC in the safe clock to the current time,
		    {ok, Stable} = vectorclock:get_stable_snapshot(),
		    LC1 = vectorclock:keep_max(Stable,LC),
		    LocalSafeClock = vectorclock:set_clock_of_dc(
				       Dc, 0,
				       vectorclock:set_clock_of_dc(
					 LocalDc, now_millisec(erlang:now()), LC1)),
		    %% LocalSafeClock = vectorclock:set_clock_of_dc(
		    %% 		       LocalDc, vectorclock:now_microsec(erlang:now()), LC),
		    %% It assumes it is a duplicate just if has a smaller CTS?
		    %% Maybe should keep a CTS per partition?
		    %% case orddict:find(Dc, LastCTS) of  % Check for duplicate
		    %%     {ok, CTS} ->
		    %%         if Ts >= CTS -> 
		    %% 		    %%lager:info("performing update1 ~p", [Transaction]),
		    %% 		    check_and_update(SnapshotTime, LocalSafeClock,
		    %%                                  Transaction,
		    %%                                  Dc, DcQ, Ts, StateData ) ;
		    %%            true ->
		    %%                 %% %% TODO: Not right way check duplicates
		    %%                 %% lager:info("Duplicate request, ~p, lastCTS ~p", [Transaction,LastCTS]),
		    %%                 %% {ok, NewState} = finish_update_dc(
		    %%                 %%                    Dc, DcQ, CTS, StateData),
		    %%                 %% %%Duplicate request, drop from queue
		    %%                 %% NewState
		    %% 		    check_and_update(SnapshotTime, LocalSafeClock,
		    %%                                  Transaction,
		    %%                                  Dc, DcQ, Ts, StateData )
		    
		    %%         end;
		    %%     _ ->
		    %% 	    %%lager:info("performing update2 ~p", [Transaction]),
		    %%         case check_and_update(SnapshotTime, LocalSafeClock, Transaction,
		    %% 				  Dc, DcQ, Ts, StateData) of
		    %% 		{ok, NewQ, NewS} ->
		    %% 		    process_q_dc(Dc, NewQ, NewS);
		    %% 		{dep_not_sat, NewS1} ->
		    %% 		    {dep_not_sat, NewS1}
		    %% 	    end
		    %% end;
		    
		    %% AM NOT CHECKING FOR DUPLICATES!!! Should fix this
		    case check_and_update(SnapshotTime, LocalSafeClock, Transaction,
					  Dc, DcQ, Ts, StateData) of
			{ok, NewQ, NewS} ->
			    %% From ! {From, done_process},
			    process_q_dc(Dc, NewQ, NewS);
			{dep_not_sat, NewS1} ->
			    {dep_not_sat, NewS1}
		    end
	    end;		
	true ->
	    {ok, StateData}
    end.

check_and_update(SnapshotTime, Localclock, Transaction,
                 Dc, DcQ, Ts,
                 StateData = #recvr_state{partition = Partition} ) ->
    {TxIdA,{DcIdA,CommitTimeA},VecSSA,Ops} = Transaction,
    Node = {Partition,node()},
    FirstOp = hd(Ops),
    FirstRecord = FirstOp#operation.payload,
    LogId = case FirstRecord#log_record.op_type of
                update ->
                    {Key1,_Type,_Op} = FirstRecord#log_record.op_payload,
                    log_utilities:get_logid_from_key(Key1);
                nonRepUpdate ->
                    {Key1,_Type,_Op} = FirstRecord#log_record.op_payload,
                    log_utilities:get_logid_from_key(Key1);
                noop ->
                    error;
                _ ->
                    lager:error("Wrong transaction record format"),
                    erlang:error(bad_transaction_record)
            end,
    case check_dep(SnapshotTime, Localclock) of
        true ->
            {TheNewOps,_NewWs,NewLogRecords}
		= lists:foldl(
		    fun(Op,{NewOps,Ws,NewRecords}) ->
			    %%lager:info("op to record: ~p", [Op]),
			    Logrecord = Op#operation.payload,
			    OpNum = Op#operation.op_number,
			    TxId = Logrecord#log_record.tx_id,
			    case Logrecord#log_record.op_type of
				noop ->
				    lager:debug("Heartbeat Received");
				nonRepUpdate ->
				    {Key2,Type2,Op2} = Logrecord#log_record.op_payload,
				    OrgTrans = #transaction{snapshot_time=CommitTimeA,
							    server_pid=DcIdA,vec_snapshot_time=VecSSA,txn_id=TxIdA},
				    {ok, Downstream} =
					clocksi_downstream:generate_downstream_op(OrgTrans,Node,Key2,Type2,Op2,Ws,[],local),
				    NewLogRecord = #log_record{tx_id=TxId,op_type=update,
							       op_payload={Key2,Type2,Downstream}},
				    NewOp = #operation{op_number=OpNum,payload=NewLogRecord},
				    {NewOps ++ [NewOp],Ws ++ [{isReplicated,Key2,Type2,Downstream}], NewRecords ++ [NewLogRecord]};
				update ->
				    {Key2,Type2,Op2} = Logrecord#log_record.op_payload,
				    {NewOps ++ [Op],Ws ++ [{isReplicated,Key2,Type2,Op2}], NewRecords ++ [Logrecord]};
				_ -> %% prepare or commit
				    lager:debug("Prepare/Commit record"),
				    %%TODO Write this to log
				    {NewOps ++ [Op],Ws, NewRecords ++ [Logrecord]}
			    end
		    end, {[],[], []}, Ops),
	    logging_vnode:append_group(Node, LogId, NewLogRecords),
	    NewTrans = {TxIdA,{DcIdA,CommitTimeA},VecSSA,TheNewOps},
            DownOps =
                clocksi_transaction_reader:get_update_ops_from_transaction(
                  NewTrans),
            lists:foreach( fun(DownOp) ->
                                   Key = DownOp#clocksi_payload.key,
                                   ok = materializer_vnode:update(Key, DownOp)
                           end, DownOps),
            %%TODO add error handling if append failed
	    finish_update_dc(
	      Dc, DcQ, Ts, StateData);
	false ->
            lager:error("Dep not satisfied ~p", [Transaction]),
            {dep_not_sat, StateData}
    end.

finish_update_dc(Dc, DcQ, Cts,
                 State=#recvr_state{lastCommitted = LastCTS,
				    partition_vclock = LC,
				    partition = _Partition,
				    recQ = RecQ}) ->
    %% Is it really needed to use -1 for the time
    %% I am doing this because that is what is in
    %% vectorclock_vnode update_clock
    %% LC shouldnt be necessary for partial replication
    NewLC = vectorclock:set_clock_of_dc(Dc, Cts, LC),
    %% dont do stable time in local computation
    %%ok = meta_data_sender:put_meta_dict(stable, Partition, NewLC),
    DcQNew = queue:drop(DcQ),
    RecQNew = set(Dc, DcQNew, RecQ),
    LastCommNew = set(Dc, Cts, LastCTS),
    {ok, DcQNew, State#recvr_state{lastCommitted = LastCommNew,
			   recQ = RecQNew,
			   partition_vclock = NewLC}}.

%% Checks depV against the committed timestamps
check_dep(DepV, Localclock) ->
    vectorclock:ge(Localclock, DepV).

%%Set a new value to the key.
set(Key, Value, Orddict) ->
    orddict:update(Key, fun(_Old) -> Value end, Value, Orddict).

%%Put a value to the Queue corresponding to Dc in RecQ orddict
enqueue(Dc, Data, RecQ) ->
    case orddict:find(Dc, RecQ) of
        {ok, Q} ->
            Q2 = queue:in(Data, Q),
            set(Dc, Q2, RecQ);
        error -> %key does not exist
            Q = queue:new(),
            Q2 = queue:in(Data,Q),
            set(Dc, Q2, RecQ)
    end.

now_millisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.
