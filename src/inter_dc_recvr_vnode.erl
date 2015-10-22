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

%% @doc This vnode is responsible for receiving updates from remote DCs and 
%% applying to local partition in causal order

-module(inter_dc_recvr_vnode).
-behaviour(riak_core_vnode).
-include("inter_dc_repl.hrl").
-include("antidote.hrl").

-export([start_vnode/1,
         %%API begin
         store_updates/1,
         %%API end
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% public API

%% @doc store_updates: sends the updates from remote DC to corresponding
%%  partition's vnode. Input is a list of transactions from remote DC.
-spec store_updates(Transactions::[clocksi_transaction_reader:transaction()])
                   -> ok.
store_updates(Transactions) ->
    lists:foreach(fun start_store_update/1,Transactions).


start_store_update(Transaction) ->
    {Txid,Committime,ST,Ops} = Transaction,
    Operation = hd(Ops),
    Logrecord = Operation#operation.payload,
    %%Payload = Logrecord#log_record.op_payload,
    Op_type = Logrecord#log_record.op_type,
    case Op_type of
        noop ->
	    lager:error("Recieved a noop message: ~p", [Transaction]);
	safe_update ->
	    %% TODO: Before calling update_safe_clock,
	    %% should wait until all updates up to this time have been processed locally
	    %% (they all have been recieved, but not yet processed yet) otherwise some new
	    %% transactions might be blocked temporarily
	    {Dc, Ts} = Committime,
	    %% {ok, _} = vectorclock:update_safe_clock_local(Dc, Ts - 1);
	    riak_core_vnode_master:command(get_random_node(),{process_safe,Dc,Ts},inter_dc_recvr_vnode_master);
	    %% FIX! This should send to all and calc min
	    %% riak_core_vnode_master:command(get_random_node(),{process_queue,Transaction,self()},inter_dc_recvr_vnode_master);
        _ ->
	    {SeparatedTransactions, FinalOps} =
		lists:foldl(fun(Op1,{DictNodeKey,ListXtraOps}) ->
				    case Op1#operation.payload#log_record.op_payload of
					{K1,_,_} ->
					    case replication_check:is_replicated_here(K1) of
						true ->
						    NewDictNodeKey = dict:append(hd(log_utilities:get_preflist_from_key(K1)),
										 Op1,DictNodeKey);
						_ ->
						    NewDictNodeKey = DictNodeKey end,
					    NewListXtraOps = ListXtraOps;
					_ ->
					    NewDictNodeKey = DictNodeKey,
					    NewListXtraOps = lists:append(ListXtraOps, [Op1])
				    end,
				    {NewDictNodeKey,NewListXtraOps} end,
			    {dict:new(),[]}, Ops),
	    %% Fix this: because sends a store_update per op, should instead send a message per partition
	    _WaitCount = dict:fold(fun(Node,Op2,Count) ->
					  %% Maybe should only run this once???
					  %% store_update(Node,{Txid,Committime,ST,lists:append(Op2,FinalOps)}),
					  riak_core_vnode_master:command(Node,{process_queue,
									       {Txid,Committime,ST,lists:append(Op2,FinalOps)}, self()},
									 inter_dc_recvr_vnode_master),
					  Count + 1
				  end, 0, SeparatedTransactions)
	    %% receive_loop(WaitCount,self())
    end,
    ok.

%% Helper function
%% receive_loop(0,_MyPid) ->
%%     ok;
%% receive_loop(Count,MyPid) ->
%%     receive
%% 	{MyPid, done_process} ->
%% 	    ok
%%     end,
%%     receive_loop(Count - 1,MyPid).
    

%% @doc Returns a random node
get_random_node() ->
    % Send the update to a random node
    Node = node(),
    Preflist = riak_core_apl:active_owners(inter_dc_recvr),
    Prefnode = [{Partition, Node1} ||
                   {{Partition, Node1},_Type} <- Preflist, Node1 =:= Node],
    %% Take a random vnode
    {A1,A2,A3} = os:timestamp(),
    random:seed(A1, A2, A3),
    Index = random:uniform(length(Prefnode)),
    lists:nth(Index, Prefnode).


%% store_update(Node, Transaction) ->
%%     riak_core_vnode_master:sync_command(Node,
%%                                         {store_update, Transaction},
%%                                         inter_dc_recvr_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    StateFile = string:concat(integer_to_list(Partition), "replstate"),
    Path = filename:join(
             app_helper:get_env(riak_core, platform_data_dir), StateFile),
    case dets:open_file(StateFile, [{file, Path}, {type, set}]) of
        {ok, StateStore} ->
            case dets:lookup(StateStore, recvr_state) of
                %%If file already exists read previous state from it.
                [{recvr_state, State}] ->
                    {ok, State};
                [] ->
                    {ok, State } = inter_dc_repl_update:init_state(Partition),
                    {ok, State#recvr_state{statestore = StateStore}};
                Error -> Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

handle_command({process_queue, From}, _Sender, State) ->
    %%ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {Result, NewState2} = inter_dc_repl_update:process_queue(State),
    case Result of
	ok ->
	    %%ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState2}),
	    %% should probably remove this, and do groups of transactions instead of one at a time
	    %% The reply to let the proccesing for this transaction has finished is in the process_queue
	    ok;
	dep_not_sat ->
	    riak_core_vnode:send_command_after(?META_DATA_SLEEP,{process_queue,From})
    end,
    {noreply, NewState2};

%% process one replication request from other Dc. Update is put in a queue for each DC.
%% Updates are expected to recieve in causal order.
handle_command({process_queue, Transaction, From}, _Sender, State) ->
    {ok, NewState} = inter_dc_repl_update:enqueue_update(
                       {Transaction, From}, State),
    %%ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {Result, NewState2} = inter_dc_repl_update:process_queue(NewState),
    case Result of
	ok ->
	    %%ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState2}),
	    %% should probably remove this, and do groups of transactions instead of one at a time
	    %% The reply to let the proccesing for this transaction has finished is in the process_queue
	    ok;
	dep_not_sat ->
	    riak_core_vnode:send_command_after(?META_DATA_SLEEP,{process_queue,From})
    end,
    {noreply, NewState2};

handle_command({process_safe, Dc, Ts}, _Sender, State=#recvr_state{partition=Partition}) ->
    {ok, _} = vectorclock:update_safe_clock_local(Partition, Dc, Ts - 1),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State = #recvr_state{partition=Partition}) ->
    meta_data_sender:remove_partition(stable,Partition),
    ok.
