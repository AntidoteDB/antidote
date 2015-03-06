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
%% @doc The coordinator for a given Clock SI static transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted), the fsm
%%      also finishes.

-module(clocksi_static_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/3,
         start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([execute_batch_ops/2]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
          from :: pid(),
          tx_id :: tx_id(),
          tx_coord_pid :: pid(),
          operations :: list(),
          state:: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, ClientClock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ClientClock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).


%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    {ok, _Pid} = case ClientClock of
                     ignore ->
                         clocksi_interactive_tx_coord_sup:start_fsm([self()]);
                     _ ->
                         clocksi_interactive_tx_coord_sup:start_fsm([self(), ClientClock])
                 end,
    receive
        {ok, TxId} ->
            {_, _, TxCoordPid} = TxId,
            {ok, execute_batch_ops, #state{tx_id=TxId, tx_coord_pid = TxCoordPid,
                                           from = From, operations = Operations}, 0}
    after
        10000 ->
            lager:error("Tx was not started!"),
            gen_fsm:reply(From, {error, timeout}),
            {stop, normal, #state{}}
    end.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_batch_ops(timeout, SD=#state{from = From,
                                     tx_id = TxId,
                                     tx_coord_pid = TxCoordPid,
                                     operations = Operations}) ->                     
    
    ExecuteOp = fun (Operation, Acc) ->
    					case Acc of 
						{error, Reason} ->
							{error, Reason};
						{ReadBuffer, ReadPartitions} ->
							lager:info("partitions are: ~p~n buffer is: ~p~n",[ReadPartitions, ReadBuffer]),
							case Operation of
								{update, Key, Type, OpParams} ->
									case gen_fsm:sync_send_event(TxCoordPid, {update, {Key, Type, OpParams}}, infinity) of
									ok ->
										{ReadBuffer, ReadPartitions};
									{error, Reason} ->
										{error, Reason}
									end;
								{read, Key, Type} ->
									Preflist = log_utilities:get_preflist_from_key(Key),
									IndexNode = hd(Preflist),
									case dict:find(IndexNode, ReadBuffer) of
										{ok, Dict0} ->
											Dict1 = dict:store(Key, Type, Dict0),
											ReadBuffer1 = dict:store(IndexNode, Dict1, ReadBuffer),
											{ReadBuffer1, ReadPartitions};
										error ->
											Dict0 = dict:new(),
											Dict1 = dict:store(Key, Type, Dict0),
											ReadPartitions1 = lists:append(ReadPartitions, [IndexNode]),
											ReadBuffer1 = dict:store(IndexNode, Dict1, ReadBuffer),
											{ReadBuffer1, ReadPartitions1}
									end
							end
						end
                end,
    {ReadBuffer, ReadPartitions} = lists:foldl(ExecuteOp, {dict:new(), []}, Operations), 
    case dict:size(ReadBuffer) == 0 of
    false ->			
		case gen_fsm:sync_send_event(TxCoordPid, {batch_read, {ReadBuffer, ReadPartitions}}, infinity) of
			{ok, ReadSet} ->
				case ReadSet of 
				{error, Reason} ->
					From ! {error, Reason},
					{stop, normal, SD};
				Other ->
					lager:info("received reply ~p ~n ", [Other]),
					case gen_fsm:sync_send_event(TxCoordPid, {prepare, empty}, infinity) of
						{ok, TxId} ->
							From ! {ok, {TxId, ReadSet}},
							{stop, normal, SD};
						_ ->
							From ! {error, commit_fail},
							{stop, normal, SD}
					end
				end;
			{error, Reason} ->
				{error, Reason}
			end;
	_ ->
		case gen_fsm:sync_send_event(TxCoordPid, {prepare, empty}, infinity) of
		{ok, TxId} ->
			From ! {ok, {TxId, []}},
			{stop, normal, SD};
		_ ->
			From ! {error, commit_fail},
			{stop, normal, SD}
		end
	end.
			
	


%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
