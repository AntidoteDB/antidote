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

%% @doc : An fsm to send messages to other DCs over a TCP connection

-module(inter_dc_communication_sender).
-behaviour(gen_fsm).

-include("antidote.hrl").

-export([propagate_sync_safe_time/3,
         propagate_sync/4,
	 perform_external_read/4
        ]).
-export([init/1,
	 start_link/5,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([connect/2,
	 do_send/2,
         wait_for_ack/2,
	 wait_for_ack_no_close/2,
         stop/2,
	 stop_error/2
        ]).

-record(state, {port, host, socket,message, caller, reply, reason, reason_type}). % the current socket

%% ===================================================================
%% Public API
%% ===================================================================

%% Send a message to all DCs over a tcp connection
%% In partial repl alg this takes as input a dictionary with
%% keys as list of DCs for transacitons that are replicated
%% and values as the transactions

%-spec propagate_sync(DictTransactionsDcs :: dict(), StableTime :: pos_integer(),
%		     Partition :: term())
%				  -> ok | error.
propagate_sync(DictTransactionsDcs, StableTime, Partition,CD1) ->
    ListTxnDcs = dict:to_list(DictTransactionsDcs),
    {CD2,FailedDCs} = lists:foldl(
		  fun({DCs, Message}, {CD,Err}) ->
			  lists:foldl(
			    fun({DcAddress, Port}, {ConDict,Acc}) ->
				    Res = case dict:find({DcAddress,Port},ConDict) of
					      {ok,Pid1} ->
						  gen_fsm:send_event(Pid1,{send,{replicate, inter_dc_manager:get_my_dc(), Message}}),
						  {ok,Pid1};
					      error ->
						  inter_dc_communication_sender_fsm_sup:start_fsm(
						    [Port, DcAddress, {replicate, inter_dc_manager:get_my_dc(), Message}, self(), send_propagate])
					  end,
				    case Res of
					{ok, Pid} ->
					    receive
						{done, normal, _Reply, _Soc} ->
						    %% Update the sent clock for partial repl alg
						    %% This DC should have recieved updates up to safe time
						    %% Fix TODO: This should use the unique DcId
						    %% for storing items here
						    %% (i.e. the one that is included in the transactions) instead
						    %% of the address and port, but I don't know how to
						    %% get these ids??
						    %% {ok, _} = vectorclock:update_sent_clock(
						    %%	     Partition, {DcAddress, Port}, StableTime),
						    %% Instead of using riak meta data, just send
						    %% to a single server
						    %%
						    vectorclock:update_sent_clock({DcAddress,Port}, Partition, StableTime),
						    {dict:store({DcAddress,Port},Pid,ConDict), Acc};
						{done, Other, _Reply, _Soc} ->
						    lager:error(
						      "Send failed Reason:~p Message: ~p",
						      [Other, Message]),
						    {dict:erase({DcAddress,Port}, ConDict),[{DcAddress,Port} | Acc]}
						    %%TODO: Retry if needed
					    after
						?SEND_TIMEOUT + ?CONNECT_TIMEOUT ->
						    lager:error("did not get reply ~w", [Res]),
						    {error, dict:erase({DcAddress,Port}, ConDict)}
					    end;
					Error ->
					    lager:error("Error sending to ~w, ~w.  Error: ~w", [Port,DcAddress,Error]),
					    {dict:erase({DcAddress,Port}, ConDict),[{DcAddress,Port} | Acc]}
				    end
			    end, {CD,Err}, DCs)
		  end, {CD1,[]}, ListTxnDcs), 
    case FailedDCs of
        [] ->
            {ok,CD2};
        _ -> 
	    {error,CD2}
    end.


%% Used in partial repl alg
%% Sends a single transaction like a heartbeat, except with
%% the min time of all the partitions of the local DC have sent to
%% the desination DC
-spec propagate_sync_safe_time({DcAddress :: non_neg_integer(), Port :: port()}, Transaction :: tx(), dict())
			      -> ok | error.
propagate_sync_safe_time({DcAddress, Port}, Transaction, ConDict) ->
    Res = case dict:find({DcAddress,Port},ConDict) of
	      {ok,Pid1} ->
		  gen_fsm:send_event(Pid1,{send,{replicate, inter_dc_manager:get_my_dc(), [Transaction]}}),
		  {ok,Pid1};
	      error ->
		  inter_dc_communication_sender_fsm_sup:start_fsm(
		    [Port, DcAddress, {replicate, inter_dc_manager:get_my_dc(), [Transaction]}, self(), send_safe_time])
	  end,
    case Res of
	{ok, Pid} ->
	    receive
		{done, normal, _Reply, _Soc} ->
		    {ok, dict:store({DcAddress,Port},Pid,ConDict)};
		{done, Other, _Reply, _Soc} ->
		    lager:error(
		      "Send failed Reason:~p Message: ~p",
		      [Other, Transaction]),
		    {error,dict:erase({DcAddress,Port}, ConDict)}
		    %%TODO: Retry if needed
	    after
		?SEND_TIMEOUT + ?CONNECT_TIMEOUT ->
		    lager:error("did not get reply ~w", [Res]),
		    {error, dict:erase({DcAddress,Port}, ConDict)}
	    end;
	Res ->
	    lager:error("Could not send ~w", [Res]),
	    {error, dict:erase({DcAddress,Port}, ConDict)}
    end.



-spec perform_external_read({DcAddress :: non_neg_integer(), Port :: port()}, Key :: key(), Type :: crdt(), Transaction :: tx())
			   -> {ok, term()} | error.
perform_external_read({DcAddress, Port}, Key, Type, Transaction) ->
    %% MyPid = self(),
    case ext_read_connection_fsm:perform_read(DcAddress,Port, {Key, Type, Transaction, dc_utilities:get_my_dc_id()}) of
	{acknowledge, Reply} ->
	    {ok, Reply};
	Err ->
	    lager:error("Error getting ext read: ~w", [Err]),
	    {ok, error, timeout}
    end.



%% Starts a process to send a message to a single Destination 
%%  DestPort : TCP port on which destination DCs inter_dc_communication_recvr listens
%%  DestHost : IP address (or hostname) of destination DC
%%  Message : message to be sent
%%  ReplyTo : Process id to which the success or failure message has
%%             to be send (Usually the caller of this function) 
%% -spec start_link([port(), non_neg_integer(), [term()], pid(), atom()])
%% 		-> {ok, pid()} | ignore | {error, term()}.
start_link(DestPort, DestHost, Message, ReplyTo, _MsgType) ->
	    gen_fsm:start_link(?MODULE, [DestPort, DestHost, Message, ReplyTo], []).

%% start_link(Socket, Message, ReplyTo, _MsgType) ->
%%     gen_fsm:start_link(?MODULE, [Socket, Message, ReplyTo], []).


%% ===================================================================
%% gen_fsm callbacks
%% ===================================================================


init([Port,Host,Message,ReplyTo]) ->
    {ok, connect, #state{port=Port,
                         host=Host,
                         message=Message,
                         caller=ReplyTo,
			 reply=empty}, 0}.

%% init([Socket,Message,ReplyTo]) ->
%%     {ok, send, #state{socket=Socket,
%% 		      message=Message,
%% 		      caller=ReplyTo,
%% 		      reason_type=error,
%% 		      reply=empty}, 0}.

do_send(timeout,State) ->
    perform_send(State);
do_send({send,Message},State) ->
    perform_send(State#state{message=Message}).

perform_send(State=#state{socket=Socket,message=Message}) ->
    case gen_tcp:send(Socket,term_to_binary(Message)) of
	ok ->
	    {next_state,wait_for_ack_no_close,State,?SEND_TIMEOUT};
	{error, Res} ->
	    lager:error("Cound not send on tcp: ~w", [Res]),
	    {next_state,stop_error,State,0}
    end.

connect(timeout, State=#state{port=Port,host=Host,message=Message, caller=Caller}) ->
    case  gen_tcp:connect(Host, Port,
                          [{active,true}, binary, {packet,4}], ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, term_to_binary(Message)),
            {next_state, wait_for_ack_no_close, State#state{socket=Socket},?SEND_TIMEOUT};
        {error, Reason} ->
            lager:error("Couldnot connect to remote DC: ~p", [Reason]),
	    Caller ! {done, error, Reason, undefined},
            {stop, normal, State#state{reason=Reason}}
    end.

wait_for_ack_no_close(acknowledge,State = #state{socket=Soc, caller = Caller}) ->
    Caller ! {done, normal, normal, Soc},
    {next_state,do_send,State};
wait_for_ack_no_close(timeout, State) ->
    %%TODO: Retry if needed
    lager:error("timeout in wait for ack"),
    {next_state,stop_error,State#state{reason=timeout},0}.

wait_for_ack(acknowledge, State)->
    {next_state, stop, State#state{reason=normal},0};

wait_for_ack(timeout, State) ->
    %%TODO: Retry if needed
    lager:error("Timeout in wait for ACK",[]),
    %% Retry after timeout is handled in the propagate_sync loop above
    %% So the fsm returns a normal stop so that it isn't restarted by the supervisor
    {next_state,stop_error,State#state{reason=timeout},0}.

stop(timeout, State=#state{socket=Socket, reason=Res, caller=Caller}) ->
    _ = gen_tcp:close(Socket), 
    Caller ! {done, normal, Res, Socket},
    {stop, normal, State#state{reason_type=normal}}.

stop_error(timeout, State=#state{socket=Socket, reason=Res, caller=Caller}) ->
    _ = gen_tcp:close(Socket),
    Caller ! {done, error, Res, Socket},
    {stop, normal, State#state{reason_type=error}}.

%% Converts incoming tcp message to an fsm event to self
handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    gen_fsm:send_event(self(), binary_to_term(Bin)),
    {next_state, StateName, StateData};

handle_info({tcp_closed, Socket}, _StateName,
            #state{socket=Socket} = StateData) ->
    %%TODO: Retry if needed
    {stop, normal, StateData};

handle_info(Message, _StateName, StateData) ->
    lager:error("Unexpected message: ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _State = #state{port=_Port,host=_Host}) ->
    ok.
