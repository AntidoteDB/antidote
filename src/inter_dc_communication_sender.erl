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

-export([start_link/4,
         propagate_sync/2
        ]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([connect/2,
         wait_for_ack/2,
         stop/2,
         stop_error/2
        ]).

-record(state, {port, host, socket,message, caller}). % the current socket

-define(TIMEOUT,20000).
-define(CONNECT_TIMEOUT,5000).

%% Send a message to all DCs over a tcp connection
propagate_sync(Message, DCs) ->
    Errors = lists:foldl(
               fun({DcAddress, Port}, Acc) ->
                       case inter_dc_communication_sender:start_link(
                              Port, DcAddress, Message, self()) of
                           {ok, _} ->
                               receive
                                   {done, normal} ->
                                       Acc;
                                   {done, Other} ->
                                       lager:error(
                                         "Send failed Reason:~p Message: ~p",
                                         [Other, Message]),
                                       Acc ++ [error]
                                       %%TODO: Retry if needed
                               after ?TIMEOUT ->
                                       lager:error(
                                         "Send failed timeout Message ~p"
                                         ,[Message]),
                                       Acc ++ [{error, timeout}]
                                       %%TODO: Retry if needed
                               end;
                           _ ->
                               Acc ++ [error]
                       end
               end, [],
               DCs),
    case length(Errors) of
        0 ->
            ok;
        _ -> error
    end.

start_link(Port, Host, Message, ReplyTo) ->
    gen_fsm:start_link(?MODULE, [Port, Host, Message, ReplyTo], []).

init([Port,Host,Message,ReplyTo]) ->
    {ok, connect, #state{port=Port,
                         host=Host,
                         message=Message,
                         caller=ReplyTo}, 0}.

connect(timeout, State=#state{port=Port,host=Host,message=Message}) ->
    case  gen_tcp:connect(Host, Port,
                          [{active,true},binary, {packet,2}], ?CONNECT_TIMEOUT) of
        { ok, Socket} ->
            ok = inet:setopts(Socket, [{active, once}]),
            ok = gen_tcp:send(Socket, term_to_binary(Message)),
            ok = inet:setopts(Socket, [{active, once}]),
            {next_state, wait_for_ack, State#state{socket=Socket},?CONNECT_TIMEOUT};
        {error, _Reason} ->
            lager:error("Couldnot connect to remote DC"),
            {stop, normal, State}
    end.

wait_for_ack(acknowledge, State=#state{socket=_Socket, message=_Message} )->
    {next_state, stop, State,0};

wait_for_ack(timeout, State) ->
    %%TODO: Retry if needed
    {next_state,stop_error,State,0}.

stop(timeout, State=#state{socket=Socket}) ->
    _ = gen_tcp:close(Socket),
    {stop, normal, State}.

stop_error(timeout, State=#state{socket=Socket}) ->
    _ = gen_tcp:close(Socket),
    {stop, error, State}.

handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    _ = inet:setopts(Socket, [{active, once}]),
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

terminate(Reason, _SN, _State = #state{caller = Caller}) ->
    Caller ! {done, Reason},
    ok.
