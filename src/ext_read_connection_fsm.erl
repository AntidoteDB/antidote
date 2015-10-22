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

%% @doc : This gen_fsm is similar to inter_dc_repl_vnode, but instead of sending
%% transactions, it send safe_time messages to external DCs when they have recieved
%% all updates up to the given time

-module(ext_read_connection_fsm).
-behaviour(gen_server).
-include("antidote.hrl").



-export([start_link/1]).
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).
-export([%% loop_receive/2,
	 perform_read/3]).

-record(state, {socket
                }).

%% SAFE_SEND_PERIOD: Frequency of checking new transactions and sending to other DC
%%-define(REGISTER, local).
%%-define(REGNAME(DC,PORT), get_atom(DC,PORT)).
%%-define(REGISTER, global).
%%-define(REGNAME(DC,PORT), global:whereis_name(get_atom(DC,PORT))).

-define(REGISTER, global).
-define(REGNAME(MYDC,DC), {global,get_atom(MYDC,DC)}).


perform_read(DcAddress,Port,Message) ->
    %% Pid = ?REGNAME(DcAddress,Port),
    %%Pid ! {read, Message}.
    gen_server:call(?REGNAME(inter_dc_manager:get_my_dc(),{DcAddress,Port}),
		    {read,Message},?EXT_READ_TIMEOUT).

start_link({DcAddress,Port}) ->
    gen_server:start_link({?REGISTER, get_atom(inter_dc_manager:get_my_dc(),{DcAddress,Port})},
			  ?MODULE, [{DcAddress,Port}], []).

init([{DcAddress,Port}]) ->
    case gen_tcp:connect(DcAddress, Port,
			 [{active,true},binary, {packet,2}], ?CONNECT_TIMEOUT) of
	{ok, Socket} ->
	    lager:info("connected to ext DC for reads ~w ~w", [DcAddress,Port]),
	    {ok, #state{socket=Socket}};
	{error, Reason} ->
	    lager:error("Couldnot connect to remote DC"),
	    {error, Reason}
    end.

handle_call({read, Message},Sender,
	    State=#state{socket=Socket}) ->
    ok = gen_tcp:send(Socket,term_to_binary({read_external, Sender, Message})),
    {noreply,State}.

handle_cast(Msg,State) ->
    lager:info("Weird message received ~w", [Msg]),
    {noreply, State}.

%% loop_receive(timeout, State=#state{socket=Socket
%% 				  }) ->
%%     lager:info("loop rec~n",[]),
%%     receive
%% 	{read,Message} ->
%% 	    lager:info("in read connection sender"),
%% 	    ok = gen_tcp:send(Socket,term_to_binary(Message)),
%% 	    {next_state, loop_receive, State,0};
%% 	{tcp,_Sender,Data} ->
%% 	    received_tcp(Data,State);
%% 	{tcp_closed,_S} ->
%% 	    {stop,badmsg,State};
%% 	OtherMsg ->
%% 	    lager:error("Weird msg recieved in ext read connection2: ~p", [OtherMsg]),
%% 	    {stop,badmsg,State}
%%     end.
    
received_tcp(Data,State) ->
    case binary_to_term(Data) of
	{acknowledge, Pid, Reply} ->
	    _Ignore=gen_server:reply(Pid, {acknowledge, Reply}),
	    %% Pid ! {acknowledge, Pid, Reply},
	    {noreply, State};
	Other ->
	    lager:error("Weird msg recieved in ext read connection1: ~p", [Other]),
	    {stop,badmsg,State}
    end.
    
handle_info(Message, StateData) ->
    case Message of
	{tcp,_Sender,Data} ->
	    received_tcp(Data,StateData);
	_ ->
	    {stop,badmsg,StateData}
    end.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

%% Helper function
%% get_atom(DcAddr, Port) ->
%%     list_to_atom(atom_to_list(?MODULE) ++ atom_to_list(node()) ++
%% 		     atom_to_list(DcAddr) ++ integer_to_list(Port)).

get_atom({MyDcAddr, MyPort}, {DcAddr, Port}) ->
    list_to_atom(atom_to_list(?MODULE) ++ atom_to_list(MyDcAddr) ++
		     integer_to_list(MyPort) ++ atom_to_list(DcAddr) ++ integer_to_list(Port)).

%% my_ip() ->
%%     {ok, List} = inet:getif(),
%%     {Ip, _, _} = hd(List),
%%     inet_parse:ntoa(Ip).
