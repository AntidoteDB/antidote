-module(cross_dc_read_communication_fsm).
-behaviour(gen_fsm).

-record(state, {socket}). % the current socket

-export([start_link/1]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([receive_message/2,
         close_socket/2
        ]).

-define(TIMEOUT,10000).


%% This fsm is receives connections from tx coordinators located
%% at external DCs that don't replicate the key  they are looking
%% for reads

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, [Socket], []).

init([Socket]) ->
    {ok, receive_message, #state{socket=Socket},0}.


receive_message(timeout, State=#state{socket=Socket}) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Message} ->
            ReplyValue = case binary_to_term(Message) of
			     {read_external, {Key,Type,Transaction}} ->
				 Preflist = log_utilities:get_preflist_from_key(Key),
				 IndexNode = hd(Preflist),
				 %% Is it safe to do a read like this from an external transaction?
				 %% Might cause blocking because external DC might be ahead in time
				 case clocksi_vnode:read_data_item_external(IndexNode, Transaction,
								   Key, Type) of
				     error ->
					 {reply, error, abort};
				     {error, _Reason} ->
					 {reply, error, abort};
				     {ok, Snapshot} ->
					 ReadResult = Type:value(Snapshot),
					 {reply, {ok, ReadResult}}
				 end;
			     Unknown ->
				 lager:error("Weird message received in cross_dc_read_comm ~p end", [Unknown]),
				 Unknown
			 end,
	    ok = gen_tcp:send(Socket,
			      term_to_binary({acknowledge,
					      {ReplyValue, inter_dc_manager:get_my_dc()}}));
	{error, Reason} ->
	    lager:error("Problem with the socket, reason: ~p", [Reason])
    end,
    {next_state, close_socket,State,0}.

close_socket(timeout, State=#state{socket=Socket}) ->
    gen_tcp:close(Socket),
    {stop, normal, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
