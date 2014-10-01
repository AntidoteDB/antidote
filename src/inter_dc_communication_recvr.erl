%%% Handles socket connections, and bridges a remote server
%%% With a progressquest game.
-module(inter_dc_communication_recvr).
-behaviour(gen_fsm).

-record(state, {socket}). % the current socket

-export([start_link/1]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([accept/2,
         wait_for_message/2
         %%stop_server/2
        ]).

-define(TIMEOUT,20000).

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, Socket, []).

init(Socket) ->
    lager:info("started"),
    {ok, accept, #state{socket=Socket},0}.

accept(timeout, State=#state{socket=ListenSocket}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    ok = inet:setopts(AcceptSocket, [{active, once}]),
    {next_state, wait_for_message, State#state{socket=AcceptSocket}, ?TIMEOUT}.

wait_for_message({replicate,Updates}, State=#state{socket=Socket}) ->
    case inter_dc_recvr_vnode:store_updates(Updates) of
        ok ->
            ok = gen_tcp:send(Socket, term_to_binary({acknowledge, inter_dc_manager:get_my_dc()}));
        {error, _Reason} ->
            lager:error(" Did not replicate messages received")
    end,
    stop_server(State),
    {stop,normal,State};

wait_for_message(timeout, State) ->
    stop_server(State),
    {stop, normal, State};

wait_for_message(Message, State) ->
    lager:error("Unexpected Message: ~p", [Message]),
    {next_state, wait_for_message, State, ?TIMEOUT}.

stop_server(_State=#state{socket=Socket}) ->
    %% start a new listener
    {ok, _Pid} = inter_dc_communication_sup:start_socket(),
    gen_tcp:close(Socket).

handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    %% flow control: enable forwarding of next tcp message
    ok = inet:setopts(Socket, [{active, once}]),
    gen_fsm:send_event(self(),binary_to_term(Bin)),
    {next_state, StateName, StateData};

handle_info({tcp_closed, Socket}, _StateName,
            #state{socket=Socket} = StateData) ->
    lager:debug("TCP disconnect."),
    stop_server(StateData),
    {stop, normal, StateData};

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    stop_server(StateData),
    {stop, normal, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
