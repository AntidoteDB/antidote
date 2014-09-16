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
         wait_for_message/2,
         stop_server/2
        ]).

-define(TIMEOUT,10000).

start_link(Socket) ->
    gen_fsm:start_link(?MODULE, Socket, []).

init(Socket) ->
    lager:info("started"),
    {ok, accept, #state{socket=Socket},0}.

accept(timeout, State=#state{socket=ListenSocket}) ->
    lager:info("Waiting for connection"),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    lager:info("connection accepted"),
    inet:setopts(AcceptSocket, [{active, once}]),
    {next_state, wait_for_message, State#state{socket=AcceptSocket}, ?TIMEOUT}.

wait_for_message({replicate,Updates}, State=#state{socket=Socket}) ->
    inter_dc_recvr_vnode:store_updates(Updates),
    lager:debug("Replication request received: ~p",[Updates]),
    gen_tcp:send(Socket, term_to_binary(acknowledge)),
    {next_state,stop_server,State,0};
wait_for_message(timeout, State) ->
    {next_state, stop_server, State, 0};
wait_for_message(Message, State) ->
    lager:debug("Unexpected Message: ~p", [Message]),
    {next_state, stop_server, State,0}.

stop_server(timeout, State=#state{socket=Socket}) ->
    %% start a new listener
    inter_dc_communication_sup:start_socket(),
    gen_tcp:close(Socket),
    {stop, normal, State}.

handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    % flow control: enable forwarding of next tcp message
    inet:setopts(Socket, [{active, once}]),
    gen_fsm:send_event(self(),binary_to_term(Bin)),
    {next_state, StateName, StateData};

handle_info({tcp_closed, Socket}, _StateName,
            #state{socket=Socket} = StateData) ->
    lager:debug("TCP disconnect."),
    {next_state, stop_server,StateData,0};

handle_info(Message, _StateName, StateData) ->
    lager:debug("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
