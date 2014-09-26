-module(inter_dc_communication_sender).
-behaviour(gen_fsm).

-include("floppy.hrl").

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
         stop/2
]).

-record(state, {port, host, socket,message, caller}). % the current socket

-define(TIMEOUT,5000).

%% Send a message to all DCs over a tcp connection
propagate_sync(Message, DCs) ->
    lists:foreach( fun({DcAddress, Port}) ->
                           inter_dc_communication_sender:start_link(
                             Port, DcAddress, Message, self()),
                           receive
                               {done, normal} ->
                                   ok;
                               _ ->
                                   {error}
                                   %%TODO: Retry if needed
                           after ?TIMEOUT ->
                                   {error, timeout}
                                   %%TODO: Retry if needed
                           end
                   end,
                   DCs),
    ok.

start_link(Port, Host, Message, ReplyTo) ->
    gen_fsm:start_link(?MODULE, [Port, Host, Message, ReplyTo], []).

init([Port,Host,Message,ReplyTo]) ->
    {ok, connect, #state{port=Port,
                         host=Host,
                         message=Message,
                         caller=ReplyTo}, 0}.

connect(timeout, State=#state{port=Port,host=Host,message=Message}) ->
    lager:info("Connecting to ~p : ~p",[Host, Port]),
    case  gen_tcp:connect(Host, Port,
                                 [{active,true},binary, {packet,2}], ?TIMEOUT) of
        { ok, Socket} ->
            lager:info("Connected"),
            inet:setopts(Socket, [{active, once}]),
            ok = gen_tcp:send(Socket, term_to_binary(Message)),
            inet:setopts(Socket, [{active, once}]),
            {next_state, wait_for_ack, State#state{socket=Socket},?TIMEOUT};
        {error, _Reason} ->
            lager:info("Couldnot connect to remote DC"),
            {stop, normal, State}
    end.

wait_for_ack({acknowledge, _DC}, State=#state{socket=_Socket, message=_Message} )->
    {next_state, stop , State,0};

wait_for_ack(timeout, State) ->
    %%TODO: Retry if needed
    {next_state,stop,State,0}.

stop(timeout, State=#state{socket=Socket}) ->
    gen_tcp:close(Socket),
    {stop, normal, State}.

handle_info({tcp, Socket, Bin}, StateName, #state{socket=Socket} = StateData) ->
    inet:setopts(Socket, [{active, once}]),
    gen_fsm:send_event(self(), binary_to_term(Bin)),
    {next_state, StateName, StateData}; 

handle_info({tcp_closed, Socket}, _StateName,
            #state{socket=Socket} = StateData) ->
    %%TODO: Retry if needed
    {stop, normal, StateData};

handle_info(Message, _StateName, StateData) ->
    lager:debug("Unexpected message: ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(Reason, _SN, _State = #state{caller = Caller}) ->
    Caller ! {done, Reason},
    ok.
