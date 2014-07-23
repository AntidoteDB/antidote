-module(floppyc_pb_socket).

-include_lib("riak_pb/include/floppy_pb.hrl").

-behaviour(gen_server).

-define(FIRST_RECONNECT_INTERVAL, 100).
-define(TIMEOUT, 1000).

-type address() :: string() | atom() | inet:ip_address(). %% The TCP/IP host name or address of the Riak node
-type portnum() :: non_neg_integer(). %% The TCP port number of the Riak node's Protocol Buffers interface
-type msg_id() :: non_neg_integer().
-type rpb_req() :: {tunneled, msg_id(), binary()} | atom() | tuple().

-record(request, {ref :: reference(), msg :: rpb_req(), from, timeout :: timeout(),
                  tref :: reference() | undefined }).

-record(state, {
          address :: address(),    % address to connect to
          port :: portnum(),       % port to connect to
          sock :: port(),       % gen_tcp socket
          active :: #request{} | undefined,     % active request
          connect_timeout=infinity :: timeout(), % timeout of TCP connection
          keepalive = false :: boolean() % if true, enabled TCP keepalive for the socket
         }).

-export([start_link/2, start_link/3,
         start/2, start/3,
         stop/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         init/1,
         code_change/3,
         terminate/2]).

-export([
         store_crdt/2,
         get_crdt/3
        ]).

%% @private
init([Address, Port, _Options]) ->
    State = #state{address = Address, port = Port, active = undefined},
    case connect(State) of
        {error, Reason} ->
            {stop, {tcp, Reason}};
        Ok -> Ok
    end.

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port with Options.
%%      Client id will be assigned by the server.
%-spec start_link(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Create a process to talk with the riak server on Address:Port.
%%      Client id will be assigned by the server.
-spec start(address(), portnum()) -> {ok, pid()} | {error, term()}.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port with Options.
%-spec start(address(), portnum(), client_options()) -> {ok, pid()} | {error, term()}.
start(Address, Port, Options) when is_list(Options) ->
    gen_server:start(?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
-spec stop(pid()) -> ok.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%% @private
handle_call({req, Msg, Timeout}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout), State)}.

%% @private
%% @todo handle timeout
handle_info({_Proto, Sock, Data}, State=#state{active = (Active = #request{})}) ->
    <<MsgCode:8, MsgData/binary>> = Data,
    Resp = riak_pb_codec:decode(MsgCode, MsgData),
    NewState = case Resp of
                   %Must abstract message handling
                   #fpboperationresp{success = true} ->
                       cancel_req_timer(Active#request.tref),
                       _ = send_caller(ok, Active),
                       State#state{ active = undefined };
                   #fpbgetcounterresp{value = Val} ->
                       cancel_req_timer(Active#request.tref),
                       _ = send_caller({ok,Val}, Active),
                       State#state{ active = undefined };
                   #fpbgetsetresp{value = Val} ->
                       cancel_req_timer(Active#request.tref),
                       _ = send_caller({ok,binary:bin_to_list(Val)}, Active),
                       State#state{ active = undefined };
                   _ ->
                       lager:warning("Unexpected Message ~p",[Resp]),
                       State#state{ active = undefined }
               end,
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};

handle_info({req_timeout, _Ref}, State=#state{active = Active}) ->
    cancel_req_timer(Active#request.tref),
    _ = send_caller({error, timeout}, Active),
    {noreply, State#state{ active = undefined }};

handle_info({_Proto, Sock, _Data}, State) ->
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @private
%% Connect the socket if disconnected
connect(State) when State#state.sock =:= undefined ->
    #state{address = Address, port = Port} = State,
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4},
                          {keepalive, State#state.keepalive}],
                         State#state.connect_timeout) of
        {ok, Sock} ->
            {ok, State#state{sock = Sock}};
        Error -> Error
    end.


%% @private
new_request(Msg, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref, msg = Msg, from = From, timeout = Timeout, 
             tref = create_req_timer(Timeout, Ref)}.

%% @private
%% Create a request timer if desired, otherwise return undefined.
create_req_timer(infinity, _Ref) ->
    undefined;
create_req_timer(undefined, _Ref) ->
    undefined;
create_req_timer(Msecs, Ref) ->
    erlang:send_after(Msecs, self(), {req_timeout, Ref}).

%% Send a request to the server and prepare the state for the response
%% @private
send_request(Request0, State) when State#state.active =:= undefined  -> 
    {Request, Pkt} = encode_request_message(Request0),
    case gen_tcp:send(State#state.sock, Pkt) of
        ok ->
            maybe_reply({noreply,State#state{active = Request}});
        {error, Reason} ->
            lagger:warning("Socket error while sending riakc request: ~p.", [Reason]),
            gen_tcp:close(State#state.sock)
    end.

%% Unencoded Request (the normal PB client path)
encode_request_message(#request{msg=Msg}=Req) ->
    {Req, riak_pb_codec:encode(Msg)}.

maybe_reply({reply, Reply, State}) ->
    Request = State#state.active,
    NewRequest = send_caller(Reply, Request),
    State#state{active = NewRequest};
maybe_reply({noreply, State}) ->
    State.

% Replies the message and clears the requester id
send_caller(Msg, #request{from = From}=Request) when From /= undefined ->
    gen_server:reply(From, Msg),
    Request#request{from = undefined}.

%% @private
%% Cancel a request timer made by create_timer/2
cancel_req_timer(undefined) ->
    ok;
cancel_req_timer(Tref) ->
    _ = erlang:cancel_timer(Tref),
    ok.

%Stores a client-side crdt to the storage by converting the object state to a
%list of oeprations that will be appended to the log.
%% @todo: propagate only one operation with the list of updates to ensure atomicity.
store_crdt(Obj, Pid) ->
    Mod = floppyc_datatype:module_for_term(Obj),
    Ops = Mod:to_ops(Obj),
    case Ops of
        undefined -> ok;
        Ops -> 
            lists:foldl(fun(Op,Success) ->
                                Result = call_infinity(Pid, {req, Op, ?TIMEOUT}),
                                case Result of
                                    ok -> Success;
                                    Other -> Other
                                end
                        end, ok, Ops)
    end.

%Reads an object from the storage and returns a client-side 
%representation of the CRDT.
get_crdt(Key, Type, Pid) ->
    Mod = floppyc_datatype:module_for_type(Type),
    Op = Mod:message_for_get(Key),
    {ok, Value} = call_infinity(Pid, {req, Op, ?TIMEOUT}),
    Mod:new(Key,Value).



