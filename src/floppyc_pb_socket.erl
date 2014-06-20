-module(floppyc_pb_socket).

-include_lib("riak_pb/include/floppy_pb.hrl").

-behaviour(gen_server).

-define(FIRST_RECONNECT_INTERVAL, 100).

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
          queue :: queue() | undefined,
          auto_reconnect = false :: boolean(), % if true, automatically reconnects to server
                                               % if false, exits on connection failure/request timeout
          connect_timeout=infinity :: timeout(), % timeout of TCP connection
          keepalive = false :: boolean(), % if true, enabled TCP keepalive for the socket
          connects=0 :: non_neg_integer(), % number of successful connects
          reconnect_interval=?FIRST_RECONNECT_INTERVAL :: non_neg_integer(),
          credentials :: undefined | {string(), string()} % username/password
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
         get/1
        ]).

get(Pid) ->
    Req = #fpbincrementreq{key="key", amount=2},
    call_infinity(Pid, {req, Req}).

%% Callback functions

%% @private
init([Address, Port, _Options]) ->
    State = #state{address = Address, port = Port, queue = queue:new()},
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

%% @private
handle_info(_, State) ->
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
    #state{address = Address, port = Port, connects = Connects} = State,
    case gen_tcp:connect(Address, Port,
                         [binary, {active, once}, {packet, 4},
                          {keepalive, State#state.keepalive}],
                         State#state.connect_timeout) of
        {ok, Sock} ->
            State1 = State#state{sock = Sock, connects = Connects+1,
                                 reconnect_interval = ?FIRST_RECONNECT_INTERVAL},
            {ok, State1};
        Error -> Error
    end.

handle_call({req, Msg}, From, State) ->
    {reply, send_request(new_request(Msg, From, infinity), State),State}.

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%% @private
%% Make a new request that can be sent or queued
%% @todo Support Context?? see riak-erlang-client. Also removed timeout (must do this)
new_request(Msg, From, Timeout) ->
    Ref = make_ref(),
    #request{ref = Ref, msg = Msg, from = From, timeout = Timeout, tref = create_req_timer(Timeout, Ref)}.

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
send_request(Request0, State)  -> % when State#state.active =:= undefined 
    {_Request, Pkt} = encode_request_message(Request0),
    case gen_tcp:send(State#state.sock, Pkt) of
        ok ->
            ok;
            %maybe_reply(after_send(Request, State#state{active = Request}));
        {error, Reason} ->
            error_logger:warning_msg("Socket error while sending riakc request: ~p.", [Reason]),
            gen_tcp:close(State#state.sock)
            %maybe_enqueue_and_reconnect(Request, State#state{sock=undefined})
    end.

%% Unencoded Request (the normal PB client path)
encode_request_message(#request{msg=Msg}=Req) ->
    {Req, riak_pb_codec:encode(Msg)}.

