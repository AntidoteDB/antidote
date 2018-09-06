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
-module(antidotec_pb_socket).

-include_lib("riak_pb/include/antidote_pb.hrl").

-behaviour(gen_server).

-define(FIRST_RECONNECT_INTERVAL, 100).
-define(TIMEOUT, 5000).

%% The TCP/IP host name or address of the Riak node
-type address() :: string() | atom() | inet:ip_address().
%% The TCP port number of the Riak node's Protocol Buffers interface
-type portnum() :: non_neg_integer().
-type msg_id() :: non_neg_integer().
-type rpb_req() :: {tunneled, msg_id(), binary()} | atom() | tuple().

-record(request, {ref :: reference(), msg :: rpb_req(), from, timeout :: timeout(),
                  tref :: reference() | undefined }).

-record(state, {
          address :: address(),    % address to connect to
          port :: portnum(),       % port to connect to
          sock :: port() | undefined,           % gen_tcp socket
          active :: #request{} | undefined,     % active request
          connect_timeout = infinity :: timeout(), % timeout of TCP connection
          keepalive = false :: boolean(), % if true, enabled TCP keepalive for the socket
          last_commit_time :: term() % temporarily store for static transactions
         }).


-export([start_link/2, 
         start_link/3,
         start/2,
         start/3,
         stop/1,
         handle_call/3,
         handle_info/2,
         handle_cast/2,
         init/1,
         code_change/3,
         terminate/2]).

-export([call_infinity/2,
         store_commit_time/2,
         get_last_commit_time/1
        ]).

%% @private
init([Address, Port, _Options]) ->
    State = #state{address = Address, port = Port, active = undefined, sock = undefined},
    case connect(State) of
        {error, Reason} ->
            {stop, {tcp, Reason}};
        {ok, State2} -> 
            {ok, State2}
    end.

%% @doc Create a linked process to talk with the riak server on Address:Port
%%      Client id will be assigned by the server.
-spec start_link(address(), portnum()) -> {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

%% @doc Create a linked process to talk with the riak server on Address:Port with Options.
%%      Client id will be assigned by the server.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

%% @doc Create a process to talk with the riak server on Address:Port.
%%      Client id will be assigned by the server.
start(Address, Port) ->
    start(Address, Port, []).

%% @doc Create a process to talk with the riak server on Address:Port with Options.
start(Address, Port, Options) when is_list(Options) ->
    gen_server:start(?MODULE, [Address, Port, Options], []).

%% @doc Disconnect the socket and stop the process.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @private Like `gen_server:call/3', but with the timeout hardcoded
%% to `infinity'.
call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

store_commit_time(Pid, TimeStamp) ->
    gen_server:call(Pid, {store_commit_time, TimeStamp}, infinity).

get_last_commit_time(Pid) ->
    gen_server:call(Pid, get_commit_time, infinity). 

%% @private
handle_call({req, Msg, Timeout}, From, State) ->
    {noreply, send_request(new_request(Msg, From, Timeout), State)};

handle_call({store_commit_time, TimeStamp}, _From, State) ->
    {reply, ok, State#state{last_commit_time = TimeStamp}};

handle_call(get_commit_time, _From, State=#state{last_commit_time = TimeStamp}) ->
    {reply, {ok, TimeStamp}, State#state{last_commit_time = ignore}};

handle_call(stop, _From, State) ->
    _ = disconnect(State),
    {stop, normal, ok, State}.

%% @private
%% @todo handle timeout
handle_info({_Proto, Sock, Data}, State=#state{active = (Active = #request{})}) ->
    <<MsgCode:8, MsgData/binary>> = Data,
    Response = riak_pb_codec:decode(MsgCode, MsgData),
    cancel_req_timer(Active#request.tref),
    _ = send_caller(Response, Active),
    NewState = State#state{active = undefined},
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, NewState};

handle_info({req_timeout, _Ref}, State=#state{active = Active}) ->
    cancel_req_timer(Active#request.tref),
    _ = send_caller({error, timeout}, Active),
    {noreply, State#state{ active = undefined }};

handle_info({tcp_closed, _Socket}, State) ->
    disconnect(State);

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
        {error, Reason} -> 
            {error, Reason}
    end.


disconnect(State) ->
    %% Tell any pending requests we've disconnected
    _ = case State#state.active of
            undefined ->
                ok;
            Request ->
                send_caller({error, disconnected}, Request)
        end,

    %% Make sure the connection is really closed
    case State#state.sock of
        undefined ->
            ok;
        Sock ->
            gen_tcp:close(Sock)
    end,
    NewState = State#state{sock = undefined, active = undefined},
    {stop, disconnected, NewState}.


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
            lager:warning("Socket error while sending riakc request: ~p.", [Reason]),
            gen_tcp:close(State#state.sock)
    end.

%% Unencoded Request (the normal PB client path)
encode_request_message(#request{msg=Msg}=Req) ->
    EncMsg = riak_pb_codec:encode(Msg),
    {Req, EncMsg}.
    %{Req, riak_pb_codec:encode(Msg)}.

%% maybe_reply({reply, Reply, State = #state{active = Request}}) ->
%%   NewRequest = send_caller(Reply, Request),
%%   State#state{active = NewRequest};

maybe_reply({noreply, State = #state{}}) ->
    State.

%% Replies the message and clears the requester id
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
