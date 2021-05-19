%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% This is a process running on each node, that is responsible for receiving
%% queries from other DCs, the types of messages that can be sent are found in
%% include/antidote_message_types.hrl
%% To handle new types, need to update the handle_info method below
%% As well as in the sender of the query at inter_dc_query_dealer

-module(inter_dc_query_router).

-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([
  get_address/0,
  get_address_list/0,
  send_response/2]).

%% Server methods
-export([
  init/1,
  start_link/0,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%% State
-record(state, {socket :: zmq_socket(), next, id}).

%%%% API --------------------------------------------------------------------+

%% Fetch the local address of a log_reader socket.
-spec get_address() -> socket_address().
get_address() ->
  %% first try resolving our hostname according to the node name
  [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
  Ip = case inet:getaddr(Hostname, inet) of
    {ok, HostIp} -> HostIp;
    {error, _} ->
      %% cannot resolve hostname locally, fall back to interface ip
      %% TODO check if we do not return a link-local address
      {ok, List} = inet:getif(),
      {IIp, _, _} = hd(List),
      IIp
  end,
  Port = application:get_env(antidote, logreader_port, ?DEFAULT_LOGREADER_PORT),
  {Ip, Port}.

-spec get_address_list() -> {[partition_id()], [socket_address()]}.
get_address_list() ->
    PartitionList = dc_utilities:get_my_partitions(),
    {ok, List} = inet:getif(),
    List1 = [Ip1 || {Ip1, _, _} <- List],
    %% get host name from node name
    [_, Hostname] = string:tokens(atom_to_list(erlang:node()), "@"),
    IpList = case inet:getaddr(Hostname, inet) of
      {ok, HostIp} -> [HostIp|List1];
      {error, _} -> List1
    end,
    Port = application:get_env(antidote, logreader_port, ?DEFAULT_LOGREADER_PORT),
    AddressList = [{Ip1, Port} || Ip1 <- IpList, Ip1 /= {127, 0, 0, 1}],
    {PartitionList, AddressList}.

-spec send_response(binary(), inter_dc_query_state()) -> ok.
send_response(BinaryResponse, QueryState = #inter_dc_query_state{local_pid=Sender}) ->
    ok = gen_server:cast(Sender, {send_response, BinaryResponse, QueryState}).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Ip = get_router_bind_ip(),
    {_, Port} = get_address(),

    Socket = zmq_utils:create_bind_socket(xrep, true, Port),
    ?LOG_NOTICE("Log reader router started on port ~p binding on IP ~s", [Port, Ip]),
    {ok, #state{socket = Socket, next = getid}}.

%% Handle the remote request
%% ZMQ requests come in 3 parts
%% 1st the Id of the sender, 2nd an empty binary, 3rd the binary msg
handle_info({zmq, _Socket, Id, [rcvmore]}, State=#state{next=getid}) ->
    {noreply, State#state{next = blankmsg, id=Id}};
handle_info({zmq, _Socket, <<>>, [rcvmore]}, State=#state{next=blankmsg}) ->
    {noreply, State#state{next=getmsg}};
handle_info({zmq, Socket, BinaryMsg, _Flags}, State=#state{id=Id, next=getmsg}) ->
    %% Decode the message
    {ReqId, RestMsg} = inter_dc_utils:check_version_and_req_id(BinaryMsg),
    %% Create a response
    QueryState =
        fun(RequestType) ->
            #inter_dc_query_state{
                request_type = RequestType,
                zmq_id = Id,
                request_id_num_binary = ReqId,
                local_pid = self()}
        end,
    case RestMsg of
        <<?LOG_READ_MSG, QueryBinary/binary>> ->
            ok = inter_dc_query_response:get_entries(QueryBinary, QueryState(?LOG_READ_MSG));
        <<?CHECK_UP_MSG>> ->
            ok = finish_send_response(<<?OK_MSG>>, Id, ReqId, Socket);
        <<?BCOUNTER_REQUEST, RequestBinary/binary>> ->
            ok = inter_dc_query_response:request_permissions(RequestBinary, QueryState(?BCOUNTER_REQUEST));
        %% TODO: Handle other types of requests
        _ ->
            ErrorBinary = term_to_binary(bad_request),
            ok = finish_send_response(<<?ERROR_MSG, ErrorBinary/binary>>, Id, ReqId, Socket)
    end,
    {noreply, State#state{next=getid}};
handle_info(Info, State) ->
    ?LOG_INFO("got weird info ~p", [Info]),
    {noreply, State}.

handle_call(_Request, _From, State) -> {noreply, State}.
terminate(_Reason, State) ->
    ?LOG_INFO("Query router terminating"),
    inter_dc_utils:close_socket(State#state.socket).

handle_cast({send_response, BinaryResponse,
         #inter_dc_query_state{request_type = ReqType, zmq_id = Id,
                   request_id_num_binary = ReqId}}, State=#state{socket = Socket}) ->
    finish_send_response(<<ReqType, BinaryResponse/binary>>, Id, ReqId, Socket),
    {noreply, State};

handle_cast(_Request, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finish_send_response(<<_:8, _:_*8>>, binary(), binary(), zmq_socket()) -> ok.
finish_send_response(BinaryResponse, Id, ReqId, Socket) ->
    %% Must send a response in 3 parts with ZMQ
    %% 1st Id, 2nd empty binary, 3rd the binary message
    VersionBinary = ?MESSAGE_VERSION,
    Msg = <<VersionBinary/binary, ReqId/binary, BinaryResponse/binary>>,
    ok = erlzmq:send(Socket, Id, [sndmore]),
    ok = erlzmq:send(Socket, <<>>, [sndmore]),
    ok = erlzmq:send(Socket, Msg).


-spec get_router_bind_ip() -> string().
get_router_bind_ip() ->
    application:get_env(antidote, router_bind_ip, "0.0.0.0").


%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

simple() ->
    {ok, Req} = inter_dc_query_dealer:start_link(),
    {ok, Router} = inter_dc_query_router:start_link(),

    LogReaders = inter_dc_query_router:get_address_list(),
    DcId = dc_utilities:get_my_dc_id(),

    inter_dc_query_dealer:add_dc(DcId, [LogReaders]),

    BinaryMsg = term_to_binary({request_permissions, {transfer, {"hello", 0, dcid}}, 0, dcid, 0}),
    inter_dc_query_dealer:perform_request(?BCOUNTER_REQUEST, {dcid, 0}, BinaryMsg, fun bcounter_mgr:request_response/1),

    gen_server:stop(Req),
    gen_server:stop(Router),
    ok.

request_log_entries() ->
    {ok, Req} = inter_dc_query_dealer:start_link(),
    {ok, Router} = inter_dc_query_router:start_link(),

    LogReaders = inter_dc_query_router:get_address_list(),
    DcId = dc_utilities:get_my_dc_id(),
    inter_dc_query_dealer:add_dc(DcId, [LogReaders]),
    Self = self(),

    meck:expect(inter_dc_sub_vnode, deliver_log_reader_resp, fun(BinaryRep) ->
        <<Partition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = BinaryRep,
        %% check if everything is delivered properly
        {{_DCID = dcid, Partition = 0}, _Txns = [1, 2, 3, 4]} = binary_to_term(RestBinary),
        Self ! finish
                                                             end),

    %% intercept dispatch of `perform_request` to random gen server and handle call directly
    meck:expect(inter_dc_query_response, get_entries, fun(BinaryQuery, QueryState) ->
        {read_log, 0 = Partition, 1, 4} = binary_to_term(BinaryQuery),
%%        LimitedTo = erlang:min(To, From + ?LOG_REQUEST_MAX_ENTRIES), %% Limit number of returned entries
%%        Entries = inter_dc_query_response:get_entries_internal(Partition, From, LimitedTo),
        %% return list of integers, assume read from log read is correct
        Entries = [1, 2, 3, 4],
        BinaryResp = term_to_binary({{dc_utilities:get_my_dc_id(), Partition}, Entries}),
        BinaryPartition = inter_dc_txn:partition_to_bin(Partition),
        FullResponse = <<BinaryPartition/binary, BinaryResp/binary>>,
        ok = inter_dc_query_router:send_response(FullResponse, QueryState),
        ok
                                                      end),


    %% read log entries 1-4 from partition 0
    BinaryRequest = term_to_binary({read_log, 0, 1, 4}),
    inter_dc_query_dealer:perform_request(2, {DcId, 0}, BinaryRequest, fun inter_dc_sub_vnode:deliver_log_reader_resp/1),

    receive finish -> ok after 100 -> throw(test_timeout) end,

    gen_server:stop(Req),
    gen_server:stop(Router),
    ok.

%%request_log_entries_delay() ->
%%    {ok, Req} = inter_dc_query_dealer:start_link(),
%%    {ok, Router} = inter_dc_query_router:start_link(),
%%
%%    LogReaders = inter_dc_query_router:get_address_list(),
%%    DcId = dc_utilities:get_my_dc_id(),
%%    inter_dc_query_dealer:add_dc(DcId, [LogReaders]),
%%
%%    Self = self(),
%%
%%    meck:expect(inter_dc_sub_vnode, deliver_log_reader_resp, fun(BinaryRep) ->
%%        <<Partition:?PARTITION_BYTE_LENGTH/big-unsigned-integer-unit:8, RestBinary/binary>> = BinaryRep,
%%        %% check if everything is delivered properly
%%        {{_DCID = dcid, _Partition}, _Txns = [1, 2, 3, 4]} = binary_to_term(RestBinary),
%%        Self ! {finish_test, Partition}
%%                                                             end),
%%
%%    %% intercept dispatch of `perform_request` to random gen server and handle call directly
%%    meck:expect(inter_dc_query_response, get_entries, fun(BinaryQuery, QueryState) ->
%%        {read_log, Partition, 1, 4} = binary_to_term(BinaryQuery),
%%        case Partition of
%%            0 ->
%%                %% do nothing, i.e. delay
%%                ok;
%%            _ ->
%%                %% return list of integers, assume read from log read is correct
%%                Entries = [1, 2, 3, 4],
%%                BinaryResp = term_to_binary({{dc_utilities:get_my_dc_id(), Partition}, Entries}),
%%                BinaryPartition = inter_dc_txn:partition_to_bin(Partition),
%%                FullResponse = <<BinaryPartition/binary, BinaryResp/binary>>,
%%                ok = inter_dc_query_router:send_response(FullResponse, QueryState)
%%        end
%%                                                      end),
%%
%%
%%    %% read log entries 1-4 from partition 0, delay
%%    BinaryRequest = term_to_binary({read_log, 0, 1, 4}),
%%    inter_dc_query_dealer:perform_request(2, {DcId, 0}, BinaryRequest, fun inter_dc_sub_vnode:deliver_log_reader_resp/1),
%%
%%    %% TODO this blocks with erlzmq
%%    %% do second one, should not block
%%    BinaryRequest2 = term_to_binary({read_log, 1, 1, 4}),
%%    inter_dc_query_dealer:perform_request(2, {DcId, 1}, BinaryRequest2, fun inter_dc_sub_vnode:deliver_log_reader_resp/1),
%%
%%
%%    receive {finish_test, Partition} -> Partition = 1 after 100 -> throw(test_timeout) end,
%%
%%    gen_server:stop(Req),
%%    gen_server:stop(Router),
%%    ok.

test_init() ->
    logger:add_handler_filter(default, ?MODULE, {fun(_, _) -> stop end, nostate}),

    zmq_context:start_link(),

    application:ensure_started(erlzmq),
    application:set_env(antidote, logreader_port, 14444),
    {ok, 14444} = application:get_env(antidote, logreader_port),



    meck:new(dc_utilities),
    meck:new(inter_dc_query_response),
    meck:expect(dc_utilities, get_my_partitions, fun() -> [0, 1] end),
    meck:expect(dc_utilities, get_my_dc_id, fun() -> dcid end),
    meck:expect(inter_dc_query_response, request_permissions, fun(A, B) ->
        %% send directly
        inter_dc_query_router:send_response(A, B)
                                                              end),
    ok.

test_cleanup(_) ->
    application:stop(erlzmq),
    meck:unload(dc_utilities),
    logger:remove_handler_filter(default, ?MODULE).

meck_test_() -> {
    setup,
    fun test_init/0,
    fun test_cleanup/1,
    [
        fun simple/0,
        fun request_log_entries/0
%%        fun request_log_entries_delay/0
    ]}.

-endif.
