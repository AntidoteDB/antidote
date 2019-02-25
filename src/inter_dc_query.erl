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

%% This is a process running on each node, that is responsible for sending
%% queries to other DCs, the types of messages that can be sent are found in
%% include/antidote_message_types.hrl
%% To perform a request, call the "perform_request" function below
%% Then need to update the code of the recipiant of the query at inter_dc_query_receive_socket

%% The unanswered_query caching is there only for the purpose of disconnecting DCs.
%% The reliability-related features like resending the query are handled by ZeroMQ.


-module(inter_dc_query).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  perform_request/4,
  add_dc/2,
  del_dc/1]).

%% Server methods
-export([
  start_link/0,
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%% State
-record(state, {
  sockets :: dict:dict(dcid(), erlzmq:erlzmq_socket()), % DCID -> socket
  req_id :: non_neg_integer(),
  unanswered_queries :: ets:tid() % PDCID -> query
}).

%%%% API --------------------------------------------------------------------+

%% Send any request to another DC partition
%%     RequestType must be an value defined in antidote_message_types.hrl
%%     Func is a function that will be called when the reply is received
%%          It should take two arguments the first is the binary response,
%%          the second is a #request_cache_entry{} record
%%          Note that the function should not perform anywork, instead just send
%%%         the work to another thread, otherwise it will block other messages
-spec perform_request(inter_dc_message_type(), pdcid(), binary(), fun((binary(), #request_cache_entry{})->ok))
             -> ok | unknown_dc.
perform_request(RequestType, PDCID, BinaryRequest, Func) ->
    gen_server:call(?MODULE, {any_request, RequestType, PDCID, BinaryRequest, Func}).

%% Adds the address of the remote DC to the list of available sockets.
-spec add_dc(dcid(), [socket_address()]) -> ok.
add_dc(DCID, LogReaders) -> gen_server:call(?MODULE, {add_dc, DCID, LogReaders}, ?COMM_TIMEOUT).

%% Disconnects from the DC.
-spec del_dc(dcid()) -> ok.
del_dc(DCID) -> gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> {ok, #state{sockets = dict:new(), req_id = 1, unanswered_queries = ets:new(queries, [set])}}.

%% Handle the instruction to add a new DC.
handle_call({add_dc, DCID, LogReaders}, _From, OldState) ->
    %% Create a socket and store it
    %% The DC will contain a list of ip/ports each with a list of partition ids loacated at each node
    %% This will connect to each node and store in the cache the list of partitions located at each node
    %% so that a request goes directly to the node where the needed partition is located
    {_, State} = del_dc(DCID, OldState),
    {Result, NewState} =
    lists:foldl(fun({PartitionList, AddressList}, {ResultAcc, AccState}) ->
                    case connect_to_node(AddressList) of
                        {ok, Socket} ->
                            DCPartitionDict =
                            case dict:find(DCID, AccState#state.sockets) of
                                {ok, Val} ->
                                    Val;
                                error ->
                                    dict:new()
                            end,
                            NewDCPartitionDict =
                            lists:foldl(fun(Partition, Acc) ->
                                            dict:store(Partition, Socket, Acc)
                                        end, DCPartitionDict, PartitionList),
                            NewAccState = AccState#state{sockets = dict:store(DCID, NewDCPartitionDict, AccState#state.sockets)},
                            F = fun({_ReqId, #request_cache_entry{binary_req=Request, pdcid={QDCID, Partition}}}, _Acc) ->
                                    %% if there are unanswered queries that were sent to the DC we just connected with, resend them
                                    case ((QDCID == DCID) and lists:member(Partition, PartitionList)) of
                                        true -> erlzmq:send(Socket, Request);
                                        false -> ok
                                    end
                                end,
                            ets:foldl(F, undefined, NewAccState#state.unanswered_queries),
                            {ResultAcc, NewAccState};
                        connection_error ->
                            {error, AccState}
                    end
                end, {ok, State}, LogReaders),
    {reply, Result, NewState};

%% Remove a DC. Unanswered queries are left untouched.
handle_call({del_dc, DCID}, _From, State) ->
    {_, NewState} = del_dc(DCID, State),
    {reply, ok, NewState};

%% Handle an instruction to ask a remote DC.
handle_call({any_request, RequestType, PDCID, BinaryRequest, Func}, _From, State=#state{req_id=ReqId}) ->
    {DCID, Partition} = PDCID,
    case dict:find(DCID, State#state.sockets) of
    %% If socket found
    %% Find the socket that is responsible for this partition
    {ok, DCPartitionDict} ->
        {SendPartition, Socket} = case dict:find(Partition, DCPartitionDict) of
                                      {ok, Soc} ->
                                          {Partition, Soc};
                                      error ->
                                          %% If you don't see this parition at the DC, just take the first
                                          %% socket from this DC
                                          %% Maybe should use random??
                                          hd(dict:to_list(DCPartitionDict))
                                  end,
        %% Build the binary request
        VersionBinary = ?MESSAGE_VERSION,
        ReqIdBinary = inter_dc_txn:req_id_to_bin(ReqId),
        FullRequest = <<VersionBinary/binary, ReqIdBinary/binary, RequestType, BinaryRequest/binary>>,
        ok = erlzmq:send(Socket, FullRequest),
        RequestEntry = #request_cache_entry{request_type=RequestType, req_id_binary=ReqIdBinary,
                                            func=Func, pdcid={DCID, SendPartition}, binary_req=FullRequest},
        {reply, ok, req_sent(ReqIdBinary, RequestEntry, State)};
    %% If socket not found
    _ -> {reply, unknown_dc, State}
    end.

close_dc_sockets(DCPartitionDict) ->
    F = fun({_, Socket}) ->
            (catch zmq_utils:close_socket(Socket)) end,
    lists:foreach(F, dict:to_list(DCPartitionDict)),
    ok.

%% Handle a response from any of the connected sockets
%% Possible improvement - disconnect sockets unused for a defined period of time.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, State=#state{unanswered_queries=Table}) ->
    <<ReqIdBinary:?REQUEST_ID_BYTE_LENGTH/binary, RestMsg/binary>>
    = binary_utilities:check_message_version(BinaryMsg),
    %% Be sure this is a request from this socket
    case ets:lookup(Table, ReqIdBinary) of
        [{ReqIdBinary, CacheEntry=#request_cache_entry{request_type=RequestType, func=Func}}] ->
            case RestMsg of
                <<RequestType, RestBinary/binary>> ->
                    Func(RestBinary, CacheEntry);
                Other ->
                    logger:error("Received unknown reply: ~p", [Other])
            end,
            %% Remove the request from the list of unanswered queries.
            true = ets:delete(Table, ReqIdBinary);
        [] ->
            logger:error("Got a bad (or repeated) request id: ~p", [ReqIdBinary])
    end,
    {noreply, State}.

terminate(_Reason, State) ->
    F = fun({_, DCPartitionDict}) ->
            close_dc_sockets(DCPartitionDict) end,
    lists:foreach(F, dict:to_list(State#state.sockets)),
    ok.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

del_dc(DCID, State) ->
    case dict:find(DCID, State#state.sockets) of
        {ok, DCPartitionDict} ->
            ok = close_dc_sockets(DCPartitionDict),
            {ok, State#state{sockets = dict:erase(DCID, State#state.sockets)}};
        error ->
            {ok, State}
    end.

%% Saves the request in the state, so it can be resent if the DC was disconnected.
req_sent(ReqIdBinary, RequestEntry, State=#state{unanswered_queries=Table, req_id=OldReq}) ->
    true = ets:insert(Table, {ReqIdBinary, RequestEntry}),
    State#state{req_id=(OldReq+1)}.

%% A node is a list of addresses because it can have multiple interfaces
%% this just goes through the list and connects to the first interface that works
connect_to_node([]) ->
    logger:error("Unable to subscribe to DC log reader"),
    connection_error;
connect_to_node([Address| Rest]) ->
    %% Test the connection
    Socket1 = zmq_utils:create_connect_socket(req, false, Address),
    ok = erlzmq:setsockopt(Socket1, rcvtimeo, ?ZMQ_TIMEOUT),
    BinaryVersion = ?MESSAGE_VERSION,
    %% Always use 0 as the id of the check up message
    ReqIdBinary = inter_dc_txn:req_id_to_bin(0),
    ok = erlzmq:send(Socket1, <<BinaryVersion/binary, ReqIdBinary/binary, ?CHECK_UP_MSG>>),
    Res = erlzmq:recv(Socket1),
    ok = zmq_utils:close_socket(Socket1),
    case Res of
        {ok, Binary} ->
            %% erlzmq:recv returns binary, its spec says iolist, but dialyzer compains that it is not a binary
            %% so I added this conversion, even though the result of recv is a binary anyway...
            ResBinary = iolist_to_binary(Binary),
            %% check that an ok msg was received
            {_, <<?OK_MSG>>} = binary_utilities:check_version_and_req_id(ResBinary),
            %% Create a subscriber socket for the specified DC
            Socket = zmq_utils:create_connect_socket(req, true, Address),
            %% For each partition in the current node:
            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.
