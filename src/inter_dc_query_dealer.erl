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
%% Then need to update the code of the recipiant of the query at inter_dc_query_router

%% The unanswered_query caching is there only for the purpose of disconnecting DCs.
%% The reliability-related features like resending the query are handled by ZeroMQ.


-module(inter_dc_query_dealer).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

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

-type req_dict() :: dict:dict({dcid(), term()} , zmq_socket()).

%% State
-record(state, {
  sockets :: req_dict(), % {DCID, partition} -> Socket
  req_id :: non_neg_integer(),
    unanswered_queries = create_queries_table()
}).

%%%% API --------------------------------------------------------------------+

%% Send any request to another DC partition
%% RequestType must be an value defined in antidote_message_types.hrl
%% Func is a function that will be called when the reply is received
%% It should take two arguments the first is the binary response,
%% the second is a #request_cache_entry{} record
%% Note that the function should not perform any work, instead just send
%% the work to another process, otherwise it will block other messages
-spec perform_request(inter_dc_message_type(), pdcid(), binary(), fun((binary()) -> ok))
             -> ok | unknown_dc.
perform_request(RequestType, PDCID, BinaryRequest, Func) ->
    gen_server:call(?MODULE, {any_request, RequestType, PDCID, BinaryRequest, Func}).

%% Adds the address of the remote DC to the list of available sockets.
-spec add_dc(dcid(), [socket_address()]) -> ok.
add_dc(DCID, LogReaders) ->
    ok = gen_server:call(?MODULE, {add_dc, DCID, LogReaders}, ?COMM_TIMEOUT).

%% Disconnects from the DC.
-spec del_dc(dcid()) -> ok.
del_dc(DCID) ->
    ok = gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

-spec start_link() -> {ok, pid()}.
start_link() -> {ok, _Pid} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) ->
    {ok, #state{sockets = dict:new(), req_id = 1}}.

%% Handle an instruction to ask a remote DC.
handle_call({any_request, RequestType, {DCID, Partition}, BinaryRequest, Func}, _From, State=#state{req_id=ReqId}) ->
    case dict:find({DCID, Partition}, State#state.sockets) of
        {ok, Socket} ->
            ?LOG_DEBUG("Request ~p to ~p ~p (~p)", [RequestType, DCID, Partition, Socket]),

            %% prepare message
            VersionBinary = ?MESSAGE_VERSION,
            ReqIdBinary = inter_dc_txn:req_id_to_bin(ReqId),
            FullRequest = <<VersionBinary/binary, ReqIdBinary/binary, RequestType, BinaryRequest/binary>>,

            ok = erlzmq:send(Socket, FullRequest),
            RequestEntry = #request_cache_entry{request_type=RequestType, req_id_binary=ReqIdBinary,
                func=Func, pdcid={DCID, Partition}, binary_req=FullRequest},

            {reply, ok, req_sent(ReqIdBinary, RequestEntry, State)};
        _ ->
            ?LOG_ERROR("Could not find ~p:~p in socket dict ~p", [DCID, Partition, State#state.sockets]),
            {reply, unknown_dc, State}
    end;
%% Handle the instruction to add a new DC.
handle_call({add_dc, DCID, LogReaders}, _From, State = #state{ sockets = OldDcPartitionDict }) ->
    %% delete the dc if already added
    InitialDcPartitionDict = del_dc(DCID, OldDcPartitionDict),

    %% add every DC-Partition pair to dict
    AddPartitionsToDict = fun(Partition, {CurrentDict, Socket}) ->
            Key = {DCID, Partition},
            %% assert that DC was really deleted
            error = dict:find(Key, CurrentDict),
            NewDict = dict:store(Key, Socket, CurrentDict),
            {NewDict, Socket}
        end,

    %% for each log reader add every {DCID, Partition} tuple to the dict
    AddLogReaders = fun({Partitions, AddressList}, CurrentDict) ->
            {ok, Socket} = connect_to_node(AddressList),
            {ResultDict, Socket} = lists:foldl(AddPartitionsToDict, {CurrentDict, Socket}, Partitions),
            ResultDict
        end,

    ResultDcPartitionDict = lists:foldl(AddLogReaders, InitialDcPartitionDict, LogReaders),
    {reply, ok, State#state{sockets = ResultDcPartitionDict}};

%% Remove a DC
handle_call({del_dc, DCID}, _From, State = #state{ sockets = Dict}) ->
    NewDict = del_dc(DCID, Dict),
    {reply, ok, State#state{sockets = NewDict}}.


handle_info({zmq, _Socket, BinaryMsg, _Flags}, State = #state{unanswered_queries = Table}) ->
    <<ReqIdBinary:?REQUEST_ID_BYTE_LENGTH/binary, RestMsg/binary>> = inter_dc_utils:check_message_version(BinaryMsg),
    %% Be sure this is a request from this socket
    case get_request(Table, ReqIdBinary) of
        {ok, #request_cache_entry{request_type=RequestType, func=Func}} ->
            case RestMsg of
                <<RequestType, RestBinary/binary>> ->
                    Func(RestBinary);
                Other ->
                    ?LOG_ERROR("Received unknown reply: ~p", [Other])
            end,
            %% Remove the request from the list of unanswered queries.
            true = delete_request(Table, ReqIdBinary);
        not_found ->
            ?LOG_ERROR("Got a bad (or repeated) request id: ~p", [ReqIdBinary])
    end,
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    F = fun({_, Socket}) -> inter_dc_utils:close_socket(Socket) end,
    lists:foreach(F, dict:to_list(State#state.sockets)),
    ok.

handle_cast(_Request, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%%% Internal methods ---------------------------------------------------------+

%% Saves the request in the state, so it can be resent if the DC was disconnected.
req_sent(ReqIdBinary, RequestEntry, State=#state{unanswered_queries=Table, req_id=OldReq}) ->
    true = insert_request(Table, ReqIdBinary, RequestEntry),
    State#state{req_id=(OldReq+1)}.

-spec del_dc(dcid(), req_dict()) -> req_dict().
del_dc(DCID, Dict) ->
    %% filter all DCID-Partition pairs to remove them
    MatchingDCID = fun({DictDCID, _DictPartition}, _Value) -> DictDCID == DCID end,
    ToRemoveDict = dict:filter(MatchingDCID, Dict),

    %% close sockets and erase entry of input dict
    RemoveDCIDPartitionEntry =
        fun({Key, Socket}, AccDict) ->
            %% the sockets are the same, but close all of them anyway (close socket is idempotent)
            inter_dc_utils:close_socket(Socket),
            dict:erase(Key, AccDict)
        end,

    lists:foldl(RemoveDCIDPartitionEntry, Dict, dict:to_list(ToRemoveDict)).

%% A node is a list of addresses because it can have multiple interfaces
%% this just goes through the list and connects to the first interface that works
-spec connect_to_node([socket_address()]) -> {ok, zmq_socket()} | connection_error.
connect_to_node([]) ->
    ?LOG_ERROR("Unable to subscribe to DC log reader"),
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
            {_, <<?OK_MSG>>} = inter_dc_utils:check_version_and_req_id(ResBinary),
            %% Create a subscriber socket for the specified DC
            Socket = zmq_utils:create_connect_socket(req, true, Address),
            %% For each partition in the current node:
            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.


%%%===================================================================
%%%  Ets tables
%%%
%%%  unanswered_queries_table:
%%%===================================================================

-spec create_queries_table() -> ets:tid().
create_queries_table() ->
    ets:new(queries, [set]).

-spec insert_request(ets:tid(), binary(), request_cache_entry()) -> true.
insert_request(Table, ReqIdBinary, RequestEntry) ->
    ets:insert(Table, {ReqIdBinary, RequestEntry}).

-spec delete_request(ets:tid(), binary()) -> true.
delete_request(Table, ReqIdBinary) ->
    ets:delete(Table, ReqIdBinary).

-spec get_request(ets:tid(), binary()) -> not_found | {ok, request_cache_entry()}.
get_request(Table, ReqIdBinary) ->
    case ets:lookup(Table, ReqIdBinary) of
        [] ->
            not_found;
        [{ReqIdBinary, Val}] ->
            {ok, Val}
    end.
