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

%% State
-record(state, {
  sockets :: dict:dict({dcid(), term()} , zmq_socket()), % {DCID, partition} -> Socket
  req_id :: non_neg_integer()
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
add_dc(DCID, LogReaders) -> gen_server:call(?MODULE, {add_dc, DCID, LogReaders}, ?COMM_TIMEOUT).

%% Disconnects from the DC.
-spec del_dc(dcid()) -> ok.
del_dc(DCID) ->
    gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT),
    ok.

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{sockets = dict:new(), req_id = 1}}.

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
    {reply, ok, State#state{sockets = NewDict}};

%% Handle an instruction to ask a remote DC.
handle_call({any_request, RequestType, {DCID, Partition}, BinaryRequest, Func}, _From, State=#state{req_id=ReqId}) ->
    ?LOG_NOTICE("Trying to perform request"),
    case dict:find({DCID, Partition}, State#state.sockets) of
        {ok, Socket} ->
            ?LOG_NOTICE("Prepare message (request #~p)", [ReqId]),
            VersionBinary = ?MESSAGE_VERSION,
            ReqIdBinary = inter_dc_txn:req_id_to_bin(ReqId),
            FullRequest = <<VersionBinary/binary, ReqIdBinary/binary, RequestType, BinaryRequest/binary>>,
%%            inter_dc_query_req:send(Socket, FullRequest, RequestType, Func),

            %% TODO don't block
            ?LOG_NOTICE("Send message to ~p ~p (~p)", [DCID, Partition, Socket]),
            ok = chumak:send(Socket, FullRequest),
            ?LOG_NOTICE("Receive message"),
            {ok, BinaryMsg} = chumak:recv(Socket),

            ?LOG_NOTICE("Process and Func"),
            <<_ReqIdBinary:?REQUEST_ID_BYTE_LENGTH/binary, RestMsg/binary>> = binary_utilities:check_message_version(BinaryMsg),
            case RestMsg of
                <<RequestType, RestBinary/binary>> -> Func(RestBinary);
                Other -> ?LOG_ERROR("Received unknown reply: ~p", [Other])
            end,

            ?LOG_NOTICE("Finish"),
            {reply, ok, State#state{req_id = ReqId + 1}};
        _ ->
            ?LOG_WARNING("Could not find ~p:~p in socket dict ~p", [DCID, Partition, State#state.sockets]),
            {reply, unknown_dc, State}
    end.

handle_info({'EXIT', Pid, Reason}, State) ->
    logger:notice("Connect query socket ~p shutdown: ~p", [Pid, Reason]),
    {noreply, State};
%% Handle a response from any of the connected sockets
%% Possible improvement - disconnect sockets unused for a defined period of time.
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    F = fun({{_DCID, _Partition}, Socket}) -> catch (inter_dc_utils:close_socket(Socket)) end,
    lists:foreach(F, dict:to_list(State#state.sockets)),
    ok.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

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
connect_to_node([]) ->
    ?LOG_ERROR("Unable to subscribe to DC log reader"),
    connection_error;
connect_to_node([_Address = {Ip, Port}| Rest]) ->
    %% Test the connection
    Socket1 = connect_req_test(Ip, Port),

    %% Always use 0 as the id of the check up message
    ReqIdBinary = inter_dc_txn:req_id_to_bin(0),
    BinaryVersion = ?MESSAGE_VERSION,

    ok = chumak:send(Socket1, <<BinaryVersion/binary, ReqIdBinary/binary, ?CHECK_UP_MSG>>),
    Response = chumak:recv(Socket1),
%%    ok = inter_dc_utils:close_socket(Socket1),
    gen_server:stop(Socket1),

    logger:warning("Test successful"),

    case Response of
        {ok, Binary} ->
            %% check that an ok msg was received
            {_, <<?OK_MSG>>} = binary_utilities:check_version_and_req_id(Binary),
            %% Create a subscriber socket for the specified DC
            Socket = connect_req(Ip, Port),
%%            {ok, Socket} = chumak:socket(req, SocketId),
%%            {ok, _Pid} = chumak:connect(Socket, tcp, inet:ntoa(Ip), Port),

            logger:warning("Started req socket with ~p (~p : ~p)", [Socket, Ip, Port]),

            %% For each partition in the current node:
            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.

connect_req_test(Ip, Port) ->
    Id = integer_to_list(rand:uniform(1000000)),%atom_to_list(node()) ++ inet:ntoa(Ip) ++ integer_to_list(Port),
    case chumak:socket(req, Id) of
        {ok, Socket} ->
            {ok, _Pid1} = chumak:connect(Socket, tcp, inet:ntoa(Ip), Port),
            Socket;
        {error, {already_started, Socket}} ->
            Socket
    end.

connect_req(Ip, Port) ->
    Id = integer_to_list(rand:uniform(1000000)),%atom_to_list(node()) ++ inet:ntoa(Ip) ++ integer_to_list(Port),
    case chumak:socket(req, Id) of
        {ok, Socket} ->
            {ok, _Pid1} = chumak:connect(Socket, tcp, inet:ntoa(Ip), Port),
            logger:warning("Opening socket ID ~p (~p)", [Id, Socket]),
            Socket;
        {error, {already_started, Socket}} ->
            logger:warning("Socket already started: ~p", [Id]),
            Socket
    end.
