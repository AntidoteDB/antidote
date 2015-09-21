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

%% Log reader client - stores the ZeroMQ socket connections to all other DCs,
%% performs queries and returns responses to appropriate vnodes.

%% The unanswered_query caching is there only for the purpose of disconnecting DCs.
%% The reliability-related features like resending the query are handled by ZeroMQ.


-module(inter_dc_log_reader_query).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  query/3,
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
  sockets :: dict(), % DCID -> socket
  unanswered_queries :: dict() % PDCID -> query
}).

%%%% API --------------------------------------------------------------------+

%% Instructs the log reader to ask the remote DC for a given range of operations.
%% Instead of a simple request/response with blocking, the result is delivered
%% asynchronously to inter_dc_sub_vnode.
-spec query(pdcid(), log_opid(), log_opid()) -> ok | unknown_dc.
query(PDCID, From, To) -> gen_server:call(?MODULE, {query, PDCID, From, To}).

%% Adds the address of the remote DC to the list of available sockets.
-spec add_dc(dcid(), [socket_address()]) -> ok.
add_dc(DCID, LogReaders) -> gen_server:call(?MODULE, {add_dc, DCID, LogReaders}).

%% Disconnects from the DC.
-spec del_dc(dcid()) -> ok.
del_dc(DCID) -> gen_server:call(?MODULE, {del_dc, DCID}).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> {ok, #state{sockets = dict:new(), unanswered_queries = dict:new()}}.

%% Handle the instruction to add a new DC.
handle_call({add_dc, DCID, LogReaders}, _From, State) ->
  %% Create a socket and store it
  Socket = zmq_utils:create_connect_socket(req, true, hd(LogReaders)),
  NewState = State#state{sockets = dict:store(DCID, Socket, State#state.sockets)},

  F = fun({{QDCID, _}, Request}) ->
    %% if there are unanswered queries that were sent to the DC we just connected with, resend them
    case QDCID == DCID of
      true -> erlzmq:send(Socket, term_to_binary(Request));
      false -> ok
    end
  end,

  lists:foreach(F, dict:to_list(NewState#state.unanswered_queries)),
  {reply, ok, NewState};

%% Remove a DC. Unanswered queries are left untouched.
handle_call({del_dc, DCID}, _From, State) ->
  ok = zmq_utils:close_socket(dict:fetch(DCID, State#state.sockets)),
  {reply, ok, State#state{sockets = dict:erase(DCID, State#state.sockets)}};

%% Handle an instruction to ask a remote DC.
handle_call({query, PDCID, From, To}, _From, State) ->
  {DCID, Partition} = PDCID,
  case dict:find(DCID, State#state.sockets) of
    %% If socket found
    {ok, Socket} ->
      Request = {read_log, Partition, From, To},
      ok = erlzmq:send(Socket, term_to_binary(Request)),
      {reply, ok, req_sent(PDCID, Request, State)};
    %% If socket not found
    _ -> {reply, unknown_dc, State}
  end.

%% Handle a response from any of the connected sockets
%% Possible improvement - disconnect sockets unused for a defined period of time.
handle_info({zmq, _Socket, BinaryMsg, _Flags}, State) ->
  {PDCID, Txns} = binary_to_term(BinaryMsg),
  inter_dc_sub_vnode:deliver_log_reader_resp(PDCID, Txns),
  {noreply, rsp_rcvd(PDCID, State)}.

terminate(_Reason, State) ->
  F = fun({_, Socket}) -> zmq_utils:close_socket(Socket) end,
  lists:foreach(F, dict:to_list(State#state.sockets)).

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% Saves the request in the state, so it can be resent if the DC was disconnected.
req_sent(PDCID, Req, State) -> State#state{unanswered_queries = dict:store(PDCID, Req, State#state.unanswered_queries)}.

%% Removes the request from the list of unanswered queries.
rsp_rcvd(PDCID, State) -> State#state{unanswered_queries = dict:erase(PDCID, State#state.unanswered_queries)}.

