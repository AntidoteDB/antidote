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

%% Log reader reads all transactions in the log that happened between the defined
-module(inter_dc_log_reader_query).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-record(state, {
  sockets :: dict() % DCID -> socket
}).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, query/3, add_dc/2, start_link/0]).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> {ok, #state{sockets = dict:new()}}.

handle_call({add_dc, DCID, LogReaders}, _From, State) ->
  Socket = zmq_utils:create_connect_socket(req, true, hd(LogReaders)),
  {reply, ok, State#state{sockets = dict:store(DCID, Socket, State#state.sockets)}};

handle_call({query, PDCID, From, To}, _From, State) ->
  {DCID, Partition} = PDCID,
  {ok, Socket} = dict:find(DCID, State#state.sockets),
  Request = {read_log, Partition, From, To},
  ok = erlzmq:send(Socket, term_to_binary(Request)),
  {reply, ok, State}.

handle_info({zmq, _Socket, BinaryMsg, _Flags}, State) ->
  {PDCID, Txns} = binary_to_term(BinaryMsg),
  inter_dc_sub_vnode:deliver_log_reader_resp(PDCID, Txns),
  {noreply, State}.

terminate(_Reason, _State) -> ok. %% close sockets?
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% API

query(PDCID, From, To) -> gen_server:call(?MODULE, {query, PDCID, From, To}).
add_dc(DCID, LogReaders) -> gen_server:call(?MODULE, {add_dc, DCID, LogReaders}).
