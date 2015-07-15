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
-module(log_reader).
-behaviour(gen_server).
-include("antidote.hrl").

-export([start_link/0, get_entries/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, port :: inet:port_number()}). %% socket :: erlzmq_socket()

start_link() ->
  {ok, Port} = application:get_env(antidote, logreader_port),
  start_link(Port).

start_link(Port) -> gen_server:start_link({local, ?MODULE}, ?MODULE, [Port], []).

init([Port]) ->
  Socket = create_socket(Port),
  lager:info("Log reader started on port ~p", [Port]),
  {ok, #state{socket = Socket, port = Port}}.

handle_info({zmq, Socket, BinaryMsg, _Flags}, State) ->
  Msg = binary_to_term(BinaryMsg),
  lager:info("Received MSG=~p", [Msg]),
  Response = case Msg of
    {read_log, Partition, From, To} -> get_entries(Partition, From, To);
    _ -> {error, bad_request}
  end,
  erlzmq:send(Socket, term_to_binary(Response)),
  {noreply, State}.

handle_call(_Request, _From, State) -> lager:info("call"), {noreply, State}.
terminate(_Reason, State) -> lager:info("term"), erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> lager:info("cast"), {noreply, State}.
code_change(_OldVsn, State, _Extra) -> lager:info("code"), {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_socket(Port) ->
  Ctx = zmq_context:get(),
  {ok, Socket} = erlzmq:socket(Ctx, [rep, {active, true}]),
  ConnectionString = lists:flatten(io_lib:format("tcp://~s:~p", ["*", Port])),
  ok = erlzmq:bind(Socket, ConnectionString),
  Socket.


%%%%%%%%%%%%%%%%%%%%%%%%%

get_entries(Partition, From, To) ->
  Logs = log_read_range(node(), Partition, From, To),
  Asm = log_txn_assembler:new_state(),
  {Txns, _} = log_txn_assembler:process_all(Logs, Asm),
  Txns.

%% TODO: reimplement this method efficiently once the log provides true, efficient sequential access
%% TODO: also fix the method to provide complete snapshots if the log was trimmed
log_read_range(Node, Partition, From, To) ->
  {ok, RawOpList} = logging_vnode:read({Partition, Node}, [Partition]),
  OpList = lists:map(fun({_Partition, Op}) -> Op end, RawOpList),
  filter_operations(OpList, From, To).

filter_operations(Ops, Min, Max) ->
  F = fun(Op) ->
    {Num, _Node} = Op#operation.op_number,
    (Num >= Min) and (Max >= Num)
  end,
  lists:filter(F, Ops).

