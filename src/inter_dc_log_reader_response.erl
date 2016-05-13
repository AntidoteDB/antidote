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

-module(inter_dc_log_reader_response).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  get_address/0,
  get_address_list/0]).

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
-record(state, {socket, id, next}). %% socket :: erlzmq_socket()

%%%% API --------------------------------------------------------------------+

%% Fetch the local address of a log_reader socket.
-spec get_address() -> socket_address().
get_address() ->
  {ok, List} = inet:getif(),
  {Ip, _, _} = hd(List),
  {ok, Port} = application:get_env(antidote, logreader_port),
  {Ip, Port}.

-spec get_address_list() -> {[partition_id()],[socket_address()]}.
get_address_list() ->
    PartitionList = dc_utilities:get_my_partitions(),
    {ok, List} = inet:getif(),
    {ok, Port} = application:get_env(antidote, logreader_port),
    AddressList = [{Ip1,Port} || {Ip1, _, _} <- List, Ip1 /= {127, 0, 0, 1}],
    {PartitionList, AddressList}.

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  {_, Port} = get_address(),
  Socket = zmq_utils:create_bind_socket(xrep, true, Port),
  lager:info("Log reader started on port ~p", [Port]),
  {ok, #state{socket = Socket,next=getid}}.

%% Handle the remote request
handle_info({zmq, _Socket, Id, [rcvmore]}, State=#state{next=getid}) ->
    {noreply, State#state{next = blankmsg, id=Id}};
handle_info({zmq, _Socket, <<>>, [rcvmore]}, State=#state{next=blankmsg}) ->
    {noreply, State#state{next=getmsg}};
handle_info({zmq, Socket, BinaryMsg, Flags}, State=#state{id=Id,next=getmsg}) ->
    %% Decode the message
    lager:info("got the followoing ~p and ~p and ~p", [Socket, BinaryMsg,Flags]),
    Msg = binary_to_term(BinaryMsg),
    lager:info("got msg ~p in log reader resp", [Msg]),
    %% Create a response
    case Msg of
	{read_log, Partition, From, To} ->
	    send_response({{dc_meta_data_utilities:get_my_dc_id(), Partition}, get_entries(Partition, From, To)}, Id, Socket);
	{is_up} ->
	    send_response({ok}, Id, Socket);
	_ ->
	    send_response({error, bad_request}, Id, Socket)
    end,
    {noreply, State#state{next=getid}};
handle_info(Info, State) ->
    lager:info("got weird info ~p", [Info]),
    {noreply, State}.

handle_call({read_log_response, Response, Id}, _From, State=#state{socket = Socket}) ->
    send_response(Response, Id, Socket),
    {reply, ok, State};
    
handle_call(_Request, _From, State) -> {noreply, State}.
terminate(_Reason, State) -> erlzmq:close(State#state.socket).
handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%

send_response(Response, Id, Socket) ->
    BinaryResponse = term_to_binary(Response),
    ok = erlzmq:send(Socket, Id, [sndmore]),
    ok = erlzmq:send(Socket, <<>>, [sndmore]),
    ok = erlzmq:send(Socket, BinaryResponse).

%%-spec get_entries(partition_id(), log_opid(), log_opid()) -> [#interdc_txn{}].
-spec get_entries(non_neg_integer(),non_neg_integer(),non_neg_integer()) -> [].
get_entries(Partition, From, To) ->
  Logs = log_read_range(Partition, node(), From, To),
  Asm = log_txn_assembler:new_state(),
  {OpLists, _} = log_txn_assembler:process_all(Logs, Asm),
  Txns = lists:map(fun(TxnOps) -> inter_dc_txn:from_ops(TxnOps, Partition, none) end, OpLists),
  %% This is done in order to ensure that we only send the transactions we committed.
  %% We can remove this once the read_log_range is reimplemented.
  lists:filter(fun inter_dc_txn:is_local/1, Txns).

%% TODO: reimplement this method efficiently once the log provides efficient access by partition and DC (Santiago, here!)
%% TODO: also fix the method to provide complete snapshots if the log was trimmed
-spec log_read_range(partition_id(), node(), log_opid(), log_opid()) -> [#operation{}].
log_read_range(Partition, Node, From, To) ->
  {ok, RawOpList} = logging_vnode:read({Partition, Node}, [Partition]),
  OpList = lists:map(fun({_Partition, Op}) -> Op end, RawOpList),
  filter_operations(OpList, From, To).

-spec filter_operations([#operation{}], log_opid(), log_opid()) -> [#operation{}].
filter_operations(Ops, Min, Max) ->
  F = fun(Op) ->
    Num = Op#operation.op_number#op_number.local,
    (Num >= Min) and (Max >= Num)
  end,
  lists:filter(F, Ops).

