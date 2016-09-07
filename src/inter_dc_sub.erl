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

%% InterDC subscriber - connects to remote PUB sockets and listens to a defined subset of messages.
%% The messages are filter based on a binary prefix.

-module(inter_dc_sub).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
  add_dc/3,
  del_dc/1
]).

%% Server methods
-export([
  init/1,
  start_link/0,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
  ]).

%% State
-record(state, {
  my_node_partitions :: {dict(),partition_id()}, %% Partition -> Partition
  sockets :: dict() % DCID -> socket
}).

%%%% API --------------------------------------------------------------------+

%% TODO: persist added DCs in case of a node failure, reconnect on node restart.
-spec add_dc(dcid(), [socket_address()], [partition_id()]) -> ok.
add_dc(DCID, Publishers, OtherPartitions) -> gen_server:call(?MODULE, {add_dc, DCID, Publishers, OtherPartitions}, ?COMM_TIMEOUT).

-spec del_dc(dcid()) -> ok.
del_dc(DCID) -> gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
init([]) -> {ok, #state{sockets = dict:new()}}.

handle_call({add_dc, DCID, Publishers, OtherPartitions}, _From, OldState) ->
    case ?IS_PARTIAL() of
	true -> [];
	false -> [] = OtherPartitions %% sanity check
    end,
    %% First delete the DC if it is alread connected
    {_, State} = del_dc(DCID, OldState),
    case connect_to_nodes(Publishers, [], OtherPartitions) of
	{ok, Sockets} ->
	    %% TODO maybe intercept a situation where the vnode location changes and reflect it in sub socket filer rules,
	    %% optimizing traffic between nodes inside a DC. That could save a tiny bit of bandwidth after node failure.
	    {reply, ok, State#state{sockets = dict:store(DCID, Sockets, State#state.sockets)}};
	connection_error ->
	    {reply, error, State}
    end;

handle_call({del_dc, DCID}, _From, State) ->
    {Resp, NewState} = del_dc(DCID, State),
    {reply, Resp, NewState}.

%% handle an incoming interDC transaction from a remote node.
handle_info({zmq, Socket, BinaryMsg, Flags}, State = #state{my_node_partitions = undefined}) ->
    MyPartitions = dc_utilities:get_my_partitions(),
    MyPartDict =
	lists:foldl(fun(Par, Acc) ->
			   dict:store(Par,Par,Acc)
		   end, dict:new(), MyPartitions),
    handle_info({zmq, Socket, BinaryMsg, Flags}, State#state{my_node_partitions={MyPartDict,hd(MyPartitions)}});
handle_info({zmq, _Socket, BinaryMsg, _Flags}, State) ->
  %% decode the message
  Msg = inter_dc_txn:from_bin(BinaryMsg),
  %% deliver the message to an appropriate vnode
  ok = inter_dc_sub_vnode:deliver_txn(Msg, State#state.my_node_partitions),
  {noreply, State}.

handle_cast(_Request, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, State) ->
  F = fun({_, Sockets}) -> lists:foreach(fun zmq_utils:close_socket/1, Sockets) end,
  lists:foreach(F, dict:to_list(State#state.sockets)).

del_dc(DCID, State) ->
    case dict:find(DCID, State#state.sockets) of
	{ok, Sockets} ->
	    lists:foreach(fun zmq_utils:close_socket/1, Sockets),
	    {ok, State#state{sockets = dict:erase(DCID, State#state.sockets)}};
	error ->
	    {ok, State}
    end.

-spec connect_to_nodes([[socket_address()]], [erlzmq:erlzmq_socket()] | connection_error, [partition_id()]) ->
			      connection_error | {ok, [erlzmq:erlzmq_socket()]}.
connect_to_nodes([], Acc, _OtherPartitions) ->
    {ok, Acc};
connect_to_nodes([Node|Rest], Acc, OtherPartitions) ->
    case connect_to_node(Node, OtherPartitions) of
	{ok, Socket} ->
	    connect_to_nodes(Rest, [Socket|Acc], OtherPartitions);
	connection_error ->
	    lists:foreach(fun zmq_utils:close_socket/1, Acc),
	    connection_error
    end.

-spec connect_to_node([socket_address()], [partition_id()]) ->
			     connection_error | {ok, [erlzmq:erlzmq_socket()]}.
connect_to_node([], _OtherPartitions) ->
    lager:error("Unable to subscribe to DC"),
    connection_error;
connect_to_node([Address|Rest], OtherPartitions) ->
    %% Test the connection
    Socket1 = zmq_utils:create_connect_socket(sub, false, Address),
    ok = erlzmq:setsockopt(Socket1, rcvtimeo, ?ZMQ_TIMEOUT),
    ok = zmq_utils:sub_filter(Socket1, <<>>),
    Res = erlzmq:recv(Socket1),
    ok = zmq_utils:close_socket(Socket1),
    case Res of
	{ok, _} ->
	    %% Create a subscriber socket for the specified DC
	    Socket = zmq_utils:create_connect_socket(sub, true, Address),
	    %% For each partition in the current node:
	    lists:foreach(fun(P) ->
				  %% Make the socket subscribe to messages prefixed with the given partition number
				  ok = zmq_utils:sub_filter(Socket, inter_dc_txn:get_partition_sub(P))
			  end, dc_utilities:get_my_partitions() ++ OtherPartitions),
	    {ok, Socket};
	_ ->
	    connect_to_node(Rest, OtherPartitions)
    end.
