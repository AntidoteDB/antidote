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

%% InterDC subscriber - connects to remote PUB sockets and listens to a defined subset of messages.
%% The messages are filter based on a binary prefix.

-module(inter_dc_sub).

-behaviour(gen_server).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-include_lib("kernel/include/logger.hrl").

%% API
-export([add_dc/2, del_dc/1]).

%% Server methods
-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% State
-record(state, {sockets :: dict:dict(dcid(), zmq_socket())}).

%%%% API --------------------------------------------------------------------+

-spec add_dc(dcid(), [socket_address()]) -> ok | error.
add_dc(DCID, Publishers) ->
    gen_server:call(?MODULE, {add_dc, DCID, Publishers}, ?COMM_TIMEOUT).

-spec del_dc(dcid()) -> ok.
del_dc(DCID) ->
    gen_server:call(?MODULE, {del_dc, DCID}, ?COMM_TIMEOUT).

%%%% Server methods ---------------------------------------------------------+

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{sockets = dict:new()}}.

%% TODO: persist added DCs in case of a node failure, reconnect on node restart.
handle_call({add_dc, DCID, Publishers}, _From, OldState) ->
    %% First delete the DC if it is already connected
    {_, State} = del_dc(DCID, OldState),
    case connect_to_nodes(Publishers, []) of
        {ok, Sockets} ->
            %% TODO maybe intercept a situation where the vnode location changes and
            %%      reflect it in sub socket filer rules, optimizing traffic between nodes inside a DC.
            %%      That could save a tiny bit of bandwidth after node failure.
            {reply, ok, State#state{sockets = dict:store(DCID, Sockets, State#state.sockets)}};
        connection_error ->
            {reply, error, State}
    end;
handle_call({del_dc, DCID}, _From, State) ->
    {ok, NewState} = del_dc(DCID, State),
    {reply, ok, NewState}.

%% handle an incoming interDC transaction from a remote node.
handle_info({zmq, BinaryMsg}, State) ->
    %% decode the message
    Msg = inter_dc_txn:from_bin(BinaryMsg),
    %% deliver the message to an appropriate vnode
    ok = inter_dc_sub_vnode:deliver_txn(Msg),
    {noreply, State};
%% zmq connect worker shutdown
handle_info({'EXIT', Pid, Reason}, State) ->
    logger:notice("Connect socket ~p shutdown: ~p", [Pid, Reason]),
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    % close all sockets for all dcs
    F = fun ({_, Sockets}) ->
        lists:foreach(fun inter_dc_utils:close_socket/1, Sockets)
        end,
    lists:foreach(F, dict:to_list(State#state.sockets)).

del_dc(DCID, State) ->
    case dict:find(DCID, State#state.sockets) of
        {ok, Sockets} ->
            lists:foreach(fun inter_dc_utils:close_socket/1, Sockets),
            {ok, State#state{sockets = dict:erase(DCID, State#state.sockets)}};
        error ->
            {ok, State}
    end.

connect_to_nodes([], Acc) ->
    {ok, Acc};
connect_to_nodes([Node | Rest], Acc) ->
    case connect_to_node(Node) of
        {ok, Socket} ->
            connect_to_nodes(Rest, [Socket | Acc]);
        connection_error ->
            lists:foreach(fun inter_dc_utils:close_socket/1, Acc),
            connection_error
    end.

connect_to_node([]) ->
    ?LOG_ERROR("Unable to subscribe to DC"),
    connection_error;
connect_to_node([_Address = {Ip, Port} | Rest]) ->
    %% Test the connection
    {ok, Socket1} = chumak:socket(sub),
    {ok, _Pid1} = chumak:connect(Socket1, tcp, inet_parse:ntoa(Ip), Port),
    % receives a ping
    % TODO can it receive a valid publish transaction from any partition, causing message loss?
    %%     this could only happen after a restart, if anything
    ok = chumak:subscribe(Socket1, <<>>),
    Res = chumak:recv(Socket1),
    inter_dc_utils:close_socket(Socket1),
    case Res of
        {ok, _} ->
            %% Create a subscriber socket for the specified DC
            {ok, Socket} = chumak:socket(sub),

            SubscribePartitionFun = fun(Partition) ->
                %% Make the socket subscribe to messages prefixed with the given partition number
                PartitionBin = inter_dc_txn:partition_to_bin(Partition),
                ?LOG_INFO("Subscribing to ~p of ~p:~p", [PartitionBin, Ip, Port]),
                ok = chumak:subscribe(Socket, <<<<"P">>/binary, PartitionBin/binary>>)
                                    end,

            %% For each partition in the current node, subscribe
            %% this assumes that every DC has its cluster fully set up and partitions won't change anymore
            lists:foreach(SubscribePartitionFun, inter_dc_utils:get_my_partitions()),

            %% connect the socket
            {ok, _Pid} = chumak:connect(Socket, tcp, inet_parse:ntoa(Ip), Port),

            %% spawn receive subscription worker and trap exit
            process_flag(trap_exit, true),
            Self = self(),
            spawn_link(fun () ->
                ?LOG_DEBUG("Spawned sub connect worker ~p", [self()]),
                loop(Self, Socket)
                       end),

            {ok, Socket};
        _ ->
            connect_to_node(Rest)
    end.

loop(Parent, Socket) ->
    {ok, <<P:1/binary, Data/binary>>} = chumak:recv(Socket),
    <<"P">> = P, % first byte should be partition delimiter
    Parent ! {zmq, Data},
    loop(Parent, Socket).
