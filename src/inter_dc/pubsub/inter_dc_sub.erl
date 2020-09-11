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

-type conn_err() :: connection_error.

%% API
-export([add_dc/2, del_dc/1]).

%% Server methods
-export([init/1, start_link/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% State
-record(state, {sockets :: dict:dict(dcid(), zmq_socket())}).

%%%% API --------------------------------------------------------------------+

-spec add_dc(dcid(), [socket_address()]) -> ok | error.
add_dc(DCID, Publishers) ->
    gen_server:call(?MODULE, {add_dc, DCID, Publishers}).

-spec del_dc(dcid()) -> ok.
del_dc(DCID) ->
    gen_server:call(?MODULE, {del_dc, DCID}).

%%%% Server methods ---------------------------------------------------------+

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{sockets = dict:new()}}.

handle_call({add_dc, DCID, Publishers}, _From, State) ->
    %% First delete the DC if it is already connected
    {_, NewDict} = del_dc(DCID, State#state.sockets),
    case connect_to_nodes(Publishers, []) of
        {ok, Sockets} ->
            {reply, ok, State#state{sockets = dict:store(DCID, Sockets, NewDict)}};
        connection_error ->
            {reply, error, State}
    end;
handle_call({del_dc, DCID}, _From, State) ->
    {ok, NewDict} = del_dc(DCID, State#state.sockets),
    {reply, ok, State#state{sockets = NewDict}}.

%% handle an incoming interDC transaction from a remote node.
handle_info({zmq, BinaryMsg}, State) ->
    %% decode and deliver to corresponding vnode
    Msg = inter_dc_txn:from_bin(BinaryMsg),
    ok = inter_dc_sub_vnode:deliver_txn(Msg),
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, State) ->
    logger:info("Subsriber connect socket ~p shutdown: ~p", [Pid, Reason]),
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    % close all sockets for all dcs
    F = fun ({_, Sockets}) -> lists:foreach(fun inter_dc_utils:close_socket/1, Sockets) end,
    lists:foreach(F, dict:to_list(State#state.sockets)).



%%%% Internal methods ---------------------------------------------------------+

-spec del_dc(dcid(), dict:dict(dcid(), zmq_socket())) -> {ok, dict:dict(dcid(), zmq_socket())}.
del_dc(DCID, DCIDSocketDict) ->
    case dict:find(DCID, DCIDSocketDict) of
        {ok, Sockets} ->
            lists:foreach(fun inter_dc_utils:close_socket/1, Sockets),
            {ok, dict:erase(DCID, DCIDSocketDict)};
        error ->
            {ok, DCIDSocketDict}
    end.

-spec connect_to_nodes([socket_address()], [zmq_socket()]) -> {ok, [zmq_socket()]} | conn_err().
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

-spec connect_to_node([socket_address()]) -> {ok, zmq_socket()} | conn_err().
connect_to_node([]) ->
    ?LOG_ERROR("Unable to subscribe to DC"),
    connection_error;
connect_to_node([_Address = {Ip, Port} | Rest]) ->
    %% Test the connection
    case sub_socket_and_connect(Ip, Port) of
        {ok, Socket1} ->
            %% receives a ping
            %% TODO can it receive a valid publish transaction from any partition, causing message loss?
            %%      this could only happen after a restart, if anything
            ok = chumak:subscribe(Socket1, <<>>),
            Res = chumak:recv(Socket1),
            inter_dc_utils:close_socket(Socket1),

            case Res of
                {ok, _} ->
                    %% Create a subscriber socket for the specified DC
                    {ok, Socket} = chumak:socket(sub),

                    SubscribePartitionFun = fun(Partition) ->
                        %% Make the socket subscribe to messages prefixed with the given partition number and <<"P">>
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
                    Self = self(),
                    spawn_link(fun () ->
                        ?LOG_DEBUG("Spawned sub connect worker ~p", [self()]),
                        loop(Self, Socket)
                               end),

                    {ok, Socket};
                _ ->
                    connect_to_node(Rest)
            end;

        connection_error -> connect_to_node(Rest)
    end.


-spec sub_socket_and_connect(inet:ip_address(), inet:port_number()) -> {ok, zmq_socket()} | conn_err().
sub_socket_and_connect(Ip, Port) ->
    case chumak:socket(sub) of
        {ok, Socket1} ->
            case chumak:connect(Socket1, tcp, inet_parse:ntoa(Ip), Port) of
                {ok, _Pid} ->
                    {ok, Socket1};
                {error, Reason} ->
                    ?LOG_WARNING("Could not connect to publisher: ~p", [Reason]),
                    connection_error
            end;
        {error, Reason} ->
            ?LOG_ERROR("Could not create subscriber socket: ~p", [Reason]),
            connection_error
    end.


loop(Parent, Socket) ->
    {ok, <<P:1/binary, Data/binary>>} = chumak:recv(Socket),
    <<"P">> = P, % first byte should be partition delimiter
    Parent ! {zmq, Data},
    loop(Parent, Socket).
