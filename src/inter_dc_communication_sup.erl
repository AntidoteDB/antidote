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

%% @doc : The supervisor in charge of all the socket acceptors.
%%  Supervisor starts up a pool of listeners which can accept incoming
%%  connection request from other DCs

-module(inter_dc_communication_sup).
-behaviour(supervisor).

-export([start_link/1, start_socket/0]).
-export([init/1]).

start_link(Port) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Port]).

init([Port]) ->
    {ok, ListenSocket} = gen_tcp:listen(
                           Port,
                           [{active,false}, binary,
                            {packet,2},{reuseaddr, true}
                           ]),
    spawn_link(fun empty_listeners/0),
    {ok, {{simple_one_for_one, 60, 3600},
         [{socket,
          {inter_dc_communication_recvr, start_link, [ListenSocket]}, % pass the socket!
          temporary, 1000, worker, [inter_dc_communication_recvr]}
         ]}}.

start_socket() ->
    supervisor:start_child(?MODULE, []).

%% Start with 20 listeners so that many multiple connections can
%% be started at once, without serialization.
empty_listeners() ->
    _ = [start_socket() || _ <- lists:seq(1,20)],
    ok.
