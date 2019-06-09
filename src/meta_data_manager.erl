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

-module(meta_data_manager).
-behaviour(gen_server).

-include("antidote.hrl").

-export([start_link/1,
         remove_node/2,
         send_meta_data/4,
         add_new_meta_data/2]).
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
      table :: ets:tid()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% This is just a helper server for meta_data_sender.
%% See the meta_data_sender file for information on how to use the meta-data
%% This is a separate server from meta_data_sender, which will be triggered
%% by meta_data_sender to broadcast the data.
%% It also keeps track of the names of physical nodes in the cluster.

-spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name) ->
    gen_server:start_link({global, generate_server_name(Name, node())}, ?MODULE, [Name], []).

-spec send_meta_data(atom(), atom(), atom(), any()) -> ok.
send_meta_data(Name, DestinationNodeId, NodeId, Data) ->
    gen_server:cast({global, generate_server_name(Name, DestinationNodeId)}, {send_meta_data, NodeId, Data}).

-spec remove_node(atom(), atom()) -> ok.
remove_node(Name, NodeId) ->
    gen_server:cast({global, generate_server_name(Name, node())}, {remove_node, NodeId}).

-spec add_new_meta_data(atom(), atom()) -> ok.
add_new_meta_data(Name, NodeId) ->
    gen_server:cast({global, generate_server_name(Name, node())}, {add_new_meta_data, NodeId}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Name]) ->
    Table = ets:new(meta_data_sender:get_table_name(Name, ?REMOTE_META_TABLE_NAME), [set, named_table, protected, ?META_TABLE_CONCURRENCY]),
    {ok, #state{table = Table}}.

handle_cast({send_meta_data, NodeId, Data}, State = #state{table = Table}) ->
    true = ets:insert(Table, {NodeId, Data}),
    {noreply, State};

handle_cast({add_new_meta_data, NodeId}, State = #state{table = Table}) ->
    ets:insert_new(Table, {NodeId, undefined}),
    {noreply, State};

handle_cast({remove_node, NodeId}, State = #state{table = Table}) ->
    ets:delete(Table, NodeId),
    {noreply, State};

handle_cast(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

generate_server_name(Name, Node) ->
    list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE) ++ atom_to_list(Node)).
