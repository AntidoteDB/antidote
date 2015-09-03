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

-module(meta_data_manager).
-behaviour(gen_server).

-include("antidote.hrl").

-export([start_link/0,
	 generate_server_name/1,
	 remove_node/1,
	 send_meta_data/3]).
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

%% This is just a helper fsm for meta_data_sender.
%% See the meta_data_sender file for information on how to use the meta-data
%% This is a seperate fsm from meta_data_sender, which will be triggered
%% by meta_data_sender to broadcast the data.
%% It also keeps track of the names of physical nodes in the cluster.

-spec start_link() -> {ok,pid()} | ignore | {error,term()}.
start_link() ->
    gen_server:start_link({global,generate_server_name(node())}, ?MODULE, [], []).

%% Add a list of DCs to this DC
-spec send_meta_data(atom(),atom(),dict()) -> ok.
send_meta_data(DestinationNodeId,NodeId,Dict) ->
    gen_server:cast({global,generate_server_name(DestinationNodeId)}, {update_meta_data, NodeId, Dict}).

-spec remove_node(atom()) -> ok.
remove_node(NodeId) ->
    gen_server:cast({global,generate_server_name(node())}, {remove_node,NodeId}).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    Table = ets:new(?REMOTE_META_TABLE_NAME, [set, named_table, protected, ?META_TABLE_CONCURRENCY]),
    {ok, #state{table=Table}}.

handle_cast({update_meta_data, NodeId, Dict}, State) ->
    true = ets:insert(?REMOTE_META_TABLE_NAME, {NodeId, Dict}),
    {noreply, State};

handle_cast({remove_node,NodeId}, State) ->
    true = ets:delete(?REMOTE_META_TABLE_NAME, NodeId),
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

generate_server_name(Node) ->
    list_to_atom("meta_manager" ++ atom_to_list(Node)).

