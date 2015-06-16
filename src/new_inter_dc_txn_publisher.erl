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
-module(new_inter_dc_txn_publisher).
-behaviour(gen_server).
-include("antidote.hrl").

-export([start_link/0, publish/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {pending :: list()}).

start_link() -> gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

init([]) -> {ok, #state{pending = []}}.

handle_call({publish, Transaction}, _From, State) ->
  %% TODO insert transaction causal ordering here
  new_inter_dc_pub:broadcast({replicate, Transaction}),
  {reply, ok, State}.

terminate(_Reason, _State) -> ok.
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%%%%
publish(Transaction) -> gen_server:call({global, ?MODULE}, {publish, Transaction}).
