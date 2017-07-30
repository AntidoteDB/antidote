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
%%
%% This module handles calculation of K-Stability information.
%% The K is defined in /src/antidote.erl under "replication_factor"
%%
%% -------------------------------------------------------------------
%% TODO [] Get 'replication_factor' from metadata automatically when it changes

-module(k_stable).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API/Client functions
-export([
    start_link/1,
    generate_server_name/1,
    update_dc_vector/2,
    get_kvector/0
    ]).

%% server functions
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2,
    code_change/3]).

%% Definitions
-record(state, {
    table :: ets:tid(),
    kvector :: vectorclock()
}).

%%% ETS table metadata
-define(TABLE_NAME, k_stability_table).
-define(KSTABILITY_TABLE_CONCURRENCY, {read_concurrency, false}, {write_concurrency, false}).
-define(KSTABILITY_TABLE_NAME, kstability_table).

%% ===================================================================
%% Public API (client API)
%% ===================================================================
-spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name) ->
    gen_server:start_link({global, generate_server_name(node())}, ?MODULE, [Name], []).

%% Called with new inter-dc TXs (or heartbeat). Updates the state.
-spec update_dc_vector(dcid(), vectorclock()) -> ok. % ets:insert
update_dc_vector(Dcid, Vc) ->
    gen_server:cast({global, generate_server_name(node())}, {update_dc_vc, Dcid, Vc}),
    KV = get_kvector(),
    {noreply, #state{kvector = KV}}.

%% Goes through the ETS table, collects VCs from each DC,
%% and computes the k_stable (read) vector.
%% TODO [] Implement get_kvector
-spec get_kvector() -> vectorclock().
get_kvector() ->
    ok.

%% ===================================================================
%% gen_server callbacks (server functions)
%% ===================================================================
init([]) ->
    case ets:info(?KSTABILITY_TABLE_NAME) of
        undefined ->
            Table = ets:new(?KSTABILITY_TABLE_NAME, [set, named_table, public, ?KSTABILITY_TABLE_CONCURRENCY]),
            {ok, #state{table=Table}};
        _ -> {ok}
    end.

%% TODO Implement update_dc_vc
handle_cast({update_dc_vc, Tx = #interdc_txn{}}, State) ->
    {noreply, {Tx, State}}.

%%%% TODO Implement update_kvector
%%handle_cast({update_kvector}, State) ->
%%    {noreply, State}.
%% calls are synchronous
%% casts are asynchronous

generate_server_name(Node) ->
    list_to_atom("kstability_manager" ++ atom_to_list(Node)).

terminate(_Reason, _State) ->
    ets:delete(?TABLE_NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

handle_call(_Info, _From, State) ->
    {reply, error, State}.

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

kstable_test() ->
    DC1 = {'antidote1@127.0.0.1', {1490, 186897, 598677}},
    DC2 = {'antidote2@127.0.0.1', {1490, 186897, 598677}},
    DC3 = {'antidote3@127.0.0.1', {1490, 186897, 598677}},
    DC4 = {'antidote4@127.0.0.1', {1490, 186897, 598677}},
    V1 = vectorclock:from_list([{1, 4, 0, 3}]),
    V2 = vectorclock:from_list([{1, 5, 2, 4}]),
    V3 = vectorclock:from_list([{0, 5, 4, 12}]),
    V4 = vectorclock:from_list([{1, 0, 0, 12}]),
    update_dc_vector(DC1, V1),
    update_dc_vector(DC2, V2),
    update_dc_vector(DC3, V3),
    update_dc_vector(DC4, V4),
    ExpectedVC = vectorclock:from_list([{1, 4, 0, 4}]),
    OutputVC = get_k_vector(),
    Output = vectorclock:eq(ExpectedVC, OutputVC),
    ?assertEqual(Output , true).
.

- endif.
