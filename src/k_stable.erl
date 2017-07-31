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
%% TODO [] Get 'replication_factor' to metadata to make it dynamically adjustable
%% TODO [] When delivering an interdc TX, update k-stable module

-module(k_stable).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Public API/Client functions
-export([
    start_link/0,
    deliver_update/2,
    deliver_update/1,
    get_kvector/0,
    generate_server_name/1
]).

%% server functions
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    terminate/2,
    code_change/3,
    build_kstable_vector/0
]).

%% Definitions
-record(state, {
    table :: ets:tid(),
    kvector :: vectorclock()
}).

%%% ETS table metadata
-define(KSTABILITY_TABLE_NAME, k_stability_table).
-define(KSTABILITY_TABLE_CONCURRENCY, {read_concurrency, false}, {write_concurrency, false}).
-define(KSTABILITY_REPL_FACTOR, 2).


%% ===================================================================
%% Public API (client API)
%% ===================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_server_name(node())}, ?MODULE, [], []).

%% Called with new inter-dc TXs (or heartbeat). Updates the state.
-spec deliver_update(dcid(), vectorclock()) -> ok. % ets:insert
deliver_update(Dcid, Vc) ->
    gen_server:cast({global, generate_server_name(node())}, {update_dc_vc, #interdc_txn{dcid = Dcid, gss = Vc}}).

-spec deliver_update(#interdc_txn{}) -> ok.
deliver_update(Tx) ->
    gen_server:cast({global, generate_server_name(node())}, {update_dc_vc, Tx}).

%% Goes through the ETS table, collects VCs from each DC,
%% and computes the k_stable (read) vector.
-spec get_kvector() -> vectorclock().
get_kvector() ->
    gen_server:call({global, generate_server_name(node())}, update_dc_vc).



%% ===================================================================
%% gen_server callbacks (server functions)
%% ===================================================================
init(_A) ->
    case ets:info(?KSTABILITY_TABLE_NAME) of
        undefined ->
            Table = ets:new(?KSTABILITY_TABLE_NAME, [set, named_table, public, ?KSTABILITY_TABLE_CONCURRENCY]),
            {ok, #state{table=Table}};
        _ -> {ok}
    end.

handle_cast({update_dc_vc, Tx = #interdc_txn{}}, State) ->
    DC = Tx#interdc_txn.dcid,
    VC = Tx#interdc_txn.gss,
    KVect = build_kstable_vector(),
    ets:insert(?KSTABILITY_TABLE_NAME, {DC, VC}),
    NewState = state_factory(State#state.table, #state.kvector=KVect),
    {noreply, NewState};

handle_cast(_Info, State) ->
    {noreply, State}.

%% Returns the k-vector
handle_call(get_kvect, _From, State) ->
    KVec = build_kstable_vector(),
    NewState = state_factory(State#state.table, KVec),
    {reply, KVec, NewState}.



%% ===================================================================
%% internal functions
%% ===================================================================
generate_server_name(Node) ->
    list_to_atom("kstability_manager" ++ atom_to_list(Node)).

-spec state_factory(ets:tid(), vectorclock()) -> #state{}.
state_factory(Tab, Kvect) ->
    #state{table=Tab, kvector = Kvect}.

%% This is what a vectorclock looks like
%% My ets:tab2list will look like this
%%{{'antidote2@127.0.0.1',{1490,186897,598677}}, <- dcid()
%%{dict,2,16,16,8,80,48,
%%{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%{{[[{'antidote2@127.0.0.1',{1490,186897,598677}}|15]],
%%[],[],[],[],[],[],[],[],[],[],[],
%%[[{'antidote@127.0.0.1',{1501,537303,...}}|5]],
%%[],[],[]}}} <- vectorclock()

-spec build_kstable_vector() -> vectorclock().
build_kstable_vector() ->
    _List = ets:tab2list(?KSTABILITY_TABLE_NAME),
    vectorclock:new().

terminate(_Reason, _State) ->
    ets:delete(?KSTABILITY_TABLE_NAME),
    ok.

%% Unused
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% ===================================================================
%% Tests
%% ===================================================================
-ifdef(TEST).

%%kstable_test() ->
%%    DC1 = {'antidote1@127.0.0.1', {1490, 186897, 598677}},
%%    DC2 = {'antidote2@127.0.0.1', {1490, 186897, 598677}},
%%    DC3 = {'antidote3@127.0.0.1', {1490, 186897, 598677}},
%%    DC4 = {'antidote4@127.0.0.1', {1490, 186897, 598677}},
%%    V1 = vectorclock:from_list([{1, 4, 0, 3}]),
%%    deliver_update(DC1, V1),
%%    deliver_update(DC2, V2),
%%    deliver_update(DC3, V3),
%%    deliver_update(DC4, V4),
%%    ExpectedVC = vectorclock:from_list([{1, 4, 0, 4}]),
%%    OutputVC = get_k_vector(),
%%    Output = vectorclock:eq(ExpectedVC, OutputVC),
%%    ?assertEqual(Output , true).

- endif.

%% DC1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}}.
%% DC2 = {'antidote2@127.0.0.1', {1490, 186897, 598677}}.
%% {ok, STS} = dc_utilities:get_stable_snapshot().
%% {ok,{dict,0,16,16,8,80,48,
%%  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%  {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}}
%% TE = vectorclock:set_clock_of_dc(DCID, 5, STS).
%% TE2 = vectorclock:set_clock_of_dc(DC2, 15, TE).
%% {dict,2,16,16,8,80,48,
%%  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
%%  {{[[{'antidote2@127.0.0.1',{1490,186897,598677}}|15]],
%%      [],[],[],[],[],[],[],[],[],[],[],
%%      [[{'antidote1@127.0.0.1',{1501,537303,598423}}|5]],
%%      [],[],[]}}}
%% ets:insert(?KSTABLE_TABLE_NAME, {DC1, TE2}).
