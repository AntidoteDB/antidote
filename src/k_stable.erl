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
    deliver_tx/1,
    fetch_kvector/0]).

%% server and internal functions
-export([
    init/1,
    handle_cast/2,
    handle_call/3,
    handle_info/2,
    get_dc_ids/0,
    get_dc_vals/3,
    get_version_matrix/1,
    build_kvector/1,
    get_k_vector/0,
    generate_name/1,
    code_change/3,
    terminate/2
]).

%% Definitions
-record(state, {
    kvector :: vectorclock()
}).

%%% ETS table metadata
-define(KSTABILITY_TABLE_NAME, k_stability_table).
-define(KSTABILITY_TABLE_CONCURRENCY,
    {read_concurrency, false}, {write_concurrency, false}).
-define(KSTABILITY_REPL_FACTOR, 2). % 2 DCs total will store updates,


%% ===================================================================
%% Public API (client API)
%% ===================================================================
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_name(node())}, ?MODULE, [], []).

%% Called with new inter-dc TXs (or heartbeat). Updates the state.
-spec deliver_tx(#interdc_txn{}) -> ok.
deliver_tx(Tx) ->
    gen_server:cast({global, generate_name(node())}, {update_dc_vc, Tx}).

%% Goes through the ETS table, collects VCs from each DC,
%% and computes the k_stable (read) vector.
-spec fetch_kvector() -> vectorclock().
fetch_kvector() ->
    gen_server:call({global, generate_name(node())}, get_kvect).



%% ===================================================================
%% gen_server callbacks (server functions)
%% ===================================================================
init(_A) ->
    case ets:info(?KSTABILITY_TABLE_NAME) of
        undefined ->
            ets:new(?KSTABILITY_TABLE_NAME,
                [set, named_table, public, ?KSTABILITY_TABLE_CONCURRENCY]),
            {ok, []};
        _ -> {ok, []}
    end.

handle_cast({update_dc_vc, Tx = #interdc_txn{}}, _State) ->
    DC = Tx#interdc_txn.dcid,
    VC = Tx#interdc_txn.gss,
    ets:insert(?KSTABILITY_TABLE_NAME, {DC, VC}),
    KVect = get_k_vector(),
    ets:insert(?KSTABILITY_TABLE_NAME, {kvector, KVect}),
    NewState = state_factory(KVect),
    {noreply, NewState};

handle_cast(_Info, State) ->
    {noreply, State}.

%% Returns the k-vector
handle_call(get_kvect, _From, _State) ->
    KVec = get_k_vector(),
    NewState = state_factory(KVec),
    {reply, KVec, NewState}.



%% ===================================================================
%% internal functions
%% ===================================================================
generate_name(Node) ->
    list_to_atom("kstability_manager" ++ atom_to_list(Node)).

-spec state_factory(vectorclock()) -> #state{}.
state_factory(Kvect) ->
    #state{kvector = Kvect}.

%% Goes through the ets table and builds the
%% remote state vector. Intended to be used
%% to build version matrix.
-spec get_dc_vals(dcid(), [tuple()], [integer()]) -> [].
get_dc_vals(_, [], Acc) ->
    Acc;
get_dc_vals(DC, [{_, Dico} | T], Acc) ->
    Val = case dict:find(DC, Dico) of
              {ok, Value} ->
                  Value;
              error -> % DC not found
                  0
          end,
    get_dc_vals(DC, T, [Val | Acc]).


%% Builds the version matrix
%% The parameter is list of dcid() we want to search for.
%% Used for k-stable vector calculation
-spec get_version_matrix([]) -> [] | {error, empty_list}.
get_version_matrix([]) ->
    {error, empty_list};
get_version_matrix(DC_IDs) ->
    TabList = ets:tab2list(?KSTABILITY_TABLE_NAME),
    lists:foldl(fun(X, Acc) ->
        VC = get_dc_vals(X, TabList, []),
        [{X, VC} | Acc] end,
        [], DC_IDs).

%% Builds k-stable vector for reads from version matrix
%% k is hardcoded for now
%% takes versionmatrix as argument
-spec build_kvector([tuple()]) -> [tuple()] | {error, matrix_size}.
build_kvector([]) ->
    {error, matrix_size};
build_kvector(VerM) when length(VerM) >= ?KSTABILITY_REPL_FACTOR ->
    lists:foldl(fun(Row, Acc) ->
        {DC, VC} = Row,
        Sorted = lists:reverse(lists:sort(VC)),
        [{DC, lists:nth(?KSTABILITY_REPL_FACTOR, Sorted)} | Acc] end,
        [], VerM).


-spec get_k_vector() -> [tuple()] | {error, matrix_size}.
get_k_vector() ->
    DCs = get_dc_ids(),
    VerM = get_version_matrix(DCs),
    KVect = build_kvector(VerM),
    KVect.

-spec get_dc_ids() -> [dcid()].
get_dc_ids() ->
    List = ets:tab2list(?KSTABILITY_TABLE_NAME),
    Dico = dict:from_list(List),
    dict:fetch_keys(Dico).

terminate(_Reason, _State) ->
    ets:delete(?KSTABILITY_TABLE_NAME),
    ok.

% Unused
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

