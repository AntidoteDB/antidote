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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc A limited sized, object caching for object snapshots that
%%%      must persist in memory for faster accesses.
%%%      The building structure for caching objects is the Cache
%%%      dependency (https://github.com/fogfish/cache).
%%%      The current settings for this cache are:
%%%        - type: set
%%%        - policy: lru
%%%        - ttl: 120
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(metadata_caching).

-define(CACHE, antidote_cache).
-define(KEY_NOT_FOUND, {error, key_not_found}).

%% API
-export([key_exists/1,
         key_exists/2,
         insert_key/2,
         insert_key/3,
         lookup_key/1,
         lookup_key/2,
         get_key/1,
         get_key/2,
         remove_key/1,
         remove_key/2]).

key_exists(Key) ->
    cache:has(?CACHE, Key).
key_exists(Key, TxId) ->
    {_, TS, _} = TxId,
    key_exists({Key, TS}).

insert_key(Key, Value) ->
    %lager:info("Storing new object on cache: {~p, ~p}~n", [Key, Value]),
    cache:put(?CACHE, Key, Value).

%% Save the transaction timestamp alongside the key
insert_key(Key, Value, TxId) ->
    {_, TS, _} = TxId,
    insert_key({Key, TS}, Value).

lookup_key(Key) ->
    case cache:lookup(?CACHE, Key) of
        undefined ->
            %lager:info("Could not find key ~p on cache~n", [Key]),
            ?KEY_NOT_FOUND;
        Value ->
            %lager:info("Found key ~p on cache: ~p~n", [Key, Value]),
            Value
    end.
lookup_key(Key, TxId) ->
    {_, TS, _} = TxId,
    lookup_key({Key, TS}).

get_key(Key) ->
    case cache:get(?CACHE, Key) of
        undefined ->
            %lager:info("Could not get key ~p on cache~n", [Key]),
            ?KEY_NOT_FOUND;
        Value ->
            %lager:info("Got key ~p on cache: ~p~n", [Key, Value]),
            Value
    end.
get_key(Key, TxId) ->
    {_, TS, _} = TxId,
    get_key({Key, TS}).

remove_key(Key) ->
    %lager:info("Removing key ~p from cache~n", [Key]),
    cache:remove(?CACHE, Key).
remove_key(Key, TxId) ->
    {_, TS, _} = TxId,
    remove_key({Key, TS}).


%% ====================================================================
%% Internal functions
%% ====================================================================
%%build_select(Table, MatchHead, Guards, Result) ->
%%    MatchSpec = [{MatchHead, Guards, Result}],
%%    ets:select(Table, MatchSpec).
%%
%%whole_table(Table) ->
%%    MatchHead = '$1',   %% match all
%%    Result = '$$',      %% match all
%%    build_select(Table, MatchHead, [], [Result]).
