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
%%% @doc An limited sized, object caching for object snapshots that
%%%      must persist in memory for faster accesses.
%%%      The building structure for caching objects is the Cache
%%%      dependency (https://github.com/fogfish/cache).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(object_caching).

-define(CACHE, antidote_cache).
%-define(CACHE_CONF, [{type, set}, {policy, lru}, {ttl, 300}]).
-define(KEY_NOT_FOUND, {error, key_not_found}).

%% API
-export([key_exists/1,
         insert_key/2,
         lookup_key/1,
         get_key/1,
         remove_key/1]).

key_exists(Key) ->
    cache:has(?CACHE, Key).

insert_key(Key, Value) ->
    cache:put(?CACHE, Key, Value).

lookup_key(Key) ->
    case cache:lookup(?CACHE, Key) of
        undefined -> ?KEY_NOT_FOUND;
        Value -> Value
    end.

get_key(Key) ->
    case cache:get(?CACHE, Key) of
        undefined -> ?KEY_NOT_FOUND;
        Value -> Value
    end.

remove_key(Key) ->
    cache:remove(?CACHE, Key).

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
