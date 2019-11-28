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

-module(dc_meta_data_utilities).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
         dc_start_success/0,
         is_restart/0,
         load_env_meta_data/0,
         get_env_meta_data/2,
         store_env_meta_data/2,
         store_meta_data_name/1,
         get_meta_data_name/0,
         get_key/1,
         key_as_integer/1,
         store_dc_descriptors/1,
         get_dc_descriptors/0
         ]).


%% Should be called once a DC has successfully started
%% Once this is set, when the nodes in the DC are restarted
%% they will load their config from disk
-spec dc_start_success() -> ok.
dc_start_success() ->
    stable_meta_data_server:broadcast_meta_data(has_started, true).

%% This is to check if the DC had been previously started
-spec is_restart() -> boolean().
is_restart() ->
    case stable_meta_data_server:read_meta_data(has_started) of
        {ok, Value} ->
            Value;
        error ->
            false
    end.

-spec store_meta_data_name(atom()) -> ok.
store_meta_data_name(MetaDataName) ->
    stable_meta_data_server:broadcast_meta_data(meta_data_name, MetaDataName).

%% For loading enviroment varialbes
-spec get_env_meta_data(atom(), term()) -> atom().
get_env_meta_data(Name, Default) ->
    case stable_meta_data_server:read_meta_data({env, Name}) of
        {ok, Value} -> Value;
        error ->
            Val = application:get_env(antidote, Name, Default),
            ok = stable_meta_data_server:broadcast_meta_data_env({env, Name}, Val),
            Val
    end.

%% Load all envoriment variables from disk
%% Should be run on node restart
-spec load_env_meta_data() -> ok.
load_env_meta_data() ->
    lists:foreach(fun({Key, Val}) ->
                          case Key of
                              {env, Name} ->
                                  application:set_env(antidote, Name, Val);
                              _ -> ok
                          end
                  end, stable_meta_data_server:read_all_meta_data()).

%% Store an environment variable on disk
-spec store_env_meta_data(atom(), term()) -> ok.
store_env_meta_data(Name, Value) ->
    stable_meta_data_server:broadcast_meta_data_env({env, Name}, Value).

-spec get_meta_data_name() -> {ok, atom()} | error.
get_meta_data_name() ->
    stable_meta_data_server:read_meta_data(meta_data_name).


%% Store an dc descriptor
-spec store_dc_descriptors([descriptor()]) -> ok.
store_dc_descriptors(Descriptors) ->
    MergeFunc = fun(DescList, PrevDict) ->
                        lists:foldl(fun(Desc = #descriptor{dcid = DCID}, Acc) ->
                                            dict:store(DCID, Desc, Acc)
                                    end, PrevDict, DescList)
                end,
    stable_meta_data_server:broadcast_meta_data_merge(external_descriptors, Descriptors, MergeFunc, fun dict:new/0).

%% Gets the list of all known dc descriptors
-spec get_dc_descriptors() -> [descriptor()].
get_dc_descriptors() ->
    case stable_meta_data_server:read_meta_data(external_descriptors) of
        {ok, Dict} ->
            dict:fold(fun(_DCID, Desc, Acc) ->
                              [Desc | Acc]
                      end, [], Dict);
        error ->
            ?LOG_DEBUG("Could not read shared meta data for external_descriptors"),
            %% return self descriptor only
            [inter_dc_manager:get_descriptor()]
    end.


-spec get_key(term()) -> term().
get_key(Key) when is_binary(Key) ->
    binary_to_integer(Key);
get_key(Key) ->
    Key.

-spec key_as_integer(term()) -> integer().
key_as_integer(Key) when is_integer(Key)->
    Key;
key_as_integer(Key) when is_binary(Key) ->
    binary_to_integer(Key);
key_as_integer(Key) ->
    key_as_integer(term_to_binary(Key)).
