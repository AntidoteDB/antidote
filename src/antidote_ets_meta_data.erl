%% -------------------------------------------------------------------
%%
%% Copyright <2013-2020> <
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
-module(antidote_ets_meta_data).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(META_TABLE_NAME, a_meta_data_table).
-define(REMOTE_META_TABLE_NAME, a_remote_meta_data_table).
-define(META_DATA_SENDER_TABLE_NAME, a_meta_data_sender_table).

-export([create_meta_data_table/1,
    create_meta_data_sender_table/1,
    create_remote_meta_data_table/1,
    insert_meta_data_sender_merged_data/2,
    insert_meta_data/3,
    delete_meta_data_partition/2,
    get_meta_data_sender_merged_data/2,
    remote_table_ready/1,
    get_meta_data_as_map/1,
    get_remote_meta_data_as_map/1,
    delete_meta_data_table/1,
    delete_meta_data_sender_table/1,
    delete_remote_meta_data_table/1,
    insert_remote_meta_data/3,
    insert_remote_meta_data_new/3,
    delete_remote_meta_data_node/2]).

%%%===================================================================
%%% API
%%%===================================================================

- spec create_meta_data_table(atom()) -> ets:tab().
create_meta_data_table(Name) ->
    ets:new(get_table_name(Name, ?META_TABLE_NAME), [set, named_table, public, ?META_TABLE_CONCURRENCY]).

- spec create_meta_data_sender_table(atom()) -> ets:tab().
create_meta_data_sender_table(Name) ->
    ets:new(get_table_name(Name, ?META_DATA_SENDER_TABLE_NAME), [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]).

- spec create_remote_meta_data_table(atom()) -> ets:tab().
create_remote_meta_data_table(Name) ->
    ets:new(get_table_name(Name, ?REMOTE_META_TABLE_NAME), [set, named_table, protected, ?META_TABLE_CONCURRENCY]).

- spec insert_meta_data_sender_merged_data(atom(), term()) -> true.
insert_meta_data_sender_merged_data(Name, Data) ->
    ets:insert(get_table_name(Name, ?META_DATA_SENDER_TABLE_NAME), {merged_data, Data}).

-spec insert_meta_data(atom(), partition_id(), term()) -> true.
insert_meta_data(Name, Partition, Data) ->
    ets:insert(get_table_name(Name, ?META_TABLE_NAME), {Partition, Data}).


%% Remove meta data for partition
-spec delete_meta_data_partition(atom(), partition_id()) -> true.
delete_meta_data_partition(Name, Partition) ->
    ets:delete(get_table_name(Name, ?META_TABLE_NAME), Partition).

-spec get_meta_data_sender_merged_data(atom(), X) -> X.
get_meta_data_sender_merged_data(Name, Default) ->
    case ets:lookup(get_table_name(Name, ?META_DATA_SENDER_TABLE_NAME), merged_data) of
        [] ->
            Default;
        [{merged_data, Other}] ->
            Other
    end.

-spec remote_table_ready(atom()) -> boolean().
remote_table_ready(Name) ->
    case ets:info(get_table_name(Name, ?REMOTE_META_TABLE_NAME)) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec get_meta_data_as_map(atom()) -> map().
get_meta_data_as_map(Name) ->
    maps:from_list(ets:tab2list(get_table_name(Name, ?META_TABLE_NAME))).

-spec get_remote_meta_data_as_map(atom()) ->map().
get_remote_meta_data_as_map(Name) ->
    maps:from_list(ets:tab2list(get_table_name(Name, ?REMOTE_META_TABLE_NAME))).

-spec delete_meta_data_table(atom()) -> true.
delete_meta_data_table(Name) ->
    ets:delete(get_table_name(Name, ?META_TABLE_NAME)).

-spec delete_meta_data_sender_table(atom()) -> true.
delete_meta_data_sender_table(Name) ->
    ets:delete(get_table_name(Name, ?META_DATA_SENDER_TABLE_NAME)).

-spec delete_remote_meta_data_table(atom()) -> true.
delete_remote_meta_data_table(Name) ->
    ets:delete(get_table_name(Name, ?REMOTE_META_TABLE_NAME)).

-spec insert_remote_meta_data(ets:tab(), atom(), any()) -> true.
insert_remote_meta_data(Table, NodeId, Data) ->
    ets:insert(Table, {NodeId, Data}).

-spec insert_remote_meta_data_new(ets:tab(), atom(), any()) -> true.
insert_remote_meta_data_new(Table, NodeId, Data) ->
    ets:insert_new(Table, {NodeId, Data}).

-spec delete_remote_meta_data_node(ets:tab(), atom()) -> true.
delete_remote_meta_data_node(Table, NodeId) ->
    ets:delete(Table, NodeId).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
-spec get_table_name(atom(), atom()) -> atom().
get_table_name(Name, TableName) ->
    list_to_atom(atom_to_list(Name) ++ atom_to_list(TableName) ++ atom_to_list(node())).
