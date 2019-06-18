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

-module(log_utilities).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([get_key_partition/1,
         get_preflist_from_key/1,
         get_logid_from_key/1,
         remove_node_from_preflist/1,
         get_my_node/1,
         log_record_version/0,
         check_log_record_version/1
        ]).

%% @doc get_logid_from_key computes the log identifier from a key
%%      Input:  Key:    The key from which the log id is going to be computed
%%      Return: Log id
%%
-spec get_logid_from_key(key()) -> log_id().
get_logid_from_key(Key) ->
    %HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PreflistAnn = get_preflist_from_key(Key),
    remove_node_from_preflist(PreflistAnn).

%% @doc get_key_partition returns the most probable node where a given
%%      key's logfile will be located.
-spec get_key_partition(key()) -> index_node().
get_key_partition(Key) ->
    hd(get_preflist_from_key(Key)).

%% @doc get_preflist_from_key returns a preference list where a given
%%      key's logfile will be located.
-spec get_preflist_from_key(key()) -> preflist().
get_preflist_from_key(Key) ->
    ConvertedKey = convert_key(Key),
    get_primaries_preflist(ConvertedKey).

%% @doc get_primaries_preflist returns the preflist with the primary
%%      vnodes. No matter they are up or down.
%%      Input:  A hashed key
%%      Return: The primaries preflist
%%
-spec get_primaries_preflist(non_neg_integer()) -> preflist().
get_primaries_preflist(Key)->
    NumPartitions = dc_meta_data_utilities:get_num_partitions(),
    Pos = Key rem NumPartitions + 1,
    [dc_meta_data_utilities:get_partition_at_index(Pos)].

-spec get_my_node(partition_id()) -> node().
get_my_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:index_owner(Ring, Partition).

%% @doc remove_node_from_preflist: From each element of the input
%%      preflist, the node identifier is removed
%%      Input:  Preflist: list of pairs {Partition, Node}
%%      Return: List of Partition identifiers
%%
-spec remove_node_from_preflist(preflist()) -> [partition_id()].
remove_node_from_preflist(Preflist) ->
    F = fun({P, _}) -> P end,
    lists:map(F, Preflist).

%% @doc Convert key. If the key is integer(or integer in form of binary),
%% directly use it to get the partition. If it is not integer, convert it
%% to integer using hash.
-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) ->
    case is_binary(Key) of
        true ->
            KeyInt = (catch list_to_integer(binary_to_list(Key))),
            case is_integer(KeyInt) of
                true -> abs(KeyInt);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, Key}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end;
        false ->
            case is_integer(Key) of
                true ->
                    abs(Key);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end
    end.

-spec log_record_version() -> non_neg_integer().
log_record_version() -> ?LOG_RECORD_VERSION.


%% Check the version of the log record and convert
%% to a different version if necessary
%% Checked when loading the log from disk, or
%% when log messages are received from another DC
-spec check_log_record_version(#log_record{}) -> #log_record{}.
check_log_record_version(LogRecord) ->
    %% Only support one version for now
    ?LOG_RECORD_VERSION = LogRecord#log_record.version,
    LogRecord.

-ifdef(TEST).



%% Testing remove_node_from_preflist
remove_node_from_preflist_test()->
    Preflist = [{partition1, node},
                {partition2, node},
                {partition3, node}],
    ?assertEqual([partition1, partition2, partition3],
                 remove_node_from_preflist(Preflist)).

%% Testing convert key
convert_key_test()->
    ?assertEqual(1, convert_key(1)),
    ?assertEqual(1, convert_key(-1)),
    ?assertEqual(0, convert_key(0)),
    ?assertEqual(45, convert_key(<<"45">>)),
    ?assertEqual(45, convert_key(<<"-45">>)).

-endif.
