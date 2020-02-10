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

-module(meta_data_sender).
-behaviour(gen_statem).

-include("antidote.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(GET_NODE_LIST(), get_node_list_t()).
-define(GET_NODE_AND_PARTITION_LIST(), get_node_and_partition_list_t()).
-else.
-define(GET_NODE_LIST(), get_node_list()).
-define(GET_NODE_AND_PARTITION_LIST(), get_node_and_partition_list()).
-endif.

-export([start_link/1,
    start/1,
    put_meta/3,
    get_node_list/0,
    get_node_and_partition_list/0,
    get_merged_data/2,
    remove_partition/2,
    send_meta_data/3,
    callback_mode/0]).

%% Callbacks
-export([init/1,
         code_change/4,
         terminate/3]).

-record(state, {
      last_result :: term(),
      name :: atom(),
      should_check_nodes :: boolean()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% This state machine is responsible for sending meta-data that has been collected
%% on a physical node to all other physical nodes in the riak ring.
%% There will be one instance of this state machine running on each physical machine.
%% During execution of the system, vnodes may be continually writing to the meta data item.
%%
%% Periodically, as defined by META_DATA_SLEEP in antidote.hrl, it will trigger
%% itself to send the meta-data.
%% This will cause the meta-data to be broadcast to all other physical nodes in
%% the cluster.  Before sending the meta-data, it calls the merge function on the
%% meta-data stored by each vnode located at this partition.
%%
%% Each partition can store meta-data by calling the update function.
%% To synchronize meta-data, first the vnodes data for each physical node is merged,
%% then is broadcast, then the physical nodes meta-data is merged.  This way
%% network traffic is reduced.
%%
%% Once the data is fully merged, it does not immediately replace the old merged
%% data.  Instead, the UpdateFunction is called on each entry of the new and old
%% versions. It should return true if the new value should be kept, false otherwise.
%% The completely merged data can then be read using the get_merged_data function.
%%
%% InitialLocal and InitialMerged are used to prepopulate the meta-data for the vnode
%% and for the merged data.
%%
%% Note that there is one of these state machines per physical node. Meta-data is only
%% stored in memory.  Do not use this if you want to persist data safely, it
%% was designed with light heart-beat type meta-data in mind.

%% Start meta data sender
-spec start(atom()) -> ok.
start(Name) ->
    gen_statem:call(get_name(Name), start).

-spec start_link(atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name) ->
    gen_statem:start_link({local, get_name(Name)}, ?MODULE, [Name], []).

-spec get_name(atom()) -> atom().
get_name(Name) ->
    list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE)).

%% Insert meta data for some partition
-spec put_meta(atom(), partition_id(), term()) -> ok.
put_meta(Name, Partition, NewData) ->
    true = antidote_ets_meta_data:insert_meta_data(Name, Partition, NewData),
    ok.

%% Remove meta data for partition
-spec remove_partition(atom(), partition_id()) -> ok.
remove_partition(Name, Partition) ->
    true = antidote_ets_meta_data:delete_meta_data_partition(Name, Partition),
    ok.

%% Get merged meta data
-spec get_merged_data(atom(), X) -> X.
get_merged_data(Name, Default) ->
    antidote_ets_meta_data:get_meta_data_sender_merged_data(Name, Default).

%% ===================================================================
%% gen_statem callbacks
%% ===================================================================

init([Name]) ->
    _MetaTable = antidote_ets_meta_data:create_meta_data_table(Name),
    _StableTable = antidote_ets_meta_data:create_meta_data_sender_table(Name),
    true = antidote_ets_meta_data:insert_meta_data_sender_merged_data(Name, Name:initial_merged()),
    {ok, send_meta_data, #state{last_result = Name:initial_local(),
                                name = Name,
                                should_check_nodes = true}}.

send_meta_data({call, Sender}, start, State) ->
    {next_state, send_meta_data, State#state{should_check_nodes = true},
        [{reply, Sender, ok}, {state_timeout, ?META_DATA_SLEEP, timeout}]
    };

%% internal timeout transition
send_meta_data(state_timeout, timeout, State) ->
    send_meta_data(cast, timeout, State);
send_meta_data(cast, timeout, State = #state{last_result = LastResult,
                                       name = Name,
                                       should_check_nodes = CheckNodes}) ->
    {WillChange, Data} = get_merged_meta_data(Name, CheckNodes),
    NodeList = ?GET_NODE_LIST(),
    LocalMerged = maps:get(local_merged, Data),
    MyNode = node(),
    ok = lists:foreach(fun(Node) ->
                           ok = meta_data_manager:send_meta_data(Name, Node, MyNode, LocalMerged)
                       end, NodeList),

    MergedDict = Name:merge(Data),
    {HasChanged, NewResult} = update_stable(LastResult, MergedDict, Name),
    Store = case HasChanged of
                true ->
                    %% update changed counter for this metadata type
                    %?STATS({metadata_updated, Name}),
                    true = antidote_ets_meta_data:insert_meta_data_sender_merged_data(Name, NewResult),
                    NewResult;
                false ->
                    LastResult
            end,
    {next_state, send_meta_data, State#state{last_result = Store, should_check_nodes = WillChange},
        [{state_timeout, ?META_DATA_SLEEP, timeout}]
    }.

callback_mode() -> state_functions.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.

%% ===================================================================
%% Private functions
%% ===================================================================

%% @private
-spec remote_table_ready(atom()) -> boolean().
remote_table_ready(Name) ->
    antidote_ets_meta_data:remote_table_ready(Name).

%% @private
-spec update(atom(), map(), [atom()], fun((atom(), atom(), T) -> any()), fun((atom(), atom()) -> any()), T) -> map().
update(Name, MetaData, Entries, AddFun, RemoveFun, Initial) ->
    {NewMetaData, NodeErase} = lists:foldl(fun(NodeId, {Acc, Acc2}) ->
                            AccNew = case maps:find(NodeId, MetaData) of
                                        {ok, Val} ->
                                            maps:put(NodeId, Val, Acc);
                                        error     ->
                                            %% Put a record in the ets table because there is none for this node, yet
                                            AddFun(Name, NodeId, Initial),
                                            maps:put(NodeId, undefined, Acc)
                                     end,
                            Acc2New = maps:remove(NodeId, Acc2),
                            {AccNew, Acc2New}
                            end, {maps:new(), MetaData}, Entries),
    %% Remove entries that no longer exist
    _ = maps:map(fun(NodeId, _Val) -> ok = RemoveFun(Name, NodeId) end, NodeErase),
    NewMetaData.

%% @private
-spec get_merged_meta_data(atom(), boolean()) -> {boolean(), term()} | not_ready.
get_merged_meta_data(Name, CheckNodes) ->
    case remote_table_ready(Name) of
        false ->
            not_ready;
        true ->
            {NodeList, PartitionList, WillChange} = ?GET_NODE_AND_PARTITION_LIST(),
            Remote = antidote_ets_meta_data:get_remote_meta_data_as_map(Name),
            Local = antidote_ets_meta_data:get_meta_data_as_map(Name),
            %% Be sure that you are only checking active nodes
            %% This isn't the most efficient way to do this because are checking the list
            %% of nodes and partitions every time to see if any have been removed/added
            %% This is only done if the ring is expected to change, but should be done
            %% differently (check comment in get_node_and_partition_list())
            {NewRemote, NewLocal} = case CheckNodes of
                    true ->
                        {update(Name, Remote, NodeList, fun meta_data_manager:add_node/3, fun meta_data_manager:remove_node/2, undefined),
                         update(Name, Local, PartitionList, fun put_meta/3, fun remove_partition/2, Name:default())};
                    false ->
                        {Remote, Local}
                    end,
            LocalMerged = Name:merge(NewLocal),
            {WillChange, maps:put(local_merged, LocalMerged, NewRemote)}
    end.

%% @private
-spec update_stable(term(), term(), atom()) -> {boolean(), term()}.
update_stable(LastResult, NewData, Name) ->
    Name:fold(fun(DcId, Time, {Bool, Acc}) ->
                  Last = Name:lookup(DcId, LastResult),
                  case Name:update(Last, Time) of
                      true ->
                          {true, Name:store(DcId, Time, Acc)};
                      false ->
                          {Bool, Acc}
                  end
              end, {false, LastResult}, NewData).

%% @private
-spec get_node_list() -> [node()].
get_node_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    get_node_list(Ring).

%% @private
get_node_list(Ring) ->
    MyNode = node(),
    lists:delete(MyNode, riak_core_ring:ready_members(Ring)).

%% @private
-spec get_node_and_partition_list() -> {[node()], [partition_id()], true}.
get_node_and_partition_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NodeList = get_node_list(Ring),
    PartitionList = riak_core_ring:my_indices(Ring),
    %% TODO Deciding if the nodes might change by checking the is_resizing function is not
    %% safe can cause inconsistencies under concurrency, so this should
    %% be done differently
    %% Resize = riak_core_ring:is_resizing(Ring) or riak_core_ring:is_post_resize(Ring) or riak_core_ring:is_resize_complete(Ring),
    Resize = true,
    {NodeList, PartitionList, Resize}.


-ifdef(TEST).

meta_data_sender_test_() ->
    {foreach,
     fun start/0,
     fun stop/1,
     [
        fun empty_test_/1,
        fun merge_test_/1,
        fun merge_additional_test_/1,
        fun missing_test_/1,
        fun merge_node_change_test_/1,
        fun merge_node_change_additional_test_/1,
        fun merge_node_delete_test_/1,
        fun merge_node_delete_another_test_/1
    ]
    }.

start() ->
    MetaType = stable_time_functions,
    _Table  = antidote_ets_meta_data:create_meta_data_sender_table(MetaType),
    _Table2 = antidote_ets_meta_data:create_meta_data_table(MetaType),
    _Table3 = ets:new(node_table, [set, named_table, public]),
    _Table4 = antidote_ets_meta_data:create_remote_meta_data_table(MetaType),
    true = antidote_ets_meta_data:insert_meta_data_sender_merged_data(MetaType, MetaType:initial_merged()),
    MetaType.

stop(MetaType) ->
    true = antidote_ets_meta_data:delete_meta_data_table(MetaType),
    true = antidote_ets_meta_data:delete_meta_data_sender_table(MetaType),
    true = ets:delete(node_table),
    true = antidote_ets_meta_data:delete_remote_meta_data_table(MetaType),
    ok.

-spec set_nodes_and_partitions_and_willchange([node()], [partition_id()], boolean()) -> ok.
set_nodes_and_partitions_and_willchange(Nodes, Partitions, WillChange) ->
    true = ets:insert(node_table, {nodes, Nodes}),
    true = ets:insert(node_table, {partitions, Partitions}),
    true = ets:insert(node_table, {willchange, WillChange}),
    ok.

%% Basic empty test
empty_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2, p3], false),

    put_meta(MetaType, p1, vectorclock:new()),
    put_meta(MetaType, p2, vectorclock:new()),
    put_meta(MetaType, p3, vectorclock:new()),

    {false, Meta} = get_merged_meta_data(MetaType, false),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual([], vectorclock:to_list(LocalMerged)).


%% This test checks to make sure that merging is done correctly for multiple partitions
merge_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], false),
    %ets:delete_all_objects(get_table_name(MetaType, ?META_TABLE_NAME)),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    {false, Meta} = get_merged_meta_data(MetaType, false),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([{dc1, 5}, {dc2, 5}]), LocalMerged).

merge_additional_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1, n2], [p1, p2, p3], false),
    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    put_meta(MetaType, p3, vectorclock:from_list([{dc1, 20}, {dc2, 20}])),
    {false, Meta} = get_merged_meta_data(MetaType, false),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([{dc1, 5}, {dc2, 5}]), LocalMerged).

%% Be sure that when you are missing a partition in your meta_data that you get a 0 value for the vectorclock.
missing_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2, p3], false),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}])),
    put_meta(MetaType, p3, vectorclock:from_list([{dc1, 10}])),
    remove_partition(MetaType, p2),

    {false, Meta} = get_merged_meta_data(MetaType, true),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([]), LocalMerged).

%% This test checks to make sure that merging is done correctly for multiple partitions
%% when you have a node that is removed from the cluster.
merge_node_change_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], true),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),

    {true, Meta} = get_merged_meta_data(MetaType, false),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([{dc1, 5}, {dc2, 5}]), LocalMerged).

merge_node_change_additional_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1, n2], [p1, p3], true),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 10}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 5}])),
    put_meta(MetaType, p3, vectorclock:from_list([{dc1, 20}, {dc2, 20}])),
    {true, Meta} = get_merged_meta_data(MetaType, true),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([{dc1, 10}, {dc2, 10}]), LocalMerged).

merge_node_delete_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], true),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    put_meta(MetaType, p3, vectorclock:from_list([{dc1, 0}, {dc2, 0}])),

    {true, Meta} = get_merged_meta_data(MetaType, false),
    LocalMerged = maps:get(local_merged, Meta),
    ?_assertEqual(vectorclock:from_list([]), LocalMerged).


merge_node_delete_another_test_(MetaType) ->
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], true),

    put_meta(MetaType, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(MetaType, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    put_meta(MetaType, p3, vectorclock:from_list([{dc1, 0}, {dc2, 0}])),

    {true, Meta2} = get_merged_meta_data(MetaType, true),
    LocalMerged2 = maps:get(local_merged, Meta2),
    ?_assertEqual( vectorclock:from_list([{dc1, 5}, {dc2, 5}]), LocalMerged2).

get_node_list_t() ->
    [{nodes, Nodes}] = ets:lookup(node_table, nodes),
    Nodes.

get_node_and_partition_list_t() ->
    [{willchange, WillChange}] = ets:lookup(node_table, willchange),
    [{nodes, Nodes}] = ets:lookup(node_table, nodes),
    [{partitions, Partitions}] = ets:lookup(node_table, partitions),
    {Nodes, Partitions, WillChange}.

-endif.
