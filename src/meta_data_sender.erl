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

-export([start_link/9,
    start/1,
    put_meta/3,
    get_meta/3,
    get_node_list/0,
    get_node_and_partition_list/0,
    get_merged_data/2,
    remove_partition/2,
    get_name/2,
    send_meta_data/3,
    callback_mode/0]).

%% Callbacks
-export([init/1,
         code_change/4,
         terminate/3]).

-record(state, {
      table :: ets:tid(),
      last_result :: term(),
      name :: atom(),
      update_function :: fun((term(), term()) -> boolean()),
      merge_function :: fun((term()) -> term()),
      lookup_function :: fun(),
      store_function :: fun(),
      fold_function :: fun(),
      default :: term(),
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

-spec start_link(atom(), fun(), fun(), fun(), fun(), fun(), term(), term(), term()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, UpdateFun, MergeFun, LookupFun, StoreFun, FoldFun, Default, InitialLocal, InitialMerged) ->
    gen_statem:start_link({local, list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE))},
               ?MODULE, [Name, UpdateFun, MergeFun, LookupFun, StoreFun, FoldFun, Default, InitialLocal, InitialMerged], []).

-spec start(atom()) -> ok.
start(Name) ->
    gen_statem:call(list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE)), start).

-spec put_meta(atom(), partition_id(), term()) -> ok | undefined.
put_meta(Name, Partition, NewData) ->
    MetaTableName = get_name(Name, ?META_TABLE_NAME),
    case ets:info(MetaTableName) of
        undefined ->
            undefined;
        _ ->
            true = ets:insert(MetaTableName, {Partition, NewData}),
            ok
    end.


-spec get_meta(atom(), partition_id(), term()) -> term() | undefined.
get_meta(Name, Partition, Default) ->
    MetaTableName = get_name(Name, ?META_TABLE_NAME),
    case ets:info(MetaTableName) of
    undefined ->
        undefined;
    _ ->
        case ets:lookup(MetaTableName, Partition) of
            [] ->
                Default;
            [{Partition, Other}] ->
                Other
        end
    end.

-spec remove_partition(atom(), partition_id()) -> ok | undefined.
remove_partition(Name, Partition) ->
    case ets:info(get_name(Name, ?META_TABLE_NAME)) of
        undefined ->
            undefined;
        _ ->
            true = ets:delete(get_name(Name, ?META_TABLE_NAME), Partition),
            ok
    end.

-spec get_merged_data(atom(), term()) -> term() | undefined.
get_merged_data(Name, Default) ->
    case ets:info(get_name(Name, ?META_TABLE_STABLE_NAME)) of
        undefined ->
            undefined;
        _ ->
            case ets:lookup(get_name(Name, ?META_TABLE_STABLE_NAME), merged_data) of
                [] ->
                    Default;
                [{merged_data, Other}] ->
                    Other
            end
    end.

%% ===================================================================
%% gen_statem callbacks
%% ===================================================================

init([Name, UpdateFun, MergeFun, LookupFun, StoreFun, FoldFun, Default, InitialLocal, InitialMerged]) ->
    MetaTable = ets:new(get_name(Name, ?META_TABLE_STABLE_NAME), [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    _StableTable = ets:new(get_name(Name, ?META_TABLE_NAME), [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    {ok, send_meta_data, #state{table = MetaTable,
                                last_result = InitialLocal,
                                update_function = UpdateFun,
                                merge_function = MergeFun,
                                lookup_function = LookupFun,
                                store_function = StoreFun,
                                fold_function = FoldFun,
                                default = Default,
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
                                       update_function = UpdateFun,
                                       merge_function = MergeFun,
                                       lookup_function = LookupFun,
                                       store_function = StoreFun,
                                       fold_function = FoldFun,
                                       name = Name,
                                       should_check_nodes = CheckNodes}) ->
    {WillChange, Data} = get_merged_meta_data(Name, MergeFun, StoreFun, CheckNodes),
    NodeList = ?GET_NODE_LIST(),
    LocalMerged = LookupFun(local_merged, Data),
    MyNode = node(),
    ok = lists:foreach(fun(Node) ->
                           ok = meta_data_manager:send_meta_data(Name, Node, MyNode, LocalMerged)
                       end, NodeList),

    MergedDict = MergeFun(Data),
    {HasChanged, NewResult} = update_stable(LastResult, MergedDict, UpdateFun, LookupFun, StoreFun, FoldFun),
    Store = case HasChanged of
                true ->
                    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, NewResult}),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec tables_ready(atom()) -> boolean().
tables_ready(Name) ->
    case ets:info(get_name(Name, ?REMOTE_META_TABLE_NAME)) of
        undefined ->
            false;
        _ ->
            true
    end.

-spec check_nodes(atom(), maps:maps(), maps:maps(), [atom()], [any()]) -> {any(), any()}.
check_nodes(Name, RemoteDict, LocalDict, NodeList, PartitionList) ->
    {NewDict, NodeErase} = lists:foldl(fun(NodeId, {Acc, Acc2}) ->
                            AccNew = case maps:find(NodeId, RemoteDict) of
                                        {ok, Val} ->
                                            maps:put(NodeId, Val, Acc);
                                        error     ->
                                            %% Put a record in the ets table because there is none for this node
                                            meta_data_manager:add_new_meta_data(Name, NodeId),
                                            maps:put(NodeId, undefined, Acc)
                                     end,
                            Acc2New = maps:remove(NodeId, Acc2),
                            {AccNew, Acc2New}
                            end, {maps:new(), RemoteDict}, NodeList),
    %% Should remove nodes (and partitions) that no longer exist in this ring/phys node
    maps:fold(fun(NodeId, _Val, _Acc) -> ok = meta_data_manager:remove_node(Name, NodeId) end, ok, NodeErase),
    %% Be sure that you are only checking local partitions
    {NewLocalDict, PartitionErase} = lists:foldl(fun(PartitionId, {Acc, Acc2}) ->
                                        AccNew = case maps:find(PartitionId, LocalDict) of
                                                    {ok, Val} ->
                                                        maps:put(PartitionId, Val, Acc);
                                                    error ->
                                                         %% Put a record in the ets table because there is none for this partition
                                                         ets:insert_new(get_name(Name, ?META_TABLE_NAME), {PartitionId, maps:new()}),
                                                         maps:put(PartitionId, undefined, Acc)
                                                 end,
                                        Acc2New = maps:remove(PartitionId, Acc2),
                                        {AccNew, Acc2New}
                                    end, {maps:new(), LocalDict}, PartitionList),
    %% Should remove nodes (and partitions) that no longer exist in this ring/phys node
    maps:fold(fun(PartitionId, _Val, _Acc) -> ok = remove_partition(Name, PartitionId) end, ok, PartitionErase),
    {NewDict, NewLocalDict}.

-spec get_merged_meta_data(atom(), fun((term()) -> term()), fun(), boolean()) -> {boolean(), term()} | not_ready.
get_merged_meta_data(Name, MergeFun, StoreFun, CheckNodes) ->
    case tables_ready(Name) of
        false ->
            not_ready;
        true ->
            {NodeList, PartitionList, WillChange} = ?GET_NODE_AND_PARTITION_LIST(),
            Remote = maps:from_list(ets:tab2list(get_name(Name, ?REMOTE_META_TABLE_NAME))),
            Local = maps:from_list(ets:tab2list(get_name(Name, ?META_TABLE_NAME))),
            %% Be sure that you are only checking active nodes
            %% This isn't the most efficient way to do this because are checking the list
            %% of nodes and partitions every time to see if any have been removed/added
            %% This is only done if the ring is expected to change, but should be done
            %% differently (check comment in get_node_and_partition_list())
            {NewRemote, NewLocal} = case CheckNodes of
                    true ->
                        check_nodes(Name, Remote, Local, NodeList, PartitionList);
                    false ->
                        {Remote, Local}
                    end,
            LocalMerged = MergeFun(NewLocal),
            {WillChange, StoreFun(local_merged, LocalMerged, NewRemote)}
    end.

-spec update_stable(term(), term(), fun((term(), term()) -> boolean()), fun(), fun(), fun()) -> {boolean(), term()}.
update_stable(LastResult, NewData, UpdateFun, LookupFun, StoreFun, FoldFun) ->
    FoldFun(fun(DcId, Time, {Bool, Acc}) ->
                  Last = LookupFun(DcId, LastResult),
                  case UpdateFun(Last, Time) of
                      true ->
                          {true, StoreFun(DcId, Time, Acc)};
                      false ->
                          {Bool, Acc}
                  end
              end, {false, LastResult}, NewData).

-spec get_node_list() -> [node()].
get_node_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    lists:delete(MyNode, riak_core_ring:ready_members(Ring)).

-spec get_node_and_partition_list() -> {[node()], [partition_id()], true}.
get_node_and_partition_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NodeList = get_node_list(),
    PartitionList = riak_core_ring:my_indices(Ring),
    %% Deciding if the nodes might change by checking the is_resizing function is not
    %% safe can cause inconsistencies under concurrency, so this should
    %% be done differently
    %% Resize = riak_core_ring:is_resizing(Ring) or riak_core_ring:is_post_resize(Ring) or riak_core_ring:is_resize_complete(Ring),
    Resize = true,
    {NodeList, PartitionList, Resize}.

get_name(Name, TableName) ->
    list_to_atom(atom_to_list(Name) ++ atom_to_list(TableName) ++ atom_to_list(node())).

-ifdef(TEST).

meta_data_sender_test_() ->
    {setup,
     fun start/0,
     fun stop/1,
     {with, [
        fun empty_test/1,
        fun merge_test/1,
        fun merge_additional_test/1,
        fun missing_test/1,
        fun merge_node_change_test/1,
        fun merge_node_change_additional_test/1,
        fun merge_node_delete_test/1
    ]}
    }.

start() ->
    [Name, _UpdateFunc, _MergeFunc, _LookupFunc, _StoreFunc, _FoldFunc, _Default, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    _Table  = ets:new(get_name(Name, ?META_TABLE_STABLE_NAME), [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    _Table2 = ets:new(get_name(Name, ?META_TABLE_NAME), [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    _Table3 = ets:new(node_table, [set, named_table, public]),
    _Table4 = ets:new(get_name(Name, ?REMOTE_META_TABLE_NAME), [set, named_table, protected, ?META_TABLE_CONCURRENCY]),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    ok.

stop(_) -> ok. %TODO Delete ets tables !!

-spec set_nodes_and_partitions_and_willchange([node()], [partition_id()], boolean()) -> ok.
set_nodes_and_partitions_and_willchange(Nodes, Partitions, WillChange) ->
    true = ets:insert(node_table, {nodes, Nodes}),
    true = ets:insert(node_table, {partitions, Partitions}),
    true = ets:insert(node_table, {willchange, WillChange}),
    ok.

%% Basic empty test
empty_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1], [p1, p2, p3], false),

    put_meta(Name, p1, vectorclock:new()),
    put_meta(Name, p2, vectorclock:new()),
    put_meta(Name, p3, vectorclock:new()),

    {false, Dict} = get_merged_meta_data(Name, MergeFunc, StoreFunc, false),
    LocalMerged = vectorclock:get(local_merged, Dict),
    ?assertEqual(vectorclock:to_list(LocalMerged), []).


%% This test checks to make sure that merging is done correctly for multiple partitions
merge_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], false),

    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(Name, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    {false, Dict} = get_merged_meta_data(Name, MergeFunc, StoreFunc, false),
    LocalMerged = vectorclock:get(local_merged, Dict),
    ?assertEqual(LocalMerged, vectorclock:from_list([{dc1, 5}, {dc2, 5}])).

merge_additional_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1, n2], [p1, p2, p3], false),
    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(Name, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    put_meta(Name, p3, vectorclock:from_list([{dc1, 20}, {dc2, 20}])),
    {false, Dict} = get_merged_meta_data(Name, MergeFunc, StoreFunc, false),
    LocalMerged = vectorclock:get(local_merged, Dict),
    ?assertEqual(LocalMerged, vectorclock:from_list([{dc1, 5}, {dc2, 5}])).

%% Be sure that when you are missing a partition in your meta_data that you get a 0 value
missing_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1], [p1, p2, p3], false),

    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}])),
    put_meta(Name, p3, vectorclock:from_list([{dc1, 10}])),
    remove_partition(Name, p2), %TODO clean up test fixture

    {false, Dict} = get_merged_meta_data(Name, MergeFunc, StoreFunc, true),
    LocalMerged = vectorclock:get(local_merged, Dict),
    ?assertEqual(LocalMerged, vectorclock:from_list([{dc1, 0}])).

%% This test checks to make sure that merging is done correctly for multiple partitions
%% when you have a node that is removed from the cluster
%% It uses the functions in stable_time_functions.erl
merge_node_change_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], true),

    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(Name, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),
    remove_partition(Name, p3),

    {true, Dict1} = get_merged_meta_data(Name, MergeFunc, StoreFunc, false),
    LocalMerged1 = vectorclock:get(local_merged, Dict1),
    ?assertEqual(LocalMerged1, vectorclock:from_list([{dc1, 5}, {dc2, 5}])).

merge_node_change_additional_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1, n2], [p1, p3], true),

    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}, {dc2, 10}])),
    put_meta(Name, p2, vectorclock:from_list([{dc1, 5}, {dc2, 5}])),
    put_meta(Name, p3, vectorclock:from_list([{dc1, 20}, {dc2, 20}])),
    {true, Dict2} = get_merged_meta_data(Name, MergeFunc, StoreFunc, true),
    LocalMerged2 = vectorclock:get(local_merged, Dict2),
    ?assertEqual(LocalMerged2, vectorclock:from_list([{dc1, 10}, {dc2, 10}])).

merge_node_delete_test(_) ->
    [Name, _UpdateFunc, MergeFunc, _LookupFunc, StoreFunc, _FoldFunc, _Default, _InitialLocal, _InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    set_nodes_and_partitions_and_willchange([n1], [p1, p2], true),

    put_meta(Name, p3, vectorclock:from_list([{dc1, 0}, {dc2, 0}])),
    put_meta(Name, p1, vectorclock:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta(Name, p2, vectorclock:from_list([{dc1, 5}, {dc2, 10}])),

    {true, Vc} = get_merged_meta_data(Name, MergeFunc, StoreFunc, false),
    LocalMerged1 = maps:get(local_merged, Vc),
    ?assertEqual(LocalMerged1, vectorclock:from_list([{dc1, 0}, {dc2, 0}])),

    {true, Dict2} = get_merged_meta_data(Name, MergeFunc, StoreFunc, true),
    LocalMerged2 = vectorclock:get(local_merged, Dict2),
    ?assertEqual(LocalMerged2, vectorclock:from_list([{dc1, 5}, {dc2, 5}])).


get_node_list_t() ->
    [{nodes, Nodes}] = ets:lookup(node_table, nodes),
    Nodes.

get_node_and_partition_list_t() ->
    [{willchange, WillChange}] = ets:lookup(node_table, willchange),
    [{nodes, Nodes}] = ets:lookup(node_table, nodes),
    [{partitions, Partitions}] = ets:lookup(node_table, partitions),
    {Nodes, Partitions, WillChange}.

-endif.
