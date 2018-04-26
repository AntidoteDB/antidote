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



-export([start_link/5,
    start/1,
    put_meta_dict/3,
    put_meta_dict/4,
    put_meta_data/4,
    put_meta_data/5,
    get_meta_dict/2,
    get_node_list/0,
    get_node_and_partition_list/0,
    get_merged_data/1,
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
      table2 :: ets:tid(),
      last_result :: dict:dict(),
      name :: atom(),
      update_function :: fun((term(), term())->boolean()),
      merge_function :: fun((dict:dict())->dict:dict()),
      should_check_nodes :: boolean()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% This fsm is responsible for sending meta-data that has been collected on this
%% physical node and sending it to all other physical nodes in the riak ring.
%% There will be one instance of this fsm running on each physical machine.
%% During execution of the system, vnodes may be continually writing to the dict.
%%
%% At a period, defined in antidote.hrl, it will trigger itself to send the meta-data.
%% This will cause the meta-data to be broadcast to all other physical nodes in
%% the cluster.  Before sending the meta-data it calls the merge function on the
%% meta-data stored by each vnode located at this partition.
%%
%% Each partition can store meta-data by calling one of the put functions. The
%% meta-data will be stored as a dict for each vnode ID.
%%
%% The merge function should take as input a Dict, where each entry is a PartitionId
%% with the vaue being the dict input through the put functions. The output should
%% be a dict, with the same structure as the dict added by the put functions.
%% The idea behind this is that first the vnodes data for each phyiscal node is merged
%% then is broadcast, then the phyisical nodes meta-data is merged.  This way
%% network traffic is lowered.
%%
%% Once the data is fully merged it does not immediately replace the old merged
%% data.  Instead the UpdateFunction is called on each entry of the new and old
%% versions. It should return true if the new value should be kept, false otherwise.
%% The completely merged data can then be read using the get_merged_data function.
%%
%% InitialLocal and InitialMerged are used to prepopulate the meta-data for the vnode
%% and for the merged data.
%%
%% Note that there is 1 of these fsms per physical node. Meta-data is only
%% stored in memory.  Do not use this if you want to persist data safely, it
%% was designed with light heart-beat type meta-data in mind.


-spec start_link(atom(), fun((term(), term())->boolean()), fun((dict:dict())->dict:dict()), dict:dict(), dict:dict()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Name, UpdateFunction, MergeFunction, InitialLocal, InitialMerged) ->
    gen_statem:start_link({local, list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE))},
               ?MODULE, [Name, UpdateFunction, MergeFunction, InitialLocal, InitialMerged], []).

-spec start(atom()) -> ok.
start(Name) ->
    gen_statem:call(list_to_atom(atom_to_list(Name) ++ atom_to_list(?MODULE)), start).

-spec put_meta_dict(atom(), partition_id(), dict:dict()) -> ok.
put_meta_dict(Name, Partition, Dict) ->
    put_meta_dict(Name, Partition, Dict, undefined).

-spec put_meta_dict(atom(), partition_id(), dict:dict(), fun((dict:dict(), dict:dict())->dict:dict()) | undefined) -> ok.
put_meta_dict(Name, Partition, Dict, Func) ->
    case ets:info(get_name(Name, ?META_TABLE_NAME)) of
        undefined ->
            ok;
        _ ->
            Result = case Func of
                         undefined ->
                             Dict;
                         _ ->
                             Func(Dict, get_meta_dict(Name, Partition))
                     end,
            true = ets:insert(get_name(Name, ?META_TABLE_NAME), {Partition, Result}),
            ok
    end.


-spec put_meta_data(atom(), partition_id(), term(), term()) -> ok.
put_meta_data(Name, Partition, Key, Value) ->
    put_meta_data(Name, Partition, Key, Value, fun(_Prev, Val) -> Val end).

-spec put_meta_data(atom(), partition_id(), term(), term(), fun((term(), term()) -> term())) -> ok.
put_meta_data(Name, Partition, Key, Value, Func) ->
    case ets:info(get_name(Name, ?META_TABLE_NAME)) of
        undefined ->
            ok;
        _ ->
            Dict = case ets:lookup(get_name(Name, ?META_TABLE_NAME), Partition) of
                       [] ->
                           dict:new();
                       [{Partition, Other}] ->
                           Other
                   end,
            NewDict = case dict:find(Key, Dict) of
                          error ->
                              dict:store(Key, Value, Dict);
                          {ok, Prev} ->
                              dict:store(Key, Func(Prev, Value), Dict)
                      end,
            put_meta_dict(Name, Partition, NewDict, undefined)
    end.

-spec get_meta_dict(atom(), partition_id()) -> dict:dict() | undefined.
get_meta_dict(Name, Partition) ->
    case ets:info(get_name(Name, ?META_TABLE_NAME)) of
    undefined ->
        dict:new();
    _ ->
        case ets:lookup(get_name(Name, ?META_TABLE_NAME), Partition) of
            [] ->
                dict:new();
            [{Partition, Other}] ->
                Other
        end
    end.

-spec remove_partition(atom(), partition_id()) -> ok | false.
remove_partition(Name, Partition) ->
    case ets:info(get_name(Name, ?META_TABLE_NAME)) of
        undefined ->
            false;
        _ ->
            true = ets:delete(get_name(Name, ?META_TABLE_NAME), Partition),
            ok
    end.

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec get_merged_data(atom()) -> dict:dict() | undefined.
get_merged_data(Name) ->
    case ets:info(get_name(Name, ?META_TABLE_STABLE_NAME)) of
        undefined ->
            dict:new();
        _ ->
            case ets:lookup(get_name(Name, ?META_TABLE_STABLE_NAME), merged_data) of
                [] ->
                    dict:new();
                [{merged_data, Other}] ->
                    Other
            end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init([Name, UpdateFunction, MergeFunction, InitialLocal, InitialMerged]) ->
    Table = ets:new(get_name(Name, ?META_TABLE_STABLE_NAME), [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    Table2 = ets:new(get_name(Name, ?META_TABLE_NAME), [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    {ok, send_meta_data, #state{table = Table,
                                table2 = Table2,
                                last_result = InitialLocal,
                                update_function = UpdateFunction,
                                merge_function = MergeFunction,
                                name = Name,
                                should_check_nodes=true}}.

send_meta_data({call, Sender}, start, State) ->
    {next_state, send_meta_data, State#state{should_check_nodes=true},
        [ {reply, Sender, ok}, {state_timeout, ?META_DATA_SLEEP, timeout} ]
    };

%% internal timeout transition
send_meta_data(state_timeout, timeout, State) ->
    send_meta_data(cast, timeout, State);
send_meta_data(cast, timeout, State = #state{last_result = LastResult,
                                       update_function = UpdateFunction,
                                       merge_function = MergeFunction,
                                       name = Name,
                                       should_check_nodes = CheckNodes}) ->
    {WillChange, Dict} = get_meta_data(Name, MergeFunction, CheckNodes),
    NodeList = ?GET_NODE_LIST(),
    LocalMerged = dict:fetch(local_merged, Dict),
    MyNode = node(),
    ok = lists:foreach(fun(Node) ->
                           ok = meta_data_manager:send_meta_data(Name, Node, MyNode, LocalMerged)
                       end, NodeList),
    MergedDict = MergeFunction(Dict),
    {NewBool, NewResult} = update_stable(LastResult, MergedDict, UpdateFunction),
    Store = case NewBool of
                true ->
                    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, NewResult}),
                    NewResult;
                false ->
                    LastResult
            end,
    {next_state, send_meta_data, State#state{last_result = Store, should_check_nodes=WillChange},
        [ {state_timeout, ?META_DATA_SLEEP, timeout} ]
    }.

callback_mode() -> state_functions.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-spec get_meta_data(atom(), fun((dict:dict()) -> dict:dict()), boolean()) -> {true, dict:dict()} | false.
get_meta_data(Name, MergeFunc, CheckNodes) ->
    TablesReady = case ets:info(get_name(Name, ?REMOTE_META_TABLE_NAME)) of
                      undefined ->
                          false;
                      _ ->
                          case ets:info(get_name(Name, ?META_TABLE_NAME)) of
                              undefined ->
                                false;
                              _ ->
                                true
                          end
                  end,
    case TablesReady of
        false ->
            false;
        true ->
            {NodeList, PartitionList, WillChange} = ?GET_NODE_AND_PARTITION_LIST(),
            RemoteDict = dict:from_list(ets:tab2list(get_name(Name, ?REMOTE_META_TABLE_NAME))),
            LocalDict = dict:from_list(ets:tab2list(get_name(Name, ?META_TABLE_NAME))),
            %% Be sure that you are only checking active nodes
            %% This isnt the most efficent way to do this because are checking the list
            %% of nodes and partitions every time to see if any have been removed/added
            %% This is only done if the ring is expected to change, but should be done
            %% differently (check comment in get_node_and_partition_list())
            {NewRemote, NewLocal} =
            case CheckNodes of
                true ->
                    {NewDict, NodeErase} =
                        lists:foldl(fun(NodeId, {Acc, Acc2}) ->
                                        AccNew = case dict:find(NodeId, RemoteDict) of
                                                     {ok, Val} ->
                                                         dict:store(NodeId, Val, Acc);
                                                     error ->
                                                         %% Put a record in the ets table because there is none for this node
                                                         meta_data_manager:add_new_meta_data(Name, NodeId),
                                                         dict:store(NodeId, undefined, Acc)
                                                 end,
                                        Acc2New = dict:erase(NodeId, Acc2),
                                        {AccNew, Acc2New}
                                    end, {dict:new(), RemoteDict}, NodeList),
                    %% Should remove nodes (and partitions) that no longer exist in this ring/phys node
                    dict:fold(fun(NodeId, _Val, _Acc) ->
                                  ok = meta_data_manager:remove_node(Name, NodeId)
                              end, ok, NodeErase),

                    %% Be sure that you are only checking local partitions
                    {NewLocalDict, PartitionErase} =
                        lists:foldl(fun(PartitionId, {Acc, Acc2}) ->
                                        AccNew = case dict:find(PartitionId, LocalDict) of
                                                    {ok, Val} ->
                                                        dict:store(PartitionId, Val, Acc);
                                                    error ->
                                                         %% Put a record in the ets table because there is none for this partition
                                                         ets:insert_new(get_name(Name, ?META_TABLE_NAME), {PartitionId, dict:new()}),
                                                         dict:store(PartitionId, undefined, Acc)
                                                 end,
                                        Acc2New = dict:erase(PartitionId, Acc2),
                                        {AccNew, Acc2New}
                                    end, {dict:new(), LocalDict}, PartitionList),
                    %% Should remove nodes (and partitions) that no longer exist in this ring/phys node
                    dict:fold(fun(PartitionId, _Val, _Acc) ->
                                  ok = remove_partition(Name, PartitionId)
                              end, ok, PartitionErase),

                    {NewDict, NewLocalDict};
                false ->
                    {RemoteDict, LocalDict}
            end,
            LocalMerged = MergeFunc(NewLocal),
            {WillChange, dict:store(local_merged, LocalMerged, NewRemote)}
    end.

-spec update_stable(dict:dict(), dict:dict(), fun((term(), term()) -> boolean())) -> {boolean(), dict:dict()}.
update_stable(LastResult, NewDict, UpdateFunc) ->
    dict:fold(fun(DcId, Time, {Bool, Acc}) ->
                  Last = case dict:find(DcId, LastResult) of
                             {ok, Val} ->
                                 Val;
                             error ->
                                 undefined
                         end,
                  case UpdateFunc(Last, Time) of
                      true ->
                          {true, dict:store(DcId, Time, Acc)};
                      false ->
                          {Bool, Acc}
                  end
              end, {false, LastResult}, NewDict).

-spec get_node_list() -> [node()].
get_node_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    lists:delete(MyNode, riak_core_ring:ready_members(Ring)).

-spec get_node_and_partition_list() -> {[node()], [partition_id()], true}.
get_node_and_partition_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    NodeList = lists:delete(MyNode, riak_core_ring:ready_members(Ring)),
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

%% This test checks to make sure that merging is done correctly for multiple partitions
%% It uses the functions in stable_time_functions.erl
merge_test() ->
    [Name, _UpdateFunc, MergeFunc, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    _Table = ets:new(get_name(Name, ?META_TABLE_STABLE_NAME), [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    _Table2 = ets:new(get_name(Name, ?META_TABLE_NAME), [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    _Table3 = ets:new(node_table, [set, named_table]),
    _Table4 = ets:new(get_name(Name, ?REMOTE_META_TABLE_NAME), [set, named_table, protected, ?META_TABLE_CONCURRENCY]),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    true = ets:insert(node_table, {nodes, [n1]}),
    true = ets:insert(node_table, {testnum, test1}),
    true = ets:insert(node_table, {partitions, [p1, p2]}),
    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta_dict(Name, p2, dict:from_list([{dc1, 5}, {dc2, 10}])),
    {false, Dict1} = get_meta_data(Name, MergeFunc, false),
    LocalMerged1 = dict:fetch(local_merged, Dict1),
    ?assertEqual(LocalMerged1, dict:from_list([{dc1, 5}, {dc2, 5}])),

    true = ets:insert(node_table, {nodes, [n1, n2]}),
    true = ets:insert(node_table, {partitions, [p1, p2, p3]}),
    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta_dict(Name, p2, dict:from_list([{dc1, 5}, {dc2, 10}])),
    put_meta_dict(Name, p3, dict:from_list([{dc1, 20}, {dc2, 20}])),
    {false, Dict2} = get_meta_data(Name, MergeFunc, false),
    LocalMerged2 = dict:fetch(local_merged, Dict2),
    ?assertEqual(LocalMerged2, dict:from_list([{dc1, 5}, {dc2, 5}])),
    ok.

%% Basic empty test
empty_test() ->
    [Name, _UpdateFunc, MergeFunc, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    true = ets:insert(node_table, {nodes, [n1]}),
    true = ets:insert(node_table, {testnum, test1}),
    true = ets:insert(node_table, {partitions, [p1, p2, p3]}),

    put_meta_dict(Name, p1, dict:from_list([])),
    put_meta_dict(Name, p2, dict:from_list([])),
    put_meta_dict(Name, p3, dict:from_list([])),

    {false, Dict1} = get_meta_data(Name, MergeFunc, false),
    LocalMerged1 = dict:fetch(local_merged, Dict1),
    ?assertEqual(LocalMerged1, dict:from_list([])).

%% Be sure that when you are missing a partition in your meta_data that you get a 0 value
missing_test() ->
    [Name, _UpdateFunc, MergeFunc, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    true = ets:insert(node_table, {nodes, [n1]}),
    true = ets:insert(node_table, {testnum, test1}),
    true = ets:insert(node_table, {partitions, [p1, p2, p3]}),
    true = ets:delete(get_name(Name, ?META_TABLE_NAME), p2),

    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}])),
    put_meta_dict(Name, p3, dict:from_list([{dc1, 10}])),

    {false, Dict1} = get_meta_data(Name, MergeFunc, true),
    LocalMerged1 = dict:fetch(local_merged, Dict1),
    io:format("val ~w~n", [dict:to_list(LocalMerged1)]),
    ?assertEqual(LocalMerged1, dict:from_list([{dc1, 0}])).

%% This test checks to make sure that merging is done correctly for multiple partitions
%% when you have a node that is removed from the cluster
%% It uses the functions in stable_time_functions.erl
merge_node_change_test() ->
    [Name, _UpdateFunc, MergeFunc, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    true = ets:delete(get_name(Name, ?META_TABLE_NAME), p3),
    true = ets:insert(node_table, {nodes, [n1]}),
    true = ets:insert(node_table, {testnum, test2}),
    true = ets:insert(node_table, {partitions, [p1, p2]}),

    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta_dict(Name, p2, dict:from_list([{dc1, 5}, {dc2, 10}])),
    {true, Dict1} = get_meta_data(Name, MergeFunc, false),
    LocalMerged1 = dict:fetch(local_merged, Dict1),
    io:format("~w", [dict:to_list(LocalMerged1)]),
    ?assertEqual(LocalMerged1, dict:from_list([{dc1, 5}, {dc2, 5}])),

    true = ets:insert(node_table, {nodes, [n1, n2]}),
    true = ets:insert(node_table, {partitions, [p1, p3]}),
    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}, {dc2, 10}])),
    put_meta_dict(Name, p2, dict:from_list([{dc1, 5}, {dc2, 5}])),
    put_meta_dict(Name, p3, dict:from_list([{dc1, 20}, {dc2, 20}])),
    {true, Dict2} = get_meta_data(Name, MergeFunc, true),
    LocalMerged2 = dict:fetch(local_merged, Dict2),
    ?assertEqual(LocalMerged2, dict:from_list([{dc1, 10}, {dc2, 10}])),
    ok.

merge_node_delete_test() ->
    [Name, _UpdateFunc, MergeFunc, _InitialLocal, InitialMerged] = stable_time_functions:export_funcs_and_vals(),
    true = ets:insert(get_name(Name, ?META_TABLE_STABLE_NAME), {merged_data, InitialMerged}),
    true = ets:insert(node_table, {nodes, [n1]}),
    true = ets:insert(node_table, {testnum, test2}),
    true = ets:insert(node_table, {partitions, [p1, p2]}),

    put_meta_dict(Name, p3, dict:from_list([{dc1, 0}, {dc2, 0}])),
    put_meta_dict(Name, p1, dict:from_list([{dc1, 10}, {dc2, 5}])),
    put_meta_dict(Name, p2, dict:from_list([{dc1, 5}, {dc2, 10}])),

    {true, Dict1} = get_meta_data(Name, MergeFunc, false),
    LocalMerged1 = dict:fetch(local_merged, Dict1),
    io:format("~w", [dict:to_list(LocalMerged1)]),
    ?assertEqual(LocalMerged1, dict:from_list([{dc1, 0}, {dc2, 0}])),

    {true, Dict2} = get_meta_data(Name, MergeFunc, true),
    LocalMerged2 = dict:fetch(local_merged, Dict2),
    io:format("~w", [dict:to_list(LocalMerged2)]),
    ?assertEqual(LocalMerged2, dict:from_list([{dc1, 5}, {dc2, 5}])).


get_node_list_t() ->
    [{nodes, Nodes}] = ets:lookup(node_table, nodes),
    Nodes.

get_node_and_partition_list_t() ->
    [{testnum, TestNum}] = ets:lookup(node_table, testnum),
    case TestNum of
        test1 ->
            [{nodes, Nodes}] = ets:lookup(node_table, nodes),
            [{partitions, Partitions}] = ets:lookup(node_table, partitions),
            {Nodes, Partitions, false};
        test2 ->
            [{nodes, Nodes}] = ets:lookup(node_table, nodes),
            [{partitions, Partitions}] = ets:lookup(node_table, partitions),
            {Nodes, Partitions, true}
    end.


-endif.
