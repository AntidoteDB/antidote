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
-behaviour(gen_fsm).

-include("antidote.hrl").

-export([start_link/4,
	 put_meta_dict/2,
	 put_meta_dict/3,
	 put_meta_data/3,
	 put_meta_data/4,
	 get_meta_dict/1,
	 get_merged_data/0,
         remove_partition/1,
	 send_meta_data/2]).

%% Callbacks
-export([init/1,
	 code_change/4,
	 handle_event/3,
	 handle_info/3,
         handle_sync_event/4,
	 terminate/3]).


-record(state, {
	  table,
	  table2,
	  last_result,
	  update_function,
	  merge_function,
	  should_check_nodes}).

%% ===================================================================
%% Public API
%% ===================================================================


%% This fsm is responsible for sending meta-data that has been collected on this
%% phyical node and sending it to all other physical nodes in the riak ring.
%% There will be one instance of this fsm running on each physical machine.
%% During execution of the system, v-nodes may be continually writing to the
%% 
%% At a period given in antidote.hrl it will trigger itself to send the meta-data.
%% 

%% -spec start_link(fun(() -> [fun((term(),term())->boolean()), fun((dict())->dict()), dict(), dict()]) -> {ok,pid()} | ignore | {error,term()}.
start_link(UpdateFunction, MergeFunction, InitialLocal, InitialMerged) ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [UpdateFunction, MergeFunction, InitialLocal, InitialMerged], []).
    %%gen_fsm:start_link({local, ?MODULE}, ?MODULE, ExportFun(), []).

-spec put_meta_dict(partition_id(), dict()) -> ok.
put_meta_dict(Partition,Dict) ->
    put_meta_dict(Partition, Dict, undefined).

%% -spec put_meta_dict(partition_id(), dict(), fun((dict(),dict())->dict() | undefined) -> ok.
put_meta_dict(Partition, Dict, Func) ->
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    ok;
	_ ->
	    Result = case Func of
			 undefined ->
			     Dict;
			 _ ->
			     Func(Dict, get_meta_dict(Partition))
		     end,
	    true = ets:insert(?META_TABLE_NAME, {Partition, Result}),
	    ok
    end.


-spec put_meta_data(partition_id(), term(), term()) -> ok.
put_meta_data(Partition, Key, Value) ->
    put_meta_data(Partition, Key, Value, fun(_Prev,Val) -> Val end).

-spec put_meta_data(partition_id(), term(), term(), fun((term(),term()) -> term())) -> ok.
put_meta_data(Partition, Key, Value, Func) ->
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    ok;
	_ ->
	    Dict = case ets:lookup(?META_TABLE_NAME, Partition) of
		       [] ->
			   dict:new();
		       [{Partition,Other}] ->
			   Other
		   end,
	    NewDict = case dict:find(Key,Dict) of
			  error ->
			      dict:store(Key, Value, Dict);
			  {ok, Prev} ->
			      dict:store(Key, Func(Prev,Value), Dict)
		      end,
	    put_meta_dict(Partition, NewDict, undefined)
    end.

-spec get_meta_dict(partition_id()) -> dict().
get_meta_dict(Partition) ->
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    dict:new();
	_ ->
	    case ets:lookup(?META_TABLE_NAME, Partition) of
		[] ->
		    dict:new();
		[{Partition,Other}] ->
		    Other
	    end
    end.

-spec remove_partition(partition_id()) -> ok | false.
remove_partition(Partition) ->
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    false;
	_ ->
	    true = ets:delete(?META_TABLE_NAME, Partition),
	    ok
    end.

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec get_merged_data() -> dict().
get_merged_data() ->
    case ets:info(?META_TABLE_STABLE_NAME) of
	undefined ->
	    dict:new();
	_ ->
	    case ets:lookup(?META_TABLE_STABLE_NAME, merged_data) of
		[] ->
		    dict:new();
		[{merged_data,Other}] ->
		    Other
	    end
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



init([UpdateFunction,MergeFunction,InitialLocal,InitialMerged]) ->
    Table = ets:new(?META_TABLE_STABLE_NAME, [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    Table2 = ets:new(?META_TABLE_NAME, [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    true = ets:insert(?META_TABLE_STABLE_NAME, {merged_data, InitialMerged}),
    {ok, send_meta_data, #state{table = Table,
				table2 = Table2,
				last_result = InitialLocal,
				update_function = UpdateFunction,
				merge_function = MergeFunction,
				should_check_nodes=true},
     ?META_DATA_SLEEP}.

send_meta_data(timeout, State = #state{last_result = LastResult,
				       update_function = UpdateFunction,
				       merge_function = MergeFunction,
				       should_check_nodes = CheckNodes}) ->
    {WillChange,Dict} = get_meta_data(MergeFunction, CheckNodes),
    NodeList = get_node_list(),
    LocalMerged = dict:fetch(local_merged,Dict),
    MyNode = node(),
    ok = lists:foreach(fun(Node) ->
			       ok = meta_data_manager:send_meta_data(Node,MyNode,LocalMerged)
		       end, NodeList),
    MergedDict = MergeFunction(Dict),
    {NewBool, NewResult} = update_stable(LastResult,MergedDict,UpdateFunction),
    Store = case NewBool of
		true ->
		    true = ets:insert(?META_TABLE_STABLE_NAME, {merged_data, NewResult}),
		    NewResult;
		false ->
		    LastResult
	    end,
    {next_state, send_meta_data, State#state{last_result = Store,should_check_nodes=WillChange}, ?META_DATA_SLEEP}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




-spec get_meta_data(fun((dict()) -> dict()),boolean()) -> {boolean(),dict()} | false.			 
get_meta_data(MergeFunc, CheckNodes) ->
    TablesReady = case ets:info(?REMOTE_META_TABLE_NAME) of
		      undefined ->
			  false;
		      _ ->
			  case ets:info(?META_TABLE_NAME) of
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
	    {NodeList,PartitionList,WillChange} = get_node_and_partition_list(),
	    RemoteDict = dict:from_list(ets:tab2list(?REMOTE_META_TABLE_NAME)),
	    LocalDict = dict:from_list(ets:tab2list(?META_TABLE_NAME)),

	    %% Be sure that you are only checking active nodes
	    %% This isnt the most efficent way to do this because are checking the list
	    %% of nodes and partitions every time to see if any have been removed/added
	    %% This is only done if the ring is expected to change, but should be done
	    %% differently (check comment in get_node_and_partition_list())
	    {NewRemote,NewLocal} = 
		case CheckNodes of
		    true ->
			{NewDict,NodeErase} = 
			    lists:foldl(fun(NodeId,{Acc,Acc2}) ->
						AccNew = case dict:find(NodeId, RemoteDict) of
							     {ok, Val} ->
								 dict:store(NodeId,Val,Acc);
							     error ->
								 dict:store(NodeId,undefined,Acc)
							 end,
						Acc2New = dict:erase(NodeId,Acc2),
						{AccNew,Acc2New}
					end, {dict:new(),RemoteDict}, NodeList),
			%% Should remove nodes (and partitions) that no longer exist in this ring/phys node
			dict:fold(fun(NodeId,_Val,_Acc) ->
					  ok = meta_data_manager:remove_node(NodeId)
				  end,ok,NodeErase),
			
			%% Be sure that you are only checking local partitions
			{NewLocalDict,PartitionErase} = 
			    lists:foldl(fun(PartitionId,{Acc,Acc2}) ->
						AccNew = case dict:find(PartitionId, LocalDict) of
							     {ok, Val} ->
								 dict:store(PartitionId,Val,Acc);
							     error ->
								 dict:store(PartitionId,undefined,Acc)
							 end,
						Acc2New = dict:erase(PartitionId,Acc2),
						{AccNew,Acc2New}
					end, {dict:new(),LocalDict}, PartitionList),
			%% Should remove nodes (and partitions) that no longer exist in this ring/phys node
			dict:fold(fun(PartitionId,_Val,_Acc) ->
					  ok = remove_partition(PartitionId)
				  end,ok,PartitionErase),
			
			{NewDict,NewLocalDict};
		    false ->
			{RemoteDict,LocalDict}
		end,
	    
	    LocalMerged = MergeFunc(NewLocal),
	    {WillChange,dict:store(local_merged, LocalMerged, NewRemote)}
    end.

-spec update_stable(dict(), dict(), fun((term(),term()) -> boolean())) -> {boolean(),dict()}.
update_stable(LastResult,NewDict,UpdateFunc) ->
    dict:fold(fun(DcId, Time, {Bool,Acc}) ->
		      Last = case dict:find(DcId, LastResult) of
				 {ok, Val} ->
				     Val;
				 error ->
				     undefined
			     end,
		      case UpdateFunc(Last,Time) of
			  true ->
			      {true,dict:store(DcId, Time, Acc)};
			  false ->
			      {Bool,Acc}
		      end
	      end, {false,LastResult}, NewDict).

-spec get_node_list() -> [term()].
get_node_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    lists:delete(MyNode, riak_core_ring:ready_members(Ring)).

-spec get_node_and_partition_list() -> {list(),list(),boolean()}.
get_node_and_partition_list() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    NodeList = lists:delete(MyNode, riak_core_ring:ready_members(Ring)),
    PartitionList = riak_core_ring:my_indices(Ring),
    %% Deciding if the nodes might change by checking the is_resizing function is not
    %% safe becuase can cause inconsistencies during concurrency, so this should
    %% be done differently
    {NodeList,PartitionList,riak_core_ring:is_resizing(Ring)}.
