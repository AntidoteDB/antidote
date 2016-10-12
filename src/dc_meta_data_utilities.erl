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

-module(dc_meta_data_utilities).

-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([
	 get_dc_bucket_subs/0,
	 dc_start_success/0,
	 is_restart/0,
	 load_env_meta_data/0,
	 get_env_meta_data/2,
	 store_env_meta_data/2,
	 store_meta_data_name/1,
	 get_meta_data_name/0,
	 get_dc_partitions_detailed/1,
	 get_dc_partitions_dict/1,
	 get_my_dc_id/0,
	 get_dc_descriptors_w_me/0,
	 get_my_dc_descriptor/0,
	 reset_my_dc_id/0,
	 get_my_partitions_list/0,
	 get_dc_partitions_list/1,
	 set_dc_partitions/2,
	 get_dc_ids/1,
	 get_key/1,
	 key_as_integer/1,
	 store_dc_descriptors/1,
	 get_dc_descriptors/0,
	 load_partition_meta_data/0,
	 get_num_partitions/0,
	 get_partition_at_index/1]).


%% Should be called once a DC has successfully started
%% Once this is set, when the nodes in the DC are restarted
%% they will load their config from disk
-spec dc_start_success() -> ok.
dc_start_success() ->
    stable_meta_data_server:broadcast_meta_data(has_started,true).

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
-spec get_env_meta_data(atom(),term()) -> atom().
get_env_meta_data(Name, Default) ->
    case stable_meta_data_server:read_meta_data({env, Name}) of
	{ok, Value} -> Value;
	error ->
	    Val = application:get_env(antidote,Name,Default),
	    ok = stable_meta_data_server:broadcast_meta_data_env({env,Name}, Val),
	    Val
    end.

%% Load all envoriment variables from disk
%% Should be run on node restart
-spec load_env_meta_data() -> ok.
load_env_meta_data() ->
    lists:foreach(fun({Key,Val}) ->
			  case Key of
			      {env, Name} ->
				  application:set_env(antidote,Name,Val);
			      _ -> ok
			  end
		  end, stable_meta_data_server:read_all_meta_data()).

%% Store an enviroment variable on disk
-spec store_env_meta_data(atom(),term()) -> ok.
store_env_meta_data(Name, Value) ->
    stable_meta_data_server:broadcast_meta_data_env({env, Name}, Value).

-spec get_meta_data_name() -> {ok, atom()} | error.
get_meta_data_name() ->
    stable_meta_data_server:read_meta_data(meta_data_name).

%% Returns a tuple of three elements
%% The first is a dict with all partitions for DCID, with key and value being the partition id
%% The second is a tuple with all partitions for DCID
%% The third is an integer telling the number of partitions
-spec get_dc_partitions_detailed(dcid()) -> {dict:dict(),tuple(),non_neg_integer()}.
get_dc_partitions_detailed(DCID) ->
    case stable_meta_data_server:read_meta_data({partition_meta_data,DCID}) of
	{ok, Info} ->
	    Info;
	error ->
	    lager:error("Error no partitions for dc ~w", [DCID]),
	    {dict:new(), {}, 0}
    end.

%% Returns a dict with all partitions for DCID, with key and value being the partition id
-spec get_dc_partitions_dict(dcid()) -> dict:dict().
get_dc_partitions_dict(DCID) ->
    case stable_meta_data_server:read_meta_data({partition_dict,DCID}) of
	{ok, Dict} ->
	    Dict;
	error ->
	    lager:error("Error no partitions for dc ~w", [DCID]),
	    dict:new()
    end.

%% Returns a list of partitions at this DC
-spec get_my_partitions_list() -> [partition_id()].
get_my_partitions_list() ->
    case stable_meta_data_server:read_meta_data(my_partition_list) of
	{ok, List} ->
	    List;
	error ->
	    ok = load_partition_meta_data(),
	    get_my_partitions_list()
    end.

%% Returns a dict with all partitions for DCID, with key and value being the partition id
-spec get_dc_partitions_list(dcid()) -> [partition_id()].
get_dc_partitions_list(DCID) ->
    case stable_meta_data_server:read_meta_data({partition_list,DCID}) of
	{ok, List} ->
	    List;
	error ->
	    case get_my_dc_id() of
		DCID ->
		    load_partition_meta_data(),
		    get_my_partitions_list();
		_ ->
		    lager:error("Error no partitions for dc ~w", [DCID]),
		    []
	    end
    end.

%% Returns the id of the local dc
-spec get_my_dc_id() -> dcid().
get_my_dc_id() ->
    case stable_meta_data_server:read_meta_data(my_dc) of
	{ok, DcId} ->
	    DcId;
	error ->
	    %% Add my DC to the list of DCs since none have been added yet
	    reset_my_dc_id()
    end.

% Sets the id of the local dc
-spec reset_my_dc_id() -> dcid().
reset_my_dc_id() ->
	    MyDC = dc_utilities:get_my_dc_id(),
	    ok = stable_meta_data_server:broadcast_meta_data(my_dc, MyDC),
	    ok = stable_meta_data_server:broadcast_meta_data_merge(dc_list_w_me, MyDC, fun ordsets:add_element/2, fun ordsets:new/0),
	    MyDC.

%% Returns the descriptor of the local dc
-spec get_my_dc_descriptor() -> #descriptor{}.
get_my_dc_descriptor() ->
    case stable_meta_data_server:read_meta_data(my_dc_descriptor) of
	{ok, Des} ->
	    Des;
	error ->
	    %% Add my DC to the list of DCs since none have been added yet
	    reset_my_dc_descriptor()
    end.

% Sets the descriptor of the local dc
-spec reset_my_dc_descriptor() -> #descriptor{}.
reset_my_dc_descriptor() ->
    {ok,MyDC} = inter_dc_manager:get_descriptor(),
    ok = stable_meta_data_server:broadcast_meta_data(my_dc_descriptor, MyDC),
    ok = stable_meta_data_server:broadcast_meta_data_merge(all_descriptors, [MyDC], fun desc_merge_func/2, fun dict:new/0),
    MyDC.

%% Loads all the partitions ids into an ets table stored by
%% their index
-spec load_partition_meta_data() -> ok.
load_partition_meta_data() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionNodeList = chashbin:to_list(CHBin),
    PartitionList = dc_utilities:get_all_partitions(),
    Length = length(PartitionList),
    MyDCID = get_my_dc_id(),
    ok = set_dc_partitions(PartitionList, MyDCID),
    ok = stable_meta_data_server:broadcast_meta_data(my_partition_list, PartitionList),
    ok = stable_meta_data_server:broadcast_meta_data({part,length}, Length),
    {_Len, IdPartitionList} = lists:foldl(fun(Partition,{PrevId,Acc}) ->
						  {PrevId + 1, Acc ++ [{{part,PrevId},Partition}]}
					  end, {1,[]}, PartitionNodeList),
    ok = stable_meta_data_server:broadcast_meta_data_list(IdPartitionList).

%% Gets the number of partitions at this DC
-spec get_num_partitions() -> non_neg_integer().
get_num_partitions() ->
    case stable_meta_data_server:read_meta_data({part,length}) of
	{ok, Num} ->
	    Num;
	error ->
	    ok = load_partition_meta_data(),
	    get_num_partitions()
    end.

%% Get information about a partition based on it index
-spec get_partition_at_index(non_neg_integer()) -> index_node().
get_partition_at_index(Index) ->
    case stable_meta_data_server:read_meta_data({part,Index}) of
	{ok, Partition} ->
	    Partition;
	error ->
	    ok = load_partition_meta_data(),
	    get_partition_at_index(Index)
    end.
	
%% Store an external dc descriptor
-spec store_dc_descriptors([#descriptor{}]) -> ok.
store_dc_descriptors(Descriptors) ->
    stable_meta_data_server:broadcast_meta_data_merge(external_descriptors, Descriptors, fun desc_merge_func/2, fun dict:new/0),
    stable_meta_data_server:broadcast_meta_data_merge(external_buckets, Descriptors, fun bucket_merge_func/2, fun dict:new/0),
    stable_meta_data_server:broadcast_meta_data_merge(all_descriptors, Descriptors, fun desc_merge_func/2, fun dict:new/0).

%% Internal function for merging the list of dc descriptors to only store their bucket lists
-spec bucket_merge_func([#descriptor{}], dict:dict()) -> dict:dict().
bucket_merge_func(DescList, PrevDict) ->
    lists:foldl(fun(#descriptor{dcid = DCID, bucket_sub_list = BucketList}, Acc) ->
			dict:store(DCID, BucketList, Acc)
		end, PrevDict, DescList).

%% Internal function for merging the list of dc descriptors
-spec desc_merge_func([#descriptor{}], dict:dict()) -> dict:dict().
desc_merge_func(DescList, PrevDict) ->
    lists:foldl(fun(Desc = #descriptor{dcid = DCID}, Acc) ->
			dict:store(DCID, Desc, Acc)
		end, PrevDict, DescList).

-spec get_dc_bucket_subs() -> dict:dict().
get_dc_bucket_subs() ->
    case stable_meta_data_server:read_meta_data(external_buckets) of
	{ok, Dict} -> Dict;
	error -> dict:new()
    end.

%% Gets the list of external dc descriptors
-spec get_dc_descriptors() -> [#descriptor{}].
get_dc_descriptors() ->
    case stable_meta_data_server:read_meta_data(external_descriptors) of
	{ok, Dict} ->
	    dict:fold(fun(_DCID, Desc, Acc) ->
			      [Desc | Acc]
		      end, [], Dict);
	error ->
	    []
    end.

%% Gets a list of all dc descriptors
-spec get_dc_descriptors_w_me() -> [#descriptor{}].
get_dc_descriptors_w_me() ->
    case stable_meta_data_server:read_meta_data(all_descriptors) of
	{ok, Dict} ->
	    dict:fold(fun(_DCID, Desc, Acc) ->
			      [Desc | Acc]
		      end, [], Dict);
	error ->
	    get_my_dc_descriptor()
    end.

%% Add information about a DC to the meta_data
-spec set_dc_partitions([partition_id()],dcid()) -> ok.
set_dc_partitions(PartitionList, DCID) ->
    NumPartitions = length(PartitionList),
    PartitionTuple = list_to_tuple(PartitionList),
    PartitionDict = 
	lists:foldl(fun(Part, Acc) ->
			    dict:store(Part,Part,Acc)
		    end, dict:new(), PartitionList),
    ok = stable_meta_data_server:broadcast_meta_data({partition_meta_data,DCID}, {PartitionDict,PartitionTuple,NumPartitions}),
    ok = stable_meta_data_server:broadcast_meta_data({partition_dict,DCID}, PartitionDict),
    ok = stable_meta_data_server:broadcast_meta_data({partition_list,DCID}, PartitionList),
    %% Be sure your dc is in the list before adding the new one to the list that includes you
    _MyDCID = get_my_dc_id(),
    %% Add the new one to the list that doesnt include you
    case DCID == get_my_dc_id() of
	false ->
	    lager:info("adding dcid ~p", [DCID]),
	    ok = stable_meta_data_server:broadcast_meta_data_merge(dc_list, DCID, fun ordsets:add_element/2, fun ordsets:new/0);
	true -> ok
    end,
    %% Add the new one to the list that includes you
    ok = stable_meta_data_server:broadcast_meta_data_merge(dc_list_w_me, DCID, fun ordsets:add_element/2, fun ordsets:new/0).

%% Get an ordered list of all the dc ids
-spec get_dc_ids(boolean()) -> [dcid()].
get_dc_ids(IncludeSelf) ->
    case IncludeSelf of
	true ->
	    case stable_meta_data_server:read_meta_data(dc_list_w_me) of
		{ok, List} ->
		    List;
		error ->
		    [get_my_dc_id()]
	    end;
	false ->
	    case stable_meta_data_server:read_meta_data(dc_list) of
		{ok, List} ->
		    List;
		error ->
		    []
	    end
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

    
