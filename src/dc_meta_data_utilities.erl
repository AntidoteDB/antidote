-module(dc_meta_data_utilities).

-include("antidote.hrl").

-export([get_dc_partitions_detailed/1,
	 get_dc_partitions_dict/1,
	 get_my_dc_id/0,
	 reset_my_dc_id/0,
	 set_dc_partitions/2,
	 get_dc_ids/1,
	 get_key/1,
	 key_as_integer/1,
	 load_partition_meta_data/0,
	 get_num_partitions/0,
	 get_partition_at_index/1]).


%% Returns a tuple of three elements
%% The first is a dict with all partitions for DCID, with key and value being the partition id
%% The second is a tuple with all partitions for DCID
%% The third is an integer telling the number of partitions
-spec get_dc_partitions_detailed(dcid()) -> {dict(),tuple(),non_neg_integer()}.
get_dc_partitions_detailed(DCID) ->
    case stable_meta_data_server:read_meta_data({partition_meta_data,DCID}) of
	{ok, Info} ->
	    Info;
	error ->
	    larger:error("Error no partitions for dc ~w", [DCID]),
	    {dict:new(), {}, 0}
    end.

%% Returns a dict with all partitions for DCID, with key and value being the partition id
-spec get_dc_partitions_dict(dcid()) -> dict().
get_dc_partitions_dict(DCID) ->
    case stable_meta_data_server:read_meta_data({partition_dict,DCID}) of
	{ok, Dict} ->
	    Dict;
	error ->
	    larger:error("Error no partitions for dc ~w", [DCID]),
	    dict:new()
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

%% Loads all the partitions ids into an ets table stored by
%% their index
-spec load_partition_meta_data() -> ok.
load_partition_meta_data() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    Length = length(PartitionList),
    ok = stable_meta_data_server:broadcast_meta_data({part,length}, Length),
    {_Len, IdPartitionList} = lists:foldl(fun(Partition,{PrevId,Acc}) ->
						  {PrevId + 1, Acc ++ [{{part,PrevId},Partition}]}
					  end, {1,[]}, PartitionList),
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
-spec get_partition_at_index(non_neg_integer()) -> term().
get_partition_at_index(Index) ->
    case stable_meta_data_server:read_meta_data({part,Index}) of
	{ok, Partition} ->
	    Partition;
	error ->
	    ok = load_partition_meta_data(),
	    get_partition_at_index(Index)
    end.
	
%% Add information about a DC to the meta_data
-spec set_dc_partitions([partition_id()],dcid()) -> ok.
set_dc_partitions(PartitionList, DCID) ->
    NumPartitions = length(PartitionList),
    PartitionTuple = list_to_tuple(PartitionList),
    PartitionDict = 
	list:foldl(fun(Part, Acc) ->
			   dict:store(Part,Part,Acc)
		   end, dict:new(), PartitionList),
    ok = stable_meta_data_server:broadcast_meta_data({partition_meta_data,DCID}, {PartitionDict,PartitionTuple,NumPartitions}),
    ok = stable_meta_data_server:broadcast_meta_data({partition_dict,DCID}, PartitionDict),
    %% Add the new one to the list that doesnt include you
    ok = stable_meta_data_server:broadcast_meta_data_merge(dc_list, DCID, fun ordsets:add_element/2, fun ordsets:new/0),
    %% Be sure your dc is in the list before adding the new one to the list that includes you
    _MyDCID = get_my_dc_id(),
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

    
