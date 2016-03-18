-module(replication_check).

-export([get_dc_partitions_detailed/1,
	 get_dc_partitions_dict/1,
	 load_partition_meta_data/0,
	 get_num_partitions/0,
	 get_partition_at_index/1,
	 get_dc_replicas_read/2,
	 get_dc_replicas_update/2,
	 is_replicated_here/1,
	 set_replication_list/1,
	 set_replication_fun/4,
	 create_biased_key_function/2]).


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
	    MyDC = dc_utilities:get_my_dc_id(),
	    ok = stable_meta_data_server:broadcast_meta_data(my_dc, MyDC),
	    ok = stable_meta_data_server:merge_meta_data(dc_list_w_me, dc_utilities:get_my_dcid(), fun ordsets:add_element/2, fun ordsets:new/1),
	    MyDC
    end.

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
    ok = stable_meta_data_server:merge_meta_data(dc_list, DCID, fun ordsets:add_element/2, fun ordsets:new/1),
    %% Be sure your dc is in the list before adding the new one to the list that includes you
    _MyDCID = get_my_dc_id(),
    %% Add the new one to the list that includes you
    ok = stable_meta_data_server:merge_meta_data(dc_list_w_me, DCID, fun ordsets:add_element/2, fun ordsets:new/1).

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


get_dc_replicas_update(Key,WithSelf) ->
    Key2 = get_key(Key),
    DcListFun = case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
		    [] ->
			%% inter_dc_manager:get_read_dcs();
			empty;
		    {Func} ->
			Func
		end,
    DcListList = case riak_core_metadata:get(?META_PREFIX_REPLI_UPDATE,Key2,[{default,[]}]) of
		     [] ->
			 empty;
		     Dcs ->
			 Dcs
		 end,
    DcList = case DcListList of
		 empty ->
		     case DcListFun of
			 empty ->
			     inter_dc_manager:get_dcs();
			 _ ->
			     DcIds = DcListFun(Key2),
			     keep_dcs(DcIds,inter_dc_manager:get_dcs_wids())
		     end;
		 _ ->
		     remove_ids(DcListList)
	     end,
    case WithSelf of
	noSelf ->
	    lists:delete(inter_dc_manager:get_my_dc(),DcList);
	withSelf ->
	    DcList
    end.

remove_ids(DcList) ->
    lists:foldl(fun({_Id,DC},Acc) ->
			[DC | Acc]
		end,[],DcList).

keep_dcs(Ids,DcList) ->
    lists:foldl(fun({Id,Dc},Acc) ->
			case lists:keymember(Id,1,Ids) of
			    true ->
				Acc ++ [Dc];
			    false ->
				Acc
			end
		end,[],DcList).


%% get_dc_replicas_update(Key,WithSelf) ->
%%     DcList = case riak_core_metadata:get(?META_PREFIX_REPLI_UPDATE,Key,[{default,[]}]) of
%% 		 [] ->
%% 		     inter_dc_manager:get_read_dcs();
%% 		 Dcs ->
%% 		     Dcs
%% 	     end,
%%     case WithSelf of
%% 	noSelf ->
%% 	    lists:delete(inter_dc_manager:get_my_dc(),DcList);
%% 	withSelf ->
%% 	    DcList
%%     end.
    

get_dc_replicas(Key,WithSelf) ->
    Key2 = get_key(Key),
    DcListFun = case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
		    [] ->
			%% inter_dc_manager:get_read_dcs();
			empty;
		    {Func} ->
			Func
		end,
    DcListList = case riak_core_metadata:get(?META_PREFIX_REPLI_READ,Key2,[{default,[]}]) of
		     [] ->
			 empty;
		     Dcs ->
			 Dcs
		 end,
    DcList = case DcListList of
		 empty ->
		     case DcListFun of
			 empty ->
			     inter_dc_manager:get_read_dcs();
			 _ ->
			     DcIds = DcListFun(Key2),
			     keep_dcs(DcIds,inter_dc_manager:get_read_dcs_wids())
		     end;
		 _ ->
		     remove_ids(DcListList)
	     end,
    case WithSelf of
	noSelf ->
	    lists:delete(inter_dc_manager:get_my_dc(),DcList);
	withSelf ->
	    DcList
    end.
    
is_replicated_here(InputKey) ->
    Key = get_key(InputKey),
    ResFun = 
	case stable_meta_data_server:read_meta_data(rep_func) of
	    error ->
		true;
	    {ok, {NumDcs, Func}} ->
		DcIds = Func(Key),
		

	case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
	    [] ->
		true;
	    {Func} ->
		DcIds = Func(Key2),
		{MyId,_} = inter_dc_manager:get_my_read_dc_wid(),
		lists:keymember(MyId,1,DcIds)
	end,
    ResList =
	case riak_core_metadata:get(?META_PREFIX_REPLI_READ,Key2,[{default,[]}]) of
	    [] ->
		empty;
	    DcList ->
		Dc = inter_dc_manager:get_my_read_dc_wid(),
		lists:member(Dc, DcList)
	end,
    case ResList of
	empty ->
	    ResFun;
	_ ->
	    ResList
    end.

-spec set_replication_fun(function()) -> ok.
set_replication_fun(Fun, NumDcs) ->
    ok = stable_meta_data_server:broadcast_meta_data(rep_func, {NumDcs, Fun}).

-spec set_replication_fun_round_robin(non_neg_integer(), non_neg_integer()) -> ok.
set_replication_fun_round_robin(RepFactor,NumDcs) ->
    Fun = create_biased_key_function(RepFactor,NumDcs),
    ok = stable_meta_data_server:broadcast_meta_data(rep_func, {NumDcs, Fun}).

-spec set_replication_list(list()) -> ok.
set_replication_list(ListKeyDcsList) ->
    lists:foreach(fun({InputKey,DcList}) ->
			  Key = get_key(InputKey),
			  ok = stable_meta_data_server:broadcast_meta_data({key_read,Key}, DcList),
		  end, ListKeyDcsList),
    ok.

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

-spec create_biased_key_function(non_neg_integer(), non_neg_integer()) -> function().
create_biased_key_function(ReplicationFactor,NumDcs) ->
    fun(InputKey) ->
	    Key = key_as_integer(InputKey),
	    FirstDc = case Key rem NumDcs of
			  0 ->
			      NumDcs;
			  Else ->
			      Else
		      end,
	    ListFun = fun(Self,Count,Acc) ->
			      case Count of
				  ReplicationFactor ->
				      Acc;
				  _ ->
				      case (FirstDc + Count) rem NumDcs of
					  0 ->
					      Self(Self,Count + 1,Acc ++ [{NumDcs}]);
					  Other ->
					      Self(Self,Count+1,Acc ++ [{Other}])
				      end
			      end
		      end,
	    ListFun(ListFun,1,[{FirstDc}])
    end.
    
