-module(replication_check).

-export([get_dc_replicas_read/2,
	 get_dc_replicas_update/2,
	 is_replicated_here/1,
	 set_replication/2]).

%-define(META_PREFIX_REPLI_UPDATE, {dcidupdate,replication}).
%-define(META_PREFIX_REPLI_READ, {dcidread,replication}).
-define(META_PREFIX_REPLI_FUNC, {dcidfunc,replication}).
-define(META_PREFIX_REPLI_DCNUM, {dcidnum,replication}).



get_dc_replicas_update(Key,WithSelf) ->
    DcList = case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
		 [] ->
		     inter_dc_manager:get_dcs();
		 {Func} ->
		     Key2 = get_key(Key),
		     DcIds = Func(Key2),
		     keep_dcs(DcIds,inter_dc_manager:get_dcs_wids())
	     end,
    case WithSelf of
	noSelf ->
	    lists:delete(inter_dc_manager:get_my_dc(),DcList);
	withSelf ->
	    DcList
    end.
    



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
    

get_dc_replicas_read(Key,WithSelf) ->
 DcList = case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
	      [] ->
		  inter_dc_manager:get_read_dcs();
	      {Func} ->
		  Key2 = get_key(Key),
		  DcIds = Func(Key2),
		  keep_dcs(DcIds,inter_dc_manager:get_read_dcs_wids())
	  end,
    case WithSelf of
	noSelf ->
	    lists:delete(inter_dc_manager:get_my_dc(),DcList);
	withSelf ->
	    DcList
    end.


%% get_dc_replicas_read(Key,WithSelf) ->
%%     DcList = case riak_core_metadata:get(?META_PREFIX_REPLI_READ,Key,[{default,[]}]) of
%% 		 [] ->
%% 		     inter_dc_manager:get_dcs();
%% 		 Dcs ->
%% 		     Dcs
%% 	     end,
%%     case WithSelf of
%% 	noSelf ->
%% 	    lists:delete(inter_dc_manager:get_my_read_dc(),DcList);
%% 	withSelf ->
%% 	    DcList
%%     end.

    
is_replicated_here(Key) ->
    case riak_core_metadata:get(?META_PREFIX_REPLI_FUNC,function,[{default,[]}]) of
	[] ->
	    true;
	{Func} ->
	    Key2 = get_key(Key),
	    DcIds = Func(Key2),
	    {MyId,_} = inter_dc_manager:get_my_read_dc_wid(),
	    lists:keymember(MyId,1,DcIds)
    end.


%% is_replicated_here(Key) ->
%%     case riak_core_metadata:get(?META_PREFIX_REPLI_READ,Key,[{default,[]}]) of
%% 	[] ->
%% 	    true;
%% 	DcList ->
%% 	    Dc = inter_dc_manager:get_my_read_dc(),
%% 	    lists:member(Dc, DcList)
%%     end.



set_replication(KeyFunction,DcNum) ->
    try
	riak_core_metadata:put(?META_PREFIX_REPLI_FUNC,function,{KeyFunction}),
	riak_core_metadata:put(?META_PREFIX_REPLI_DCNUM,number,DcNum)
    catch
	_:Reason ->
	    lager:error("Exception updating prelication meta data ~p", [Reason])
    end,
    ok.

%% set_replication(ListKeyDcsList) ->
%%     lists:foldl(fun({Key,DcListRead,DcListUpdate},_Acc2) ->
%% 			try
%% 			    riak_core_metadata:put(?META_PREFIX_REPLI_READ,Key,DcListRead),
%% 			    riak_core_metadata:put(?META_PREFIX_REPLI_UPDATE,Key,DcListUpdate)
%% 			catch
%% 			    _:Reason ->
%% 				lager:error("Exception updating prelication meta data ~p", [Reason])
%% 			end
%% 		end, 0, ListKeyDcsList),
%%     ok.

get_key(Key) ->
    case is_binary(Key) of
	true ->
	    list_to_integer(binary_to_list(Key));
	false ->
	    Key
    end.
		
