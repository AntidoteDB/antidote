-module(replication_check).

-export([get_dc_replicas/2,
	 is_replicated_here/1,
	 set_replication/1]).

-define(META_PREFIX_REPLI, {dcid,replication}).


get_dc_replicas(Key,WithSelf) ->
    DcList = case riak_core_metadata:get(?META_PREFIX_REPLI,Key,[{default,[]}]) of
		 [] ->
		     inter_dc_manager:get_dcs();
		 Dcs ->
		     Dcs
	     end,
    case WithSelf of
	noSelf ->
	    lists:delete(inter_dc_manager:get_my_read_dc(),DcList);
	withSelf ->
	    DcList
    end.
    
    

is_replicated_here(Key) ->
    case riak_core_metadata:get(?META_PREFIX_REPLI,Key,[{default,[]}]) of
	[] ->
	    true;
	DcList ->
	    Dc = inter_dc_manager:get_my_read_dc(),
	    lists:member(Dc, DcList)
    end.


set_replication(ListKeyDcsList) ->
    lists:foldl(fun({Key,DcList},_Acc2) ->
			try
			    riak_core_metadata:put(?META_PREFIX_REPLI,Key,DcList)
			catch
			    _:Reason ->
				lager:error("Exception updating prelication meta data ~p", [Reason])
			end
		end, 0, ListKeyDcsList),
    ok.

