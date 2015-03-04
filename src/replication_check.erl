-module(replication_check).

-export([get_dc_replicas/1,
	 is_replicated_here/2,
	 set_replication/1]).

-define(META_PREFIX_REPLI, {dcid,replication}).


get_dc_replicas(Key) ->
    case riak_core_metadata:get(?META_PREFIX_REPLI,Key,[{default,[]}]) of
	[] ->
	    inter_dc_manager:get_dcs();
	DcList ->
	    DcList
    end.
    

is_replicated_here(Key, Dc) ->
    case riak_core_metadata:get(?META_PREFIX_REPLI,Key,[{default,[]}]) of
	[] ->
	    true;
	DcList ->
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

