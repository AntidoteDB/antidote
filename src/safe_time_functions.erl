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

-module(safe_time_functions).

-include("antidote.hrl").

-export([update_func/2,
	 merge_entries/1,
	 put_in_meta_data/2,
	 get_stable_external_ids(),
	 export_funcs_and_vals/1]).

%% These functions are input to create a meta_data_sender
%% The functions merge by taking the minimum of all entries per node per DC
export_funcs_and_vals(Name) ->
    [Name, fun update_func/2, fun merge_entries/1, dict:new(), dict:new()].

%% Function used to store the meta-data
put_in_meta_data(Time, Dict) ->
    ok = meta_data_sender:put_meta_dict(stableIds, Partition, dict:store(internal_time, Time, NewIdDict)).

-spec get_stable_external_ids() -> {ok, dict()}.
get_stable_external_ids() ->
    case meta_data_sender:get_merged_data(stableIds) of
	undefined ->
	    %% The snapshot isn't realy yet, need to wait for startup
	    timer:sleep(10),
	    get_stable_external_ids();
	IdDict ->
	    {ok, IdDict}
    end.

%% Update fun is called on each entry of the dict after merging
%% Here just always take the new dict
update_func(Last,Time) ->
    case Last of
	undefined ->
	    true;
	_ ->
	    true
    end.

%% This assumes the dicts being sent have all DCs
merge_entries(Dict) ->
    %% Each entry will be a node/parition with a time dict of dcs with ids per partition
    %% If any entry is missing it will return a dict with undefined
    %% the key_internal time always contains the smallest physical clock of all partitions
    {ResultTime, ResultDict} =
	case dict:fold(fun meta_data_fold/3, {undefined, dict:new()}, Dict) of
	    {NodeId, undefined} ->
		{0, dict:store(NodeId, undefined, dict:new())};
	    {Time, ResultDict} ->
		{Time, ResultDict}
	end,
    dict:store(internal_time, ResultTime, ResultDict).

meta_data_fold(NodeId, undefined, _Acc) ->
    {NodeId, undefined};
meta_data_fold(_NodeId, _NodeDict, {NodeId1, undefined}) ->
    {NodeId1, undefined};
meta_data_fold(NodeId, PartitionDict, {AccTime, AccDict}) ->
    %% Merge the ids for each DC into a single dict with 1 entry per DC
    dict:fold(fun internal_fold/3, {AccTime, AccDict}, PartitionDict).

internal_fold(internal_time, Time, {undefined, InternalAcc}) ->
    {Time, InternalAcc};
internal_fold(internal_time, Time, {PrevTime, InternalAcc}) ->
    {erlang:min(Time, PrevTime), InternalAcc};
internal_fold(DCID, Ids, {Time, InternalAcc}) ->
    DCDict = 
	case dict:find(DCID,InternalAcc) of
	    {ok, Val} ->
		Val;
	    error ->
		dict:new()
	end,
    NewDCDict = 
	dict:merge(fun(K,V1,V2) ->
			   lager:error("Found duplicate entries when merging Ids"),
			   V1
		   end, DCDict, Ids),
    {Time, dict:store(DCID, NewDCDict, InternalAcc)}.
