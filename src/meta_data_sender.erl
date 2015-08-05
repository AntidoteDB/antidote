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

-export([start_link/0,
	 put_meta_dict/2,
	 put_meta_data/3,
	 get_meta_dict/1,
	 get_stable_time/0,
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
	  last_time}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec put_meta_dict(partition_id(), dict()) -> ok.
put_meta_dict(Partition, Dict) ->
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    ok;
	_ ->
	    true = ets:insert(?META_TABLE_NAME, {Partition, Dict}),
	    ok
    end.

-spec put_meta_data(partition_id(), term(), term()) -> ok.
put_meta_data(Partition, Key, Value) ->
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
	    NewDict = dict:store(Key, Value, Dict),
	    put_meta_dict(Partition, NewDict)
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
    lager:info("removing partition ~p from meta data table", [Partition]),
    case ets:info(?META_TABLE_NAME) of
	undefined ->
	    false;
	_ ->
	    true = ets:delete(?META_TABLE_NAME, Partition),
	    ok
    end.

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec get_stable_time() -> vectorclock().
get_stable_time() ->
    case ets:info(?META_TABLE_STABLE_NAME) of
	undefined ->
	    dict:new();
	_ ->
	    case ets:lookup(?META_TABLE_STABLE_NAME, stable_time) of
		[] ->
		    dict:new();
		[{stable_time,Other}] ->
		    Other
	    end
    end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



init([]) ->
    Table = ets:new(?META_TABLE_STABLE_NAME, [set, named_table, ?META_TABLE_STABLE_CONCURRENCY]),
    Table2 = ets:new(?META_TABLE_NAME, [set, named_table, public, ?META_TABLE_CONCURRENCY]),
    LastTime = dict:new(),
    {ok, send_meta_data, #state{table = Table, table2 = Table2, last_time = LastTime}, ?META_DATA_SLEEP}.

send_meta_data(timeout, State = #state{last_time = LastTime}) ->
    TimeDict = get_meta_data(),
    NewTime = update_stable(TimeDict,LastTime),
    %% NodeList = nodes(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    MyNode = node(),
    NodeList = lists:delete(MyNode, riak_core_ring:ready_members(Ring)),
    ok = lists:foreach(fun(Node) ->
			       ok = meta_data_manager:update_time(Node,MyNode,dict:fetch(local_time,TimeDict))
		       end, NodeList),
    {next_state, send_meta_data, State#state{last_time = NewTime}, ?META_DATA_SLEEP}.


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




-spec get_meta_data() -> dict() | false.			 
get_meta_data() ->
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
	    TimeDict = dict:from_list(ets:tab2list(?REMOTE_META_TABLE_NAME)),
	    %% Be sure that you are only checking active nodes
	    NodeDict = node_list_to_dict(nodes()),
	    NewTimeDict =
		dict:fold(fun(NodeId, _Tab, Acc) ->
				  case dict:is_key(NodeId, NodeDict) of
				      true ->
					  Acc;
				      false ->
					  ok = meta_data_manager:remove_node(NodeId),
					  dict:erase(NodeId, Acc)
				  end
			  end, TimeDict, TimeDict),
	    LocalTime = get_min_time(dict:from_list(ets:tab2list(?META_TABLE_NAME))),
	    dict:store(local_time, LocalTime, NewTimeDict)
    end.

update_stable(TimeDict, LastTime) ->
    Min = get_min_time(TimeDict),
    {NewTime, NewMin} =
	dict:fold(fun(DcId, Time, {Acc, Bool}) ->
			  Last = case dict:find(DcId, LastTime) of
				     {ok, Val} ->
					 Val;
				     error ->
					 0
				 end,
			  case Time >= Last of
			      true ->
				  {dict:store(DcId, Time, Acc), true};
			      false ->
				  {Acc, Bool}
			  end
		  end, {LastTime, false}, Min),
    case NewMin of
	true ->
	    true = ets:insert(?META_TABLE_STABLE_NAME, {stable_time, NewTime});
	false ->
	    false
    end,
    NewTime.

%% This assumes the dicts being sent have all DCs
get_min_time(Dict) ->
    dict:fold(fun(_NodeId, NodeDict, Acc1) ->
		      dict:fold(fun(DcId, Time, Acc2) ->
					PrevTime = case dict:find(DcId, Acc2) of
						       {ok, Val} ->
							   Val;
						       error ->
							   Time
						   end,
					case PrevTime >= Time of
					    true ->
						dict:store(DcId, Time, Acc2);
					    false ->
						dict:store(DcId, PrevTime, Acc2)
					end
				end, Acc1, NodeDict)
	      end, dict:new(), Dict).

node_list_to_dict(NodeList) ->
    lists:foldl(fun(Node,Acc) ->
			dict:store(Node,0,Acc)
		end, dict:new(),NodeList).
