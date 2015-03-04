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
-module(vectorclock).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_safe_time/1,
	 %% get_clock_by_key/1,
         is_greater_than/2,
         get_clock_of_dc/2,
         set_clock_of_dc/3,
         %% get_stable_snapshot/0,
         from_list/1,
	 wait_for_clock/1,
	 get_random_node/0,
	 update_sent_clock/3,
	 update_safe_vector_local/1,
	 update_safe_vector_local/2,
	 update_safe_clock_local/2,
	 update_safe_clock_local/3,
	 get_safe_time/0,
         eq/2,lt/2,gt/2,le/2,ge/2, strict_ge/2, strict_le/2]).

-export_type([vectorclock/0]).

-type vectorclock() :: dict().

%% -spec get_clock_by_key(Key :: key()) -> {ok, vectorclock:vectorclock()} | {error, term()}.
%% get_clock_by_key(Key) ->
%%     Preflist = log_utilities:get_preflist_from_key(Key),
%%     Indexnode = hd(Preflist),
%%     try
%%         riak_core_vnode_master:sync_command(
%%           Indexnode, get_clock, vectorclock_vnode_master)
%%     catch
%%         _:Reason ->
%%             lager:error("Exception caught: ~p", [Reason]),
%%             {error, Reason}
%%     end.

-spec get_safe_time(Partition :: non_neg_integer())
               -> {ok, vectorclock()} | {error, term()}.
get_safe_time(Partition) ->
    Indexnode = {Partition, node()},
    try
%% Chage to get_safe_time from get_clock
        riak_core_vnode_master:sync_command(
           Indexnode, get_safe_time, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
%% -spec get_stable_snapshot() -> {ok, vectorclock:vectorclock()} | {error, term()}.
%% get_stable_snapshot() ->
%%     Indexnode = vectorclock:get_random_node(),
%%     try
%%         riak_core_vnode_master:sync_command(
%%           Indexnode, get_stable_snapshot, vectorclock_vnode_master)
%%     catch
%%         _:Reason ->
%%             lager:error("Exception caught: ~p", [Reason]),
%%             {error, Reason}
%%     end.



%%update_sent_clock(Partition, Dc_id, Timestamp) ->
    
update_sent_clock({DcAddress,Port}, Partition, StableTime) ->
%% need to fix this so it takes the min of the stabletime and any live
%% transactions
%% Actaully this should be ok?? bc stable time comes from 
%% clock_si_transaction_reader
    collect_sent_time_fsm:update_sent_time(
      {DcAddress,Port}, Partition, StableTime).



%% update_sent_clock_old(Partition, Dc_id, Timestamp) ->
%%     Indexnode = {Partition, node()},
%%     try
%%         case riak_core_vnode_master:sync_command(Indexnode,
%%                                              {update_sent_clock, Dc_id, Partition, Timestamp},
%%                                              vectorclock_vnode_master) of
%%             {ok, Clock} ->
%%                 {ok, Clock};
%%             {error, Reason} ->
%%                 lager:info("Update sent clock failed: ~p",[Reason]),
%%                 {error, Reason}
%%         end
%%     catch
%%         _:R ->
%%             lager:error("Exception caught: ~p", [R]),
%%             {error, R}
%%     end.



-spec get_safe_time() -> {ok, vectorclock:vectorclock()} | {error, term()}.
get_safe_time() ->
    %% Right now this only gets the safe time of the local vector
    Indexnode = vectorclock:get_random_node(),
    try
        riak_core_vnode_master:sync_command(
          Indexnode, get_safe_time, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% The update_safe_vector_local functions take a clock that is then used
%% to update the safe time, and also returns a clock with the possibly
%% updated safe time
%% If no partition is given, it just calls a random partition
-spec update_safe_vector_local(Vector :: vectorclock:vectorclock())
			      -> {ok, vectorclock:vectorclock()} | {error, term()}.
update_safe_vector_local(Vector) ->
    Indexnode = vectorclock:get_random_node(),
    update_safe_vector_local(Indexnode, Vector).

-spec update_safe_vector_local(Partition :: non_neg_integer(), Vector :: vectorclock:vectorclock())
			      -> {ok, vectorclock:vectorclock()} | {error, term()}.
update_safe_vector_local(Partition, Vector) ->
    try
%%	lager:info("in update safe vector local"),
	case riak_core_vnode_master:sync_command(Partition,
						 {update_safe_vector_local, Vector},
						 vectorclock_vnode_master) of
	    {ok, Clock} ->
		{ok, Clock};
	    {error, Reason} ->
		lager:info("Update safe vector failed ~p", [Reason]),
		{error, Reason}
	end
    catch
	_:R ->
	    lager:error("Exception caught: ~p", [R]),
	    {error, R}
    end.



-spec update_safe_clock_local(Dc_id :: term(), Timestamp :: non_neg_integer())
			     -> {ok, non_neg_integer()} | {error, term()}.
update_safe_clock_local(Dc_id, Timestamp) ->
    Indexnode = vectorclock:get_random_node(),
    update_safe_clock_local(Dc_id, Indexnode, Timestamp).


%% This just updates the safeclock of the local partition
-spec update_safe_clock_local(Dc_id :: term(), Partition :: term(), Timestamp :: non_neg_integer())
			     -> {ok, non_neg_integer()} | {error, term()}.
update_safe_clock_local(Dc_id, Partition, Timestamp) ->
    try
	case riak_core_vnode_master:sync_command(Partition,
						 {update_safe_clock, false, Dc_id, Timestamp},
						 vectorclock_vnode_master) of
	    {ok, Clock} ->
		{ok, Clock};
	    {error, Reason} ->
		lager:info("Update safe clock failed ~p",[Reason]),
		{error, Reason}
	end
    catch
	_:R ->
	    lager:error("Exception caught: ~p", [R]),
	    {error, R}
    end.
    

%% This updates the safeclock for all partitions by sending riak-core
%% meta-data
%% Dont use this for now
%% -spec update_safe_clock(Dc_id :: term(), Timestamp :: non_neg_integer())
%% 		       -> {ok, vectorclock()} | {error, term()}.
%% update_safe_clock(Dc_id, Timestamp) ->
%%     Indexnode = vectorclock:get_random_node(),
%%     try
%% 	case riak_core_vnode_master:sync_command(Indexnode,
%% 						 {update_safe_clock, false, Dc_id, Timestamp},
%% 						 vectorclock_vnode_master) of
%% 	    {ok, Clock} ->
%% 		{ok, Clock};
%% 	    {error, Reason} ->
%% 		lager:infor("Update safe clock failed ~p",[Reason]),
%% 		{error, Reason}
%% 	end
%%     catch
%% 	_:R ->
%% 	    lager:error("Exception caught: ~p", [R]),
%% 	    {error, R}
%%     end.
    

%% -spec update_clock(Partition :: non_neg_integer(),
%%                    Dc_id :: term(), Timestamp :: non_neg_integer())
%%                   -> {ok, vectorclock()} | {error, term()}.
%% update_clock(Partition, Dc_id, Timestamp) ->
%%     Indexnode = {Partition, node()},
%%     try
%%         case riak_core_vnode_master:sync_command(Indexnode,
%%                                              {update_clock, Dc_id, Timestamp},
%%                                              vectorclock_vnode_master) of
%%             {ok, Clock} ->
%%                 {ok, Clock};
%%             {error, Reason} ->
%%                 lager:info("Update vector clock failed: ~p",[Reason]),
%%                 {error, Reason}
%%         end
%%     catch
%%         _:R ->
%%             lager:error("Exception caught: ~p", [R]),
%%             {error, R}
%%     end.


-spec wait_for_clock(Clock :: vectorclock:vectorclock()) ->
                           {ok, vectorclock:vectorclock()} | {error, term()}.
wait_for_clock(Clock) ->
   case get_safe_time() of
       {ok, VecSnapshotTime} ->
           case vectorclock:ge(VecSnapshotTime, Clock) of
               true ->
                   %% No need to wait
                   {ok, VecSnapshotTime};
               false ->
                   %% wait for snapshot time to catch up with Client Clock
                   timer:sleep(100),
                   wait_for_clock(Clock)
           end;
       {error, Reason} ->
          {error, Reason}
  end.




%% @doc Returns a random node
get_random_node() ->
    % Send the update to a random node
    Node = node(),
    Preflist = riak_core_apl:active_owners(vectorclock),
    Prefnode = [{Partition, Node1} ||
                   {{Partition, Node1},_Type} <- Preflist, Node1 =:= Node],
    %% Take a random vnode
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Index = random:uniform(length(Prefnode)),
    lists:nth(Index, Prefnode).




%% @doc Return true if Clock1 > Clock2
-spec is_greater_than(Clock1 :: vectorclock(), Clock2 :: vectorclock())
                     -> boolean().
is_greater_than(Clock1, Clock2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, Clock1) of
                           {ok, Time1} ->
                               case Time1 > Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error -> %%Localclock has not observered some dcid
                               false
                       end
               end,
               true, Clock2).

get_clock_of_dc(Dcid, VectorClock) ->
    case dict:find(Dcid, VectorClock) of
        {ok, Value} ->
            {ok, Value};
        error ->
            {ok, 0}
    end.

set_clock_of_dc(DcId, Time, VectorClock) ->
    dict:update(DcId,
                fun(_Value) ->
                        Time
                end,
                Time,
                VectorClock).

from_list(List) ->
    dict:from_list(List).

eq(V1, V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 =:= Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

le(V1, V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 =< Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               Result
                       end
               end,
               true, V2).

ge(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 >= Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

lt(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 < Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               Result
                       end
               end,
               true, V2).

gt(V1,V2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, V1) of
                           {ok, Time1} ->
                               case Time1 > Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error ->
                               false
                       end
               end,
               true, V2).

strict_ge(V1,V2) ->
    ge(V1,V2) and (not eq(V1,V2)).

strict_le(V1,V2) ->
    le(V1,V2) and (not eq(V1,V2)).

-ifdef(TEST).

vectorclock_test() ->
    V1 = vectorclock:from_list([{1,5},{2,4},{3,5},{4,6}]),
    V2 = vectorclock:from_list([{1,4}, {2,3}, {3,4},{4,5}]),
    V3 = vectorclock:from_list([{1,5}, {2,4}, {3,4},{4,5}]),
    V4 = vectorclock:from_list([{1,6},{2,3},{3,1},{4,7}]),
    V5 = vectorclock:from_list([{1,6},{2,7}]),
    ?assertEqual(gt(V1,V2), true),
    ?assertEqual(lt(V2,V1), true),
    ?assertEqual(gt(V1,V3), false),
    ?assertEqual(strict_ge(V1,V3), true),
    ?assertEqual(strict_ge(V1,V1), false),
    ?assertEqual(ge(V1,V4), false),
    ?assertEqual(le(V1,V4), false),
    ?assertEqual(eq(V1,V4), false),
    ?assertEqual(ge(V1,V5), false).

-endif.
