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

-export([get_clock/1, update_clock/3, get_clock_by_key/1,
         is_greater_than/2,
         get_clock_of_dc/2,
         set_clock_of_dc/3,
         get_stable_snapshot/0,
         from_list/1,
         eq/2,lt/2,gt/2,le/2,ge/2, strict_ge/2, strict_le/2]).

-export_type([vectorclock/0]).


-spec get_clock_by_key(Key :: key()) -> {ok, vectorclock:vectorclock()} | {error, term()}.
get_clock_by_key(Key) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    Indexnode = hd(Preflist),
    try
        riak_core_vnode_master:sync_command(
          Indexnode, get_clock, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

-spec get_clock(Partition :: non_neg_integer())
               -> {ok, vectorclock()} | {error, term()}.
get_clock(Partition) ->
    Indexnode = {Partition, node()},
    try
        riak_core_vnode_master:sync_command(
           Indexnode, get_clock, vectorclock_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc get_stable_snapshot: Returns stable snapshot time
%% in the current DC. stable snapshot time is the snapshot available at
%% in all partitions
-spec get_stable_snapshot() -> {ok, vectorclock:vectorclock()}.
get_stable_snapshot() ->
    %% This is fine if transactions coordinators exists on the ring (i.e. they have access
    %% to riak core meta-data) otherwise will have to change this
    {ok,vectorclock_vnode:get_stable_snapshot()}.
    

-spec update_clock(Partition :: non_neg_integer(),
                   Dc_id :: term(), Timestamp :: non_neg_integer())
                  -> ok | {error, term()}.
update_clock(Partition, Dc_id, Timestamp) ->
    Indexnode = {Partition, node()},
    try
        riak_core_vnode_master:sync_command(Indexnode,
          {update_clock, Dc_id, Timestamp}, vectorclock_vnode_master)
    catch
        _:R ->
            lager:error("Exception caught: ~p", [R]),
            {error, R}
    end.

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
