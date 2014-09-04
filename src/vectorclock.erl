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

-include("floppy.hrl").

-export([get_clock/1, update_clock/3, get_clock_by_key/1,
         is_greater_than/2,
         get_clock_of_dc/2,
         get_clock_node/1,
         from_list/1]).

-export_type([vectorclock/0]).

-type vectorclock() :: dict().

get_clock_by_key(Key) ->
    Logid = log_utilities:get_logid_from_key(Key),
    Preflist = log_utilities:get_preflist_from_logid(Logid),
    Indexnode = hd(Preflist),
    lager:info("Preflist of Key ~p vectorclock ~p", [Key, Indexnode]),
    riak_core_vnode_master:sync_command(
      Indexnode, get_clock, vectorclock_vnode_master).

-spec get_clock(Partition :: non_neg_integer())
               -> {ok, vectorclock()} | {error, term()}.
get_clock(Partition) ->
    Logid = log_utilities:get_logid_from_partition(Partition),
    Preflist = log_utilities:get_apl_from_logid(Logid, vectorclock),
    Indexnode = hd(Preflist),
    case riak_core_vnode_master:sync_command(
           Indexnode, get_clock, vectorclock_vnode_master) of
        {ok, Clock} ->
            {ok, Clock};
        {error, Reason} ->
            lager:info("Update vector clock failed: ~p",[Reason]),
            {error, Reason}
    end.

get_clock_node(Node) ->
    Preflist = riak_core_apl:active_owners(vectorclock),
    Prefnode = [{Partition, Node1} ||
                   {{Partition, Node1},_Type} <- Preflist, Node1 =:= Node],
    %% Take a random vnode
    {A1,A2,A3} = now(),
    _Seed = random:seed(A1, A2, A3),
    Index = random:uniform(length(Prefnode)),
    VecNode = lists:nth(Index, Prefnode),
    riak_core_vnode_master:sync_command(
      VecNode, get_clock, vectorclock_vnode_master).

-spec update_clock(Partition :: non_neg_integer(),
                   Dc_id :: term(), Timestamp :: non_neg_integer())
                  -> {ok, vectorclock()} | {error, term()}.
update_clock(Partition, Dc_id, Timestamp) ->
    Indexnode = {Partition, node()},
    lager:info("Preflist of vectorclokc ~p", [Indexnode]),
    case riak_core_vnode_master:sync_command(Indexnode,
                                             {update_clock, Dc_id, Timestamp},
                                             vectorclock_vnode_master) of
        {ok, Clock} ->
            {ok, Clock};
        {error, Reason} ->
            lager:info("Update vector clock failed: ~p",[Reason]),
            {error, Reason}
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
    dict:find(Dcid, VectorClock).

from_list(List) ->
    dict:from_list(List).
