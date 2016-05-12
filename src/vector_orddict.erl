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
-module(vector_orddict).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type vector_orddict() :: {[{vectorclock(),term()}],non_neg_integer()}.
-type nonempty_vector_orddict() :: {[{vectorclock(),term()}, ...],non_neg_integer()}.

-export_type([vector_orddict/0,nonempty_vector_orddict/0]).

-export([new/0,
	 get_smaller/2,
     get_causally_compatible/2,
     is_causally_compatible/4,
	 insert/3,
	 insert_bigger/3,
	 sublist/3,
	 size/1,
	 to_list/1,
	 from_list/1,
	 first/1,
	 last/1,
	 filter/2]).


%% @doc The vector orddict is an ordered dictionary used to store materialized snapshots whose order
%%      is described by vectorclocks.
-spec new() -> {[],0}.
new() ->
    {[],0}.


%% @doc gets the causally compatible version as defined
%%      by the nmsi protocol's causal snapshot.
-spec get_causally_compatible(transaction(), vector_orddict()) -> {undefined | {{vectorclock(), vectorclock()}, term()}, boolean()}.
get_causally_compatible(Transaction, {List, _Size}) ->
    DepUpbound = Transaction#transaction.nmsi_read_metadata#nmsi_read_metadata.dep_upbound,
    CommitTimeLowbound = Transaction#transaction.nmsi_read_metadata#nmsi_read_metadata.commit_time_lowbound,
    get_causally_compatible_internal(DepUpbound, CommitTimeLowbound, List, true).

get_causally_compatible_internal(_DepUpbound, _CommitTimeLowbound, [], IsFirst) ->
    {undefined, IsFirst};
get_causally_compatible_internal(DepUpbound, CommitTimeLowbound, [{{CommitClock, DepClock}, Value} | Rest], IsFirst) ->
    case is_causally_compatible(CommitClock, CommitTimeLowbound, DepClock, DepUpbound) of
        true ->
            {{Value, {CommitClock, DepClock}}, IsFirst};
        false ->
            get_causally_compatible_internal(DepUpbound, CommitTimeLowbound, Rest, false)
    end.

is_causally_compatible(CommitClock, CommitTimeLowbound, DepClock, DepUpbound) ->
    case ((CommitTimeLowbound == undefined) or (DepUpbound == undefined) or
            (CommitTimeLowbound == []) or (DepUpbound == [])) of
        true ->
            true;
        false ->
%%            lager:info("CommitClock= ~p~n CommitTimeLowbound= ~p~n, DepClock = ~p~n, DepUpbound = ~p~n",
%%                [CommitClock,CommitTimeLowbound, DepClock, DepUpbound]),
            vectorclock:ge(CommitClock, CommitTimeLowbound) and vectorclock:le(DepClock, DepUpbound)
    end.


-spec get_smaller(vectorclock(),vector_orddict()) -> {undefined | {vectorclock(),term()},boolean()}.
get_smaller(Vector,{List,_Size}) ->
    get_smaller_internal(Vector,List,true).

-spec get_smaller_internal(vectorclock(),[{vectorclock(),term()}],boolean()) -> {undefined | {vectorclock(),term()},boolean()}.
get_smaller_internal(_Vector,[],IsFirst) ->
    {undefined,IsFirst};
get_smaller_internal(Vector,[{FirstClock,FirstVal}|Rest],IsFirst) ->
    case vectorclock:le(FirstClock,Vector) of
	true ->
	    {{FirstVal, FirstClock},IsFirst};
	false ->
	    get_smaller_internal(Vector,Rest,false)
    end.

-spec insert(vectorclock(),term(),vector_orddict()) -> vector_orddict().
insert(Vector,Val,{List,Size}) ->
    insert_internal(Vector,Val,List,Size+1,[]).

-spec insert_internal(vectorclock(),term(),[{vectorclock(),term()}],non_neg_integer(),[{vectorclock(),term()}]) -> vector_orddict().
insert_internal(Vector,Val,[],Size,PrevList) ->
    {lists:reverse([{Vector,Val}|PrevList]),Size};

insert_internal(Vector,Val,[{FirstClock,FirstVal}|Rest],Size,PrevList) ->
    case vectorclock:gt(Vector,FirstClock) of
	true ->
	    {lists:reverse(PrevList,[{Vector,Val}|[{FirstClock,FirstVal}|Rest]]),Size};
	    %%PrevList;
	false ->
	    insert_internal(Vector,Val,Rest,Size,[{FirstClock,FirstVal}|PrevList])
    end.

-spec insert_bigger({vectorclock(), vectorclock()} | vectorclock(), term(),vector_orddict()) -> nonempty_vector_orddict().
insert_bigger({Vector1, Vector2},Val,{List,Size}) ->
    insert_bigger_internal({Vector1, Vector2},Val,List,Size);
insert_bigger(Vector,Val,{List,Size}) ->
    insert_bigger_internal(Vector,Val,List,Size).

-spec insert_bigger_internal({vectorclock(), vectorclock()}| vectorclock(),term(),[{vectorclock(),term()}],non_neg_integer()) -> nonempty_vector_orddict().
insert_bigger_internal({Vector1, Vector2},Val,[],0) ->
    {[{{Vector1, Vector2},Val}],1};

insert_bigger_internal({Vector1, Vector2},Val,[{{FirstClock1, FirstClock2},FirstVal}|Rest],Size) ->
    case not vectorclock:le(Vector1,FirstClock1) of
        true ->
            {[{{Vector1, Vector2},Val}|[{{FirstClock1, FirstClock2},FirstVal}|Rest]],Size+1};
        false ->
            {[{{FirstClock1, FirstClock2},FirstVal}|Rest],Size}
    end;

insert_bigger_internal(Vector,Val,[],0) ->
    {[{Vector,Val}],1};

insert_bigger_internal(Vector,Val,[{FirstClock,FirstVal}|Rest],Size) ->
    case not vectorclock:le(Vector,FirstClock) of
	true ->
	    {[{Vector,Val}|[{FirstClock,FirstVal}|Rest]],Size+1};
	false ->
	    {[{FirstClock,FirstVal}|Rest],Size}
    end.

-spec sublist(vector_orddict(),non_neg_integer(),non_neg_integer()) -> vector_orddict().
sublist({List,_Size}, Start, Len) ->
    Res = lists:sublist(List,Start,Len),
    {Res,length(Res)}.

-spec size(vector_orddict()) -> non_neg_integer().
size({_List,Size}) ->
    Size.

-spec to_list(vector_orddict()) -> [{vectorclock(),term()}].
to_list({List,_Size}) ->
    List.

-spec from_list([{vectorclock(),term()}]) -> vector_orddict().
from_list(List) ->
    {List,length(List)}.

-spec first(vector_orddict()) -> {vectorclock(), term()}.
first({[First|_Rest],_Size}) ->
    First.

-spec last(vector_orddict()) -> {vectorclock(), term()}.
last({List,_Size}) ->
    lists:last(List).

-spec filter(fun((term()) -> boolean()),vector_orddict()) -> vector_orddict().
filter(Fun,{List,_Size}) ->
    Result = lists:filter(Fun,List),
    {Result,length(List)}.


-ifdef(TEST).

vector_orddict_get_smaller_test() ->
    %% Fill up the vector
    Vdict0 = vector_orddict:new(),
    CT1 = vectorclock:from_list([{dc1,4},{dc2,4}]),
    Vdict1 = vector_orddict:insert(CT1, 1,Vdict0),
    CT2 = vectorclock:from_list([{dc1,8},{dc2,8}]),
    Vdict2 = vector_orddict:insert(CT2, 2, Vdict1),
    CT3 = vectorclock:from_list([{dc1,1},{dc2,10}]),
    Vdict3 = vector_orddict:insert(CT3, 3, Vdict2),
    
    %% Check you get the correct smaller snapshot
    ?assertEqual({undefined,false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1,0},{dc2,0}]),Vdict3)),
    ?assertEqual({undefined,false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1,1},{dc2,6}]),Vdict3)),
    ?assertEqual({{1, CT1},false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1,5},{dc2,5}]),Vdict3)),
    ?assertEqual({{2, CT2},true}, vector_orddict:get_smaller(vectorclock:from_list([{dc1,9},{dc2,9}]),Vdict3)),
    ?assertEqual({{3,CT3},false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1,3},{dc2,11}]),Vdict3)).
    


vector_orddict_insert_bigger_test() ->
    Vdict0 = vector_orddict:new(),
    %% Insert to empty dict
    CT1 = vectorclock:from_list([{dc1,4},{dc2,4}]),
    Vdict1 = vector_orddict:insert_bigger(CT1, 1,Vdict0),
    ?assertEqual(1,vector_orddict:size(Vdict1)),
    %% Should not insert because smaller
    CT2 = vectorclock:from_list([{dc1,3},{dc2,3}]),
    Vdict2 = vector_orddict:insert_bigger(CT2, 2, Vdict1),
    ?assertEqual(1,vector_orddict:size(Vdict2)),
    %% Should insert because bigger
    CT3 = vectorclock:from_list([{dc1,6},{dc2,10}]),
    Vdict3 = vector_orddict:insert_bigger(CT3, 3, Vdict2),
    ?assertEqual(2,vector_orddict:size(Vdict3)).


-endif.
