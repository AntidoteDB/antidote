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
	 insert/3,
	 insert_bigger/3,
	 sublist/3,
	 size/1,
	 to_list/1,
	 from_list/1,
	 first/1,
	 last/1,
	 filter/2]).


-spec new() -> {[],0}.
new() ->
    {[],0}.

-spec get_smaller(vectorclock(),vector_orddict()) -> {undefined | {vectorclock(),term()},boolean()}.
get_smaller(Vector,{List,_Size}) ->
    get_smaller_internal(Vector,List,true).

-spec get_smaller_internal(vectorclock(),[{vectorclock(),term()}],boolean()) -> {undefined | {vectorclock(),term()},boolean()}.
get_smaller_internal(_Vector,[],IsFirst) ->
    {undefined,IsFirst};
get_smaller_internal(Vector,[{FirstClock,FirstVal}|Rest],IsFirst) ->
    case vectorclock:le(FirstClock,Vector) of
	true ->
	    {{FirstClock,FirstVal},IsFirst};
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
    case vectorclock:is_greater_than(Vector,FirstClock) of
	true ->
	    {lists:reverse(PrevList,[{Vector,Val}|[{FirstClock,FirstVal}|Rest]]),Size};
	    %%PrevList;
	false ->
	    insert_internal(Vector,Val,Rest,Size,[{FirstClock,FirstVal}|PrevList])
    end.


-spec insert_bigger(vectorclock(),term(),vector_orddict()) -> nonempty_vector_orddict().
insert_bigger(Vector,Val,{List,Size}) ->
    insert_bigger_internal(Vector,Val,List,Size).

-spec insert_bigger_internal(vectorclock(),term(),[{vectorclock(),term()}],non_neg_integer()) -> nonempty_vector_orddict().
insert_bigger_internal(Vector,Val,[],0) ->
    {[{Vector,Val}],1};

insert_bigger_internal(Vector,Val,[{FirstClock,FirstVal}|Rest],Size) ->
    case vectorclock:is_greater_than(Vector,FirstClock) of
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
