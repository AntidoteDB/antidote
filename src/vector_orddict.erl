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


new() ->
    {[],0}.

get_smaller(Vector,{List,_Size}) ->
    get_smaller_internal(Vector,List,true).


get_smaller_internal(_Vector,[],IsFirst) ->
    {undefined,IsFirst};
get_smaller_internal(Vector,[{FirstClock,FirstVal}|Rest],IsFirst) ->
    case vectorclock:le(FirstClock,Vector) of
	true ->
	    {{FirstClock,FirstVal},IsFirst};
	false ->
	    get_smaller_internal(Vector,Rest,false)
    end.


insert(Vector,Val,{List,Size}) ->
    insert_internal(Vector,Val,List,Size+1,[]).

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


insert_bigger(Vector,Val,{List,Size}) ->
    insert_bigger_internal(Vector,Val,List,Size).

insert_bigger_internal(Vector,Val,[],0) ->
    {[{Vector,Val}],1};

insert_bigger_internal(Vector,Val,[{FirstClock,FirstVal}|Rest],Size) ->
    case vectorclock:is_greater_than(Vector,FirstClock) of
	true ->
	    {[{Vector,Val}|[{FirstClock,FirstVal}|Rest]],Size+1};
	false ->
	    {[{FirstClock,FirstVal}|Rest],Size}
    end.


sublist({List,_Size}, Start, Len) ->
    Res = lists:sublist(List,Start,Len),
    {Res,length(Res)}.

size({_List,Size}) ->
    Size.

to_list({List,_Size}) ->
    List.

from_list(List) ->
    {List,length(List)}.

first({[First|_Rest],_Size}) ->
    First.

last({List,_Size}) ->
    lists:last(List).

filter(Fun,{List,_Size}) ->
    Result = lists:filter(Fun,List),
    {Result,length(List)}.


%% ordered_filter(Fun,{[First|Rest],Size}) ->
%%     ordered_filter_internal(Fun,List,[],Size,0,Fun(First)).

%% ordered_filter_internal(_Fun,[],Prev,_Size,PrevSize,Check) ->
%%     case Check of
%% 	true ->
%% 	    {Prev,PrevSize};
%% 	false ->
%% 	    {[],0}
%%     end;
	
%% ordered_filter_internal(Fun,[First|Rest],Prev,Size,PrevSize,Check) ->
%%     case Fun(First) of
%% 	Check ->
%% 	    ordered_filter_internal(Fun,Rest,Prev++First,Size-1,PrevSize+1,Check);
%% 	_ ->
%% 	    case Check of
%% 		true ->
%% 		    {Prev,PrevSize};
%% 		false ->
%% 		    {[First|Rest],Size}
%% 	    end
%%     end.
