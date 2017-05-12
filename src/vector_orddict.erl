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

-type vector_orddict() :: {[{vectorclock(), term()}], non_neg_integer()}.
-type nonempty_vector_orddict() :: {[{vectorclock(), term()}, ...], non_neg_integer()}.

-export_type([vector_orddict/0, nonempty_vector_orddict/0]).

-export([new/0,
  get_smaller/2,
  get_smaller_from_id/3,
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
%%      Note that the elements are stored in a sorted list going from big to small (left to right).
-spec new() -> {[], 0}.
new() ->
  {[], 0}.

%% @doc Get the first appropiate element from the dict according to a =< ordering.
%%
%%      get_smaller(Clock, Dict) will return {{DClock, _}=Entry, IsFirst},
%%      where Entry is the most recent entry such that DClock =< Clock.
%%
%%      In addition, return IsFirst, indicating if the selected entry was the newest entry
%%      in the orddict.
%%
-spec get_smaller(vectorclock(), vector_orddict()) -> {undefined | {vectorclock(), term()}, boolean()}.
get_smaller(Vector, {List, _Size}) ->
  get_smaller_internal(Vector, List, true).

-spec get_smaller_internal(vectorclock(), [{vectorclock(), term()}], boolean()) -> {undefined | {vectorclock(), term()}, boolean()}.
get_smaller_internal(_Vector, [], IsFirst) ->
  {undefined, IsFirst};
get_smaller_internal(Vector, [{FirstClock, FirstVal}|Rest], IsFirst) ->
  case vectorclock:le(FirstClock, Vector) of
    true ->
      {{FirstClock, FirstVal}, IsFirst};
    false ->
      get_smaller_internal(Vector, Rest, false)
  end.

-spec get_smaller_from_id(term(), clock_time(), vector_orddict()) -> undefined | {vectorclock(), term()}.
get_smaller_from_id(_Id, _Time, {_List, Size}) when Size == 0 ->
  undefined;
get_smaller_from_id(Id, Time, {List, _Size}) ->
  get_smaller_from_id_internal(Id, Time, List).

-spec get_smaller_from_id_internal(term(), clock_time(), [{vectorclock, term()}, ...]) -> undefined | {vectorclock(), term()}.
get_smaller_from_id_internal(_Id, _Time, []) ->
  undefined;
get_smaller_from_id_internal(Id, Time, [{Clock, Val}|Rest]) ->
  ValTime = vectorclock:get_clock_of_dc(Id, Clock),
  case ValTime =< Time of
    true ->
      {Clock, Val};
    false ->
      get_smaller_from_id_internal(Id, Time, Rest)
  end.

-spec insert(vectorclock(), term(), vector_orddict()) -> vector_orddict().
insert(Vector, Val, {List, Size}) ->
  insert_internal(Vector, Val, List, Size+1, []).

-spec insert_internal(vectorclock(), term(), [{vectorclock(), term()}], non_neg_integer(), [{vectorclock(), term()}]) -> vector_orddict().
insert_internal(Vector, Val, [], Size, PrevList) ->
  {lists:reverse([{Vector, Val}|PrevList]), Size};

insert_internal(Vector, Val, [{FirstClock, FirstVal}|Rest], Size, PrevList) ->
  case vectorclock:all_dots_greater(Vector, FirstClock) of
    true ->
      {lists:reverse(PrevList, [{Vector, Val}|[{FirstClock, FirstVal}|Rest]]), Size};
    %%PrevList;
    false ->
      insert_internal(Vector, Val, Rest, Size, [{FirstClock, FirstVal}|PrevList])
  end.


-spec insert_bigger(vectorclock(), term(), vector_orddict()) -> nonempty_vector_orddict().
insert_bigger(Vector, Val, {List, Size}) ->
  insert_bigger_internal(Vector, Val, List, Size).

-spec insert_bigger_internal(vectorclock(), term(), [{vectorclock(), term()}], non_neg_integer()) -> nonempty_vector_orddict().
insert_bigger_internal(Vector, Val, [], 0) ->
  {[{Vector, Val}], 1};

insert_bigger_internal(Vector, Val, [{FirstClock, FirstVal}|Rest], Size) ->
  case not vectorclock:le(Vector, FirstClock) of
    true ->
      {[{Vector, Val}|[{FirstClock, FirstVal}|Rest]], Size+1};
    false ->
      {[{FirstClock, FirstVal}|Rest], Size}
  end.

-spec sublist(vector_orddict(), non_neg_integer(), non_neg_integer()) -> vector_orddict().
sublist({List, _Size}, Start, Len) ->
  Res = lists:sublist(List, Start, Len),
  {Res, length(Res)}.

-spec size(vector_orddict()) -> non_neg_integer().
size({_List, Size}) ->
  Size.

-spec to_list(vector_orddict()) -> [{vectorclock(), term()}].
to_list({List, _Size}) ->
  List.

-spec from_list([{vectorclock(), term()}]) -> vector_orddict().
from_list(List) ->
  {List, length(List)}.

-spec first(vector_orddict()) -> {vectorclock(), term()}.
first({[First|_Rest], _Size}) ->
  First.

-spec last(vector_orddict()) -> {vectorclock(), term()}.
last({List, _Size}) ->
  lists:last(List).

-spec filter(fun((term()) -> boolean()), vector_orddict()) -> vector_orddict().
filter(Fun, {List, _Size}) ->
  Result = lists:filter(Fun, List),
  {Result, length(List)}.


-ifdef(TEST).

vector_oddict_get_smaller_from_id_test() ->
  %% Fill up the vector
  Vdict0 = vector_orddict:new(),
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert(CT1, 1, Vdict0),
  CT2 = vectorclock:from_list([{dc1, 8}, {dc2, 8}]),
  Vdict2 = vector_orddict:insert(CT2, 2, Vdict1),
  CT3 = vectorclock:from_list([{dc1, 1}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert(CT3, 3, Vdict2),

  %% Check you get the correct smaller snapshot
  ?assertEqual(undefined, vector_orddict:get_smaller_from_id(dc1, 0, Vdict0)),
  ?assertEqual(undefined, vector_orddict:get_smaller_from_id(dc1, 0, Vdict3)),
  ?assertEqual({CT3, 3}, vector_orddict:get_smaller_from_id(dc1, 1, Vdict3)),
  ?assertEqual({CT2, 2}, vector_orddict:get_smaller_from_id(dc2, 9, Vdict3)).


vector_orddict_get_smaller_test() ->
  %% Fill up the vector
  Vdict0 = vector_orddict:new(),
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert(CT1, 1, Vdict0),
  CT2 = vectorclock:from_list([{dc1, 8}, {dc2, 8}]),
  Vdict2 = vector_orddict:insert(CT2, 2, Vdict1),
  CT3 = vectorclock:from_list([{dc1, 1}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert(CT3, 3, Vdict2),

  %% Check you get the correct smaller snapshot
  ?assertEqual({undefined, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 0}, {dc2, 0}]), Vdict3)),
  ?assertEqual({undefined, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 1}, {dc2, 6}]), Vdict3)),
  ?assertEqual({{CT1, 1}, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 5}, {dc2, 5}]), Vdict3)),
  ?assertEqual({{CT2, 2}, true}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 9}, {dc2, 9}]), Vdict3)),
  ?assertEqual({{CT3, 3}, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 3}, {dc2, 11}]), Vdict3)).



vector_orddict_insert_bigger_test() ->
  Vdict0 = vector_orddict:new(),
  %% Insert to empty dict
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert_bigger(CT1, 1, Vdict0),
  ?assertEqual(1, vector_orddict:size(Vdict1)),
  %% Should not insert because smaller
  CT2 = vectorclock:from_list([{dc1, 3}, {dc2, 3}]),
  Vdict2 = vector_orddict:insert_bigger(CT2, 2, Vdict1),
  ?assertEqual(1, vector_orddict:size(Vdict2)),
  %% Should insert because bigger
  CT3 = vectorclock:from_list([{dc1, 6}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert_bigger(CT3, 3, Vdict2),
  ?assertEqual(2, vector_orddict:size(Vdict3)).


-endif.
