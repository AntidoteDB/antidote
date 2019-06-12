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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.

-export([all_dots_greater/2, all_dots_smaller/2, conc/2,
     eq/2, fold/3, from_list/1, ge/2, get/2,
     gt/2, le/2, lt/2, map/2, max/1, min/1,
     min_clock/2, new/0, set_all/2,
     set/3, size/1, to_list/1, update_with/4]).

-type vc_node() :: term().

-type vectorclock() :: #{vc_node() => pos_integer()}.

-export_type([vectorclock/0]).

-spec new() -> vectorclock().
new() -> maps:new().

-spec get(vc_node(), vectorclock()) -> non_neg_integer().
get(Key, VectorClock) ->
  maps:get(Key, VectorClock, 0).

-spec set(vc_node(), pos_integer(), vectorclock()) -> vectorclock().
set(Key, Value, VectorClock) when Value > 0 ->
  VectorClock#{Key => Value}.

-spec set_all(pos_integer(), vectorclock()) -> vectorclock().
set_all(Value, VectorClock) when Value > 0  ->
  Fun = fun(_K, _V) -> Value end,
  map(Fun, VectorClock).

-spec from_list([{vc_node(), pos_integer()}]) -> vectorclock().
from_list(List) -> maps:from_list(List).

-spec to_list(vectorclock()) -> [{vc_node(), pos_integer()}].
to_list(VectorClock) -> maps:to_list(VectorClock).

-spec map(fun((vc_node(), pos_integer()) -> pos_integer()), vectorclock()) -> vectorclock().
map(Fun, VectorClock) -> maps:map(Fun, VectorClock).

-spec fold(fun ((vc_node(), pos_integer(), X) -> X), X, vectorclock()) -> X.
fold(Fun, Init, VectorClock) ->
    maps:fold(Fun, Init, VectorClock).

-spec update_with(vc_node(), fun((pos_integer()) -> pos_integer()), pos_integer(), vectorclock()) -> vectorclock().
update_with(Key, Fun, Init, VectorClock) ->
    maps:update_with(Key, Fun, Init, VectorClock).

-spec min_clock(vectorclock(), [vc_node()]) -> non_neg_integer().
min_clock(_VectorClock, []) ->
    0;
min_clock(VectorClock, Nodes) ->
    lists:min([get(Node, VectorClock) || Node <- Nodes]).

-spec max([vectorclock()]) -> vectorclock().
max([]) -> new();
max([V]) -> V;
max([V1, V2 | T]) -> max([max2(V1, V2) | T]).

%% component-wise maximum of two clocks
-spec max2(vectorclock(), vectorclock()) -> vectorclock().
max2(V1, V2) ->
  FoldFun =
    fun(DC, A, Acc) ->
      B = get(DC, Acc),
      case A > B of
        true -> Acc#{DC => A};
        false -> Acc
      end
    end,
    maps:fold(FoldFun, V2, V1).

-spec min([vectorclock()]) -> vectorclock().
min([]) -> new();
min([V]) -> V;
min([V1, V2 | T]) ->
    min([min2(V1, V2) | T]).

%% component-wise minimum of two clocks
-spec min2(vectorclock(), vectorclock()) -> vectorclock().
min2(V1, V2) ->
    FoldFun = fun (DC, A, Acc) ->
        B = get(DC, V2),
        C = min(A, B),
        case C of
            0 -> Acc;
            _ -> Acc#{DC => C}
        end
    end,
    maps:fold(FoldFun, new(), V1).

-spec size(vectorclock()) -> pos_integer().
size(V) -> maps:size(V).

-spec for_all_keys(fun ((pos_integer(), pos_integer()) -> boolean()), vectorclock(), vectorclock()) -> boolean().
for_all_keys(F, V1, V2) ->
    AllDCs = maps:keys(maps:merge(V1, V2)),
    Func = fun (DC) ->
        A = get(DC, V1),
        B = get(DC, V2),
        F(A, B)
        end,
    lists:all(Func, AllDCs).

-spec eq(vectorclock(), vectorclock()) -> boolean().
eq(V1, V2) -> le(V1, V2) andalso le(V2, V1).

-spec le(vectorclock(), vectorclock()) -> boolean().
le(V1, V2) ->
  try
    maps:fold(fun (DC, V, true) ->
                case V =< get(DC, V2) of
                  true -> true;
                  false -> throw(false)
                end
              end, true, V1)
  catch
    false -> false
  end .

-spec ge(vectorclock(), vectorclock()) -> boolean().
ge(V1, V2) -> le(V2, V1).

-spec all_dots_smaller(vectorclock(), vectorclock()) -> boolean().
all_dots_smaller(V1, V2) ->
    for_all_keys(fun (A, B) -> A < B end, V1, V2).

-spec all_dots_greater(vectorclock(), vectorclock()) -> boolean().
all_dots_greater(V1, V2) ->
    for_all_keys(fun (A, B) -> A > B end, V1, V2).

-spec gt(vectorclock(), vectorclock()) -> boolean().
gt(V1, V2) -> lt(V2, V1).

-spec lt(vectorclock(), vectorclock()) -> boolean().
lt(V1, V2) ->
    try maps:fold(fun (DC, V, Acc) ->
              X = get(DC, V2),
              case V =< X of
                true -> Acc orelse V < X;
                false -> throw(false)
              end
          end,
          false, V1)
        orelse
        maps:fold(fun (DC, V, _) ->
                X = get(DC, V1),
                case V > X of
                  true -> throw(true);
                  false -> false
                end
            end,
            false, V2)
    catch
      R -> R
    end.

-spec conc(vectorclock(), vectorclock()) -> boolean().
conc(V1, V2) -> not ge(V1, V2) andalso not le(V1, V2).

-ifdef(TEST).

vectorclock_empty_test() ->
    V1 = new(),
    V2 = from_list([]),
    ?assertEqual(V1, V2),
    ?assertEqual((eq(min([]), max([]))), true),
    ?assertEqual((to_list(V1)), []).

vectorclock_test() ->
    V1 = from_list([{1, 5}, {2, 4}, {3, 5}, {4, 6}]),
    V2 = from_list([{1, 4}, {2, 3}, {3, 4}, {4, 5}]),
    V3 = from_list([{1, 5}, {2, 4}, {3, 4}, {4, 5}]),
    V4 = from_list([{1, 6}, {2, 3}, {3, 1}, {4, 7}]),
    V5 = from_list([{1, 6}, {2, 7}]),
    ?assert(all_dots_greater(V1, V2)),
    ?assert(all_dots_smaller(V2, V1)),
    ?assertNot(all_dots_greater(V1, V3)),
    ?assert(gt(V1, V3)),
    ?assertNot(gt(V1, V1)),
    ?assertNot(ge(V1, V4)),
    ?assertNot(le(V1, V4)),
    ?assertNot(eq(V1, V4)),
    ?assertNot(ge(V1, V5)).

vectorclock_lt_test() ->
    ?assertEqual((lt(from_list([{a, 1}]), from_list([{a, 1}, {b, 1}]))), true),
    ?assertEqual((lt(from_list([{a, 1}]), from_list([{a, 1}]))), false),
    ?assertEqual((lt(from_list([{a, 2}]), from_list([{a, 1}]))), false).

vectorclock_max_test() ->
    V1 = from_list([{1, 5}, {2, 4}]),
    V2 = from_list([{1, 6}, {2, 3}]),
    V3 = from_list([{1, 3}, {3, 2}]),
    Expected12 = from_list([{1, 6}, {2, 4}]),
    Expected23 = from_list([{1, 6}, {2, 3}, {3, 2}]),
    Expected13 = from_list([{1, 5}, {2, 4}, {3, 2}]),
    Expected123 = from_list([{1, 6}, {2, 4}, {3, 2}]),
    Unexpected123 = from_list([{1, 5}, {2, 5}, {3, 5}]),
    ?assertEqual((eq(max([V1, V2]), Expected12)), true),
    ?assertEqual((eq(max([V2, V3]), Expected23)), true),
    ?assertEqual((eq(max([V1, V3]), Expected13)), true),
    ?assertEqual((eq(max([V1, V2, V3]), Expected123)), true),
    ?assertEqual((eq(max([V1, V2, V3]), Unexpected123)), false).

vectorclock_min_test() ->
    V1 = from_list([{1, 5}, {2, 4}]),
    V2 = from_list([{1, 6}, {2, 3}]),
    V3 = from_list([{1, 3}, {3, 2}]),
    Expected12 = from_list([{1, 5}, {2, 3}]),
    Expected23 = from_list([{1, 3}]),
    Expected13 = from_list([{1, 3}]),
    Expected123 = from_list([{1, 3}]),
    Unexpected123 = from_list([{1, 3}, {2, 3}, {3, 2}]),
    ?assert(eq(min([V1, V2]), Expected12)),
    ?assert(eq(min([V2, V3]), Expected23)),
    ?assert(eq(min([V1, V3]), Expected13)),
    ?assert(eq(min([V1, V2, V3]), Expected123)),
    ?assertNot(eq(min([V1, V2, V3]), Unexpected123)),
    ?assert(eq(vectorclock:min([V1]), vectorclock:max([V1]))).

vectorclock_conc_test() ->
    V1 = from_list([{1, 5}, {2, 4}]),
    V2 = from_list([{1, 6}, {2, 3}]),
    V3 = from_list([{1, 3}, {3, 2}]),
    V4 = from_list([{1, 6}, {3, 3}]),
    V5 = from_list([{1, 6}]),
    ?assert(conc(V1, V2)),
    ?assert(conc(V2, V3)),
    ?assertNot(conc(V3, V4)),
    ?assertNot(conc(V5, V4)).

vectorclock_set_test() ->
    V1 = from_list([{1, 1}, {2, 2}]),
    V2 = from_list([{1, 1}, {2, 2}, {3, 3}]),
    V3 = from_list([{1, 1}, {2, 4}]),
    ?assertEqual(V2, set(3, 3, V1)),
    ?assertEqual(V3, set(2, 4, V1)).

vectorclock_setall_test() ->
    V1 = from_list([{1, 5}, {8, 4}, {3, 5}, {9, 6}]),
    V2 = from_list([{1, 7}, {8, 7}, {3, 7}, {9, 7}]),
    ?assertEqual(V2, set_all(7, V1)).

vectorclock_minclock_test() ->
    V1 = from_list([{1, 5}, {8, 4}, {3, 5}, {9, 6}]),
    ?assertEqual(4, min_clock(V1, [1, 8, 3, 9])),
    ?assertEqual(0, min_clock(V1, [])),
    ?assertEqual(0, min_clock(V1, [1, 8, 3, 9, 10])).

vectorclock_size_test() ->
    V1 = from_list([{1, 5}, {8, 4}, {3, 5}, {9, 6}]),
    V2 = new(),
    ?assertEqual(4, vectorclock:size(V1)),
    ?assertEqual(0, vectorclock:size(V2)).

vectorclock_update_test() ->
    V1 = from_list([{1, 5}, {8, 4}, {3, 5}, {9, 6}]),
    V2 = from_list([{1, 5}, {8, 8}, {3, 5}, {9, 6}]),
    ?assertEqual(V2, update_with(8, fun (X) -> X * 2 end, 0, V1)).

vectorclock_fold_test() ->
    V1 = from_list([{1, 5}, {8, 4}, {3, 5}, {9, 6}]),
    ?assertEqual(20, fold(fun (_Node, X, Acc) -> X + Acc end, 0, V1)).

-endif.
