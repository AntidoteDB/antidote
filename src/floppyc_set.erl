-module(floppyc_set).

-include_lib("riak_pb/include/floppy_pb.hrl").

-behaviour(floppyc_datatype).

-export([new/1,
         new/2,
         message_for_get/1,
         value/1,
         to_ops/1,
         is_type/1,
         type/0,
         dirty_value/1
        ]).

-export([add/2,
         remove/2,
         contains/2
        ]).

-record(floppy_set, {
          key :: binary(),
          set :: sets:sets(),
          adds :: sets:sets(),
          rems :: sets:sets()
         }).

-export_type([floppy_set/0]).
-opaque floppy_set() :: #floppy_set{}.

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.



new(Key) ->
    #floppy_set{key=Key, set=sets:new(), adds=sets:new(), rems=sets:new()}.

new(Key,[]) ->
    #floppy_set{key=Key, set=sets:new(), adds=sets:new(), rems=sets:new()};

new(Key, [_H | _] = List) ->
    Set = lists:foldl(fun(E,S) ->
                              sets:add_element(E,S)
                      end,sets:new(),List),
    #floppy_set{key=Key, set=Set, adds=sets:new(), rems=sets:new()};

new(Key, Set) ->
    #floppy_set{key=Key, set=Set, adds=sets:new(), rems=sets:new()}.

value(#floppy_set{set=Set}) -> Set.

dirty_value(#floppy_set{set=Set, adds=Adds}) ->
    sets:union(Set,Adds).

%% @doc Adds an element to the local set.
add(Elem, #floppy_set{set=Set, adds=Adds}=Fset) ->
    case sets:is_element(Elem, Set) of
        false -> Fset#floppy_set{adds=sets:add_element(Elem,Adds)};
        true -> Fset
    end.

%% @doc Removes an element to the local set.
remove(Elem, #floppy_set{set=Set, adds=Adds, rems=Rems}=Fset) ->
    case sets:is_element(Elem, Adds) of
        true ->  Fset#floppy_set{adds=sets:del_element(Elem,Adds)};
        false -> 
            case sets:is_element(Elem, Set) of
                true -> Fset#floppy_set{rems=sets:add_element(Elem,Rems)};
                false -> Fset
            end
    end.

%% @doc Checks if the set contains the given element.
contains(Elem, #floppy_set{set=Set, adds=Adds, rems=Rems}) ->
    case sets:is_element(Elem, Adds) of
        true -> true;
        false ->
            case sets:is_element(Elem, Set) of
                true -> sets:is_element(Elem, Rems);
                false -> false
            end
    end.

is_type(T) ->
    is_record(T, floppy_set).

type() -> riak_dt_orset.

to_ops(#floppy_set{key=Key, adds=Adds, rems=Rems}=Ops) -> 
    io:format("to ops ~p ~n",[Ops]),
    case sets:size(Adds) =:= 0 andalso sets:size(Rems) =:= 0 of
        true -> undefined;
        false -> 
            [#fpbsetupdatereq{key=Key, adds=sets:to_list(Adds), rems=sets:to_list(Rems)}]
    end.

message_for_get(Key) -> #fpbgetsetreq{key=Key}.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
add_op_test() ->
    New = floppyc_set:new(dumb_key),
    EmptySet = sets:size(floppyc_set:dirty_value(New)),
    OneElement = floppyc_set:add(atom1,New),
    Size1Set = sets:size(floppyc_set:dirty_value(OneElement)),
    [?_assert(EmptySet =:= 0),
     ?_assert(Size1Set =:= 1)].

add_op_existing_set_test() ->
    New = floppyc_set:new(dumb_key,[elem1,elem2,elem3]),
    ThreeElemSet = sets:size(floppyc_set:dirty_value(New)),
    AddElem = floppyc_set:add(elem4,New),
    S1 = floppyc_set:remove(elem4,AddElem),
    S2 = floppyc_set:remove(elem2,S1),
    TwoElemSet = sets:size(floppyc_set:dirty_value(S2)),
    [?_assert(ThreeElemSet =:= 3),
     ?_assert(TwoElemSet =:= 2)].
-endif.

