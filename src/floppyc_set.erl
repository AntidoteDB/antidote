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
         remove/2
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

new(Key, Set) ->
    #floppy_set{key=Key, set=Set, adds=sets:new(), rems=sets:new()}.

-spec value(floppy_set()) -> set().
value(#floppy_set{set=Set}) -> Set.

-spec dirty_value(floppy_set()) -> set().
dirty_value(#floppy_set{set=Set, adds=Adds}) ->
    sets:union(Set,Adds).

%% @doc Adds an element to the local set.
add(Elem, #floppy_set{set=Local}) ->
    #floppy_set{adds=sets:add_element(Elem,Local)}.

remove(Elem, #floppy_set{set=Set, adds=Adds, rems=Rems}) ->
    case sets:is_element(Elem, Adds) of
        true ->  #floppy_set{set=Set, adds=sets:del_element(Elem,Adds), rems=Rems};
        false ->  #floppy_set{set=Set, adds=Adds, rems=sets:add_element(Elem,Rems)}
    end.

%% @doc Determines whether the passed term is a counter container.
is_type(T) ->
    is_record(T, floppy_set).


%% @doc Returns the symbolic name of this container.
type() -> floppy_set.

to_ops(#floppy_set{key=Key, adds=Adds, rems=Rems}) -> 
    case sets:size(Adds) =:= 0 andalso sets:size(Rems) =:= 0 of
        true -> undefined;
        false -> 
            #fpbsetupdatereq{key=Key, adds=sets:to_list(Adds), rems=sets:to_list(Rems)}
    end.

message_for_get(Key) -> #fpbgetcounterreq{key=Key}.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
add_op_test() ->
    New = floppyc_set:new(dumb_key),
    EmptySet = sets:size(floppyc_set:value(New)),
    [?_assert(EmptySet =:= 0)].

-endif.

