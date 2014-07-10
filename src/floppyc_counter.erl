-module(floppyc_counter).

-include_lib("riak_pb/include/floppy_pb.hrl").

-behaviour(floppyc_datatype).

-export([new/2,
         value_op/1,
         value/1,
         to_ops/1,
         is_type/1,
         type/0,
         dirty_value/1
        ]).

-export([increment/1,
         increment/2, 
         decrement/1,
         decrement/2
        ]).

-record(counter, {
          key :: binary(),
          value :: integer(),
          increment :: integer(),
          decrement :: integer()
         }).

-export_type([counter/0]).
-opaque counter() :: #counter{}.

-spec new(Key::binary(), Value::term()) -> counter().
new(Key, Value) ->
    #counter{key=Key, value=Value, increment=0, decrement=0}.

-spec value(counter()) -> integer().
value(#counter{value=Value}) ->
    Value.

-spec dirty_value(counter()) -> integer().
dirty_value(#counter{value=Value, increment=Increment, decrement=Decrement}) ->
    Value + Increment - Decrement.

%% @doc Increments the counter by 1.
-spec increment(counter()) -> counter().
increment(Counter) ->
    increment(1, Counter).

%% @doc Increments the counter by the passed amount.
-spec increment(integer(), counter()) -> counter().
increment(Amount, #counter{increment=undefined}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Amount};
increment(Amount, #counter{increment=Incr}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Incr+Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(counter()) -> counter().
decrement(Counter) ->
    decrement(1, Counter).

%% @doc Decrements the counter by the passed amount.
-spec decrement(integer(), counter()) -> counter().
decrement(Amount, #counter{decrement=Dec}=Counter) ->
   Counter#counter{decrement=Dec+Amount}.

%% @doc Determines whether the passed term is a counter container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, counter).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> counter.

to_ops(#counter{key=Key, increment=Increment, decrement=0}) ->
    [#fpbincrementreq{key=Key, amount=Increment}];

to_ops(#counter{key=Key, increment=0, decrement=Decrement}) ->
    [#fpbdecrementreq{key=Key, amount=Decrement}];

to_ops(#counter{key=Key, increment=Increment, decrement=Decrement}) ->
    [#fpbincrementreq{key=Key, amount=Increment}, #fpbdecrementreq{key=Key, amount=Decrement}].

value_op(Key) -> #fpbgetcounterreq{key=Key}.

