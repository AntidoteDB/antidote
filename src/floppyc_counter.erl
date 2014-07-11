-module(floppyc_counter).

-include_lib("riak_pb/include/floppy_pb.hrl").

-behaviour(floppyc_datatype).

-export([new/2,
         message_for_get/1,
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
          increment :: integer()
         }).

-export_type([counter/0]).
-opaque counter() :: #counter{}.

-spec new(Key::binary(), Value::term()) -> counter().
new(Key, Value) ->
    #counter{key=Key, value=Value, increment=0}.

-spec value(counter()) -> integer().
value(#counter{value=Value}) ->
    Value.

-spec dirty_value(counter()) -> integer().
dirty_value(#counter{value=Value, increment=Increment}) ->
    Value + Increment.

%% @doc Increments the counter by 1.
-spec increment(counter()) -> counter().
increment(Counter) ->
    increment(1, Counter).

increment(Amount, #counter{increment=Value}=Counter) when is_integer(Amount) ->
    Counter#counter{increment=Value+Amount}.

%% @doc Decrements the counter by 1.
-spec decrement(counter()) -> counter().
decrement(Counter) ->
    increment(-1, Counter).

%% @doc Decrements the counter by the passed amount.
-spec decrement(integer(), counter()) -> counter().
decrement(Amount, #counter{increment=Value}=Counter) ->
   Counter#counter{increment=Value-Amount}.

%% @doc Determines whether the passed term is a counter container.
-spec is_type(term()) -> boolean().
is_type(T) ->
    is_record(T, counter).

%% @doc Returns the symbolic name of this container.
-spec type() -> atom().
type() -> counter.

to_ops(#counter{key=_Key, increment=0}) -> undefined;

to_ops(#counter{key=Key, increment=Amount}) when Amount < 0 ->
    [#fpbdecrementreq{key=Key, amount=Amount}];

to_ops(#counter{key=Key, increment=Amount}) ->
    [#fpbincrementreq{key=Key, amount=Amount}].

message_for_get(Key) -> #fpbgetcounterreq{key=Key}.

