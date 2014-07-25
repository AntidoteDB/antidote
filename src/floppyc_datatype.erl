-module(floppyc_datatype).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(QC_OUT(P), eqc:on_output(fun(Fmt, Args) -> io:format(user, Fmt, Args) end, P)).
-compile(export_all).
-endif.


-define(MODULES, [floppyc_counter, floppyc_set]).

-export([module_for_type/1,
         module_for_term/1]).

-export_type([datatype/0, update/0]).

-type maybe(T) :: T | undefined.
-type datatype() :: term().
-type typename() :: atom().
-type update() :: [term()].

%% Constructs a new container for the type with the specified
%% value and key. This should only be used internally by the client code.
-callback new(Key::binary(), Value::term()) -> datatype().

%% Returns the original, unmodified value of the type. This does
%% not include the application of any locally-queued operations.
-callback value(datatype()) -> term().

%% Returns the local value of the object.
-callback dirty_value(datatype()) -> term().

%% Returns the message to get an object of the type of this container.
-callback message_for_get(binary()) -> term().


%% Extracts the list of operations to be append to the object's log.
%% 'undefined' should be returned if the type is unmodified.
-callback to_ops(datatype()) -> update().

%% Determines whether the given term is the type managed by the
%% container module.
-callback is_type(datatype()) -> boolean().

%% Determines the symbolic name of the container's type, e.g.
%% set, map, counter.
-callback type() -> typename().

%% Returns the module that is a container for the given abstract
%% type.
-spec module_for_type(Type::atom()) -> module().
module_for_type(floppy_set) -> floppyc_set;
module_for_type(counter)  -> floppyc_counter;
module_for_type(flag)     -> floppyc_flag;
module_for_type(register) -> floppyc_register;
module_for_type(map)      -> floppyc_map.

%% @doc Returns the appropriate container module for the given term,
%% if possible.
-spec module_for_term(datatype()) -> maybe(module()).
module_for_term(T) ->
    lists:foldl(fun(Mod, undefined) ->
                        case Mod:is_type(T) of
                            true -> Mod;
                            false -> undefined
                        end;
                   (_, Mod) ->
                        Mod
                end, undefined, ?MODULES).
