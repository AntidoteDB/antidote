-module(floppy).

-include("floppy.hrl").

-export([append/2, read/2]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
append(Key, {OpParam, Actor}) ->
    Payload = #payload{key=Key, op_param=OpParam, actor=Actor},
    case floppy_rep_vnode:append(Key, Payload) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
read(Key, Type) ->
    case floppy_rep_vnode:read(Key) of
        {ok, Ops} ->
            Init = materializer:create_snapshot(Type),
            Snapshot = materializer:update_snapshot(Key, Type, Init, Ops),
            Type:value(Snapshot);
        {error, Reason} ->
            {error, Reason}
    end.
