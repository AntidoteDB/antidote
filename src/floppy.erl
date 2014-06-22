-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([append/2,
         read/2]).

-type key() :: term().
-type op()  :: term().
-type crdt() :: term().
-type val() :: term().
-type reason() :: term().

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT object stored at some key.
%% TODO What is returned in case of success?!
-spec append(key(), op()) -> {ok, term()} | {error, timeout}.
append(Key, Op) ->
    lager:info("Append called!"),
    floppy_rep_vnode:append(Key, Op).


%% @doc The read/2 function returns the current value for the CRDT object stored at some key.
%% TODO Which state is exactly returned? Related to some snapshot? What is current?
-spec read(key(), crdt()) -> val() | {error,reason()}.
read(Key, Type) ->
    case floppy_rep_vnode:read(Key, Type) of
        {ok, Ops} ->
            Init=materializer:create_snapshot(Type),
            Snapshot=materializer:update_snapshot(Type, Init, Ops),
            Type:value(Snapshot);
        {error, Reason} ->
            lager:info("Read failed: ~w~n", Reason),
	    {error, Reason}
    end.
