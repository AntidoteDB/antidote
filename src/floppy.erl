-module(floppy).
-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([append/2,
         read/2]).

%% Public API

append(Key, Op) ->
    lager:info("Append called!"),
    LogId = log_utilities:get_logid_from_key(Key),
    case floppy_rep_vnode:append(LogId, {Key, Op}) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

read(Key, _Type) ->
    LogId = log_utilities:get_logid_from_key(Key),
    case floppy_rep_vnode:read(LogId) of
        {ok, Ops} ->
            %Init=materializer:create_snapshot(Type),
            %Snapshot=materializer:update_snapshot(Key, Type, Init, Ops),
            %Type:value(Snapshot);
            Ops;
        {error, _} ->
            lager:info("Read failed!~n"),
            error
    end.
