-module(materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
   update_snapshot/4]).

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies all the operations of key from a list of log entries to a CRDT.
-spec update_snapshot(key(), type(), snapshot(), [op]) -> snapshot() | {error,unexpected_format,op()}.
update_snapshot(_, _, Snapshot, []) ->
    Snapshot;
update_snapshot(Key, Type, Snapshot, [LogEntry|Rest]) ->
    case LogEntry of
        {_, Operation} ->
            Payload = Operation#operation.payload,
            NewSnapshot = case {Payload#payload.key, Payload#payload.type} of
                {Key, Type} ->
                    OpParam = Payload#payload.op_param,
                    Actor = Payload#payload.actor,
                    lager:info("OpParam: ~p, Actor: ~p and Snapshot: ~p",
                               [OpParam, Actor, Snapshot]),
                    {ok, Value} = Type:update(OpParam, Actor, Snapshot),
                    Value;
                _ ->
                    Snapshot
            end,
            update_snapshot(Key, Type, NewSnapshot, Rest);
        _ ->
            lager:info("Unexpected log record: ~p, Actor: ~p and Snapshot: ~p",
                       [LogEntry]),
            {error, unexpected_format, LogEntry}
    end.

-ifdef(TEST).

%% @doc Testing gcounter with update log
materializer_gcounter_withlog_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [{1,#operation{payload = #payload{key=key,
                                            type=riak_dt_gcounter, op_param=increment, actor=actor1}}}, 
    {2,#operation{payload =#payload{key=key, type=riak_dt_gcounter, op_param=increment, actor=actor2}}}, 
    {3,#operation{payload =#payload{key=key, type=riak_dt_gcounter, op_param=increment, actor=actor3}}}, 
    {4,#operation{payload =#payload{key=key, type=riak_dt_gcounter, op_param={increment,3}, actor=actor4}}}],
    GCounter2 = update_snapshot(key, riak_dt_gcounter, GCounter, Ops),
    ?assertEqual(6,riak_dt_gcounter:value(GCounter2)).

%% @doc Testing gcounter with empty update log
materializer_gcounter_emptylog_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [],
    GCounter2 = update_snapshot(key, riak_dt_gcounter, GCounter, Ops),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter2)).

%% @doc Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, undef, create_snapshot(bla)).

%% @doc Testing crdt with invalid update operation
materializer_error_invalidupdate_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [{1,#operation{payload =#payload{key=key, type=riak_dt_gcounter, op_param=decrement, actor=actor1}}}],
    ?assertException(error, function_clause, update_snapshot(key, riak_dt_gcounter, GCounter, Ops)).

-endif.
