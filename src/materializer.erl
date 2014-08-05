-module(materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
   update_snapshot/3]).

%% @doc Creates an empty CRDT
-spec create_snapshot(Type::atom()) -> term().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies all the operations of a list to a CRDT.
-spec update_snapshot(Type::atom(), Snapshot::term(), [op]) -> term().
update_snapshot(_, Snapshot, []) ->
    Snapshot;
update_snapshot(Type, Snapshot, [Op|Rest]) ->
    {_, #operation{payload={OpParam, Actor}}} = Op,
    lager:info("OpParam: ~w, Actor: ~w and Snapshot: ~w~n",
               [OpParam, Actor, Snapshot]),
    {ok, NewSnapshot} = Type:update(OpParam, Actor, Snapshot),
    update_snapshot(Type, NewSnapshot, Rest).

-ifdef(TEST).

%% @doc Testing gcounter with update log
materializer_gcounter_withlog_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [{1,#operation{payload={increment, actor1}}},{2,#operation{payload={increment, actor2}}},{3,#operation{payload={{increment, 3}, actor1}}},{4,#operation{payload={increment, actor3}}}],

    GCounter2 = update_snapshot(riak_dt_gcounter, GCounter, Ops),
    ?assertEqual(6,riak_dt_gcounter:value(GCounter2)).

%% @doc Testing gcounter with empty update log
materializer_gcounter_emptylog_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [],
    GCounter2 = update_snapshot(riak_dt_gcounter, GCounter, Ops),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter2)).

%% @doc Testing non-existing crdt
materializer_error_nocreate_test() ->
    ?assertException(error, undef, create_snapshot(bla)).

%% @doc Testing crdt with invalid update operation
materializer_error_invalidupdate_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [{1,#operation{payload={decrement, actor1}}}],
    ?assertException(error, function_clause, update_snapshot(riak_dt_gcounter, GCounter, Ops)).

-endif.
