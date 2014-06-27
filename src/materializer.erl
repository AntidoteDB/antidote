-module(materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
   update_snapshot/4]).

%% @doc Creates an empty CRDT
-spec create_snapshot(Type::atom()) -> term().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies all the operations of a list of log entriesto a CRDT.
-spec update_snapshot(Key::term(), Type::atom(), Snapshot::term(), Ops::list()) -> term().
update_snapshot(_, _, Snapshot, []) ->
    Snapshot;
update_snapshot(Key, Type, Snapshot, [LogEntry|Rest]) ->
	%case Op#log_record.op_type of
	%update ->
		%case Op#log_record.op_payload of
	case LogEntry of
	{_, Operation}->
		Payload=Operation#operation.payload,
		Key=Payload#payload.key, 
		OpParam=Payload#payload.op_param, 
		Actor=Payload#payload.actor,
		lager:info("OpParam: ~w, Actor: ~w and Snapshot: ~w",
				   [OpParam, Actor, Snapshot]),
		{ok, NewSnapshot} = Type:update(OpParam, Actor, Snapshot),
		update_snapshot(Key, Type, NewSnapshot, Rest);
	_->
		lager:info("Unexpected log record: ~w, Actor: ~w and Snapshot: ~w",[LogEntry]),
		{error, unexpected_format, LogEntry}
	end.
	
-ifdef(TEST).

%% @doc Testing gcounter with update log
materializer_gcounter_withlog_test() ->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Ops = [{1,#operation{payload = #payload{key=key, op_param=increment, actor=actor1}}}, 
    {2,#operation{payload =#payload{key=key, op_param=increment, actor=actor2}}}, 
    {3,#operation{payload =#payload{key=key, op_param=increment, actor=actor3}}}, 
    {4,#operation{payload =#payload{key=key, op_param={increment,3}, actor=actor4}}}],
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
    Ops = [{1,#operation{payload =#payload{key=key, op_param=decrement, actor=actor1}}}],
    ?assertException(error, function_clause, update_snapshot(key, riak_dt_gcounter, GCounter, Ops)).

-endif.
