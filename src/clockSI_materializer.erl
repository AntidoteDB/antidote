-module(clockSI_materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([create_snapshot/1,
	 update_snapshot/4,
	 update_snapshot_eager/3,
	 materialize/3]).

%% @doc	Creates an empty CRDT
%%	Input:	Type: The type of CRDT to create
%%	Output: The newly created CRDT 
-spec create_snapshot(Type::atom()) -> term().
create_snapshot(Type) ->
    	Type:new().

%% @doc	Applies the operation of a list to a CRDT. Only the
%%	operations with smaller timestamp than the specified
%%	are considered. Newer ooperations are discarded.
%%	Input:	Type: The type of CRDT to create
%%		Snapshot: Current state of the CRDT
%%		SnapshotTime: Threshold for the operations to be applied.
%%		Ops: The list of operations to apply
%%	Output: The CRDT after appliying the operations 
-spec update_snapshot(Type::atom(), Snapshot::term(), SnapshotTime::non_neg_integer(),Ops::list()) -> term().
update_snapshot(_, Snapshot, _Snapshot_time, []) ->
    Snapshot;
update_snapshot(Type, Snapshot, Snapshot_time, [Op|Rest]) ->
    {_,#operation{payload=Payload}}=Op,
    {OpParam, Actor, Commit_ts}=Payload,
    if Commit_ts =< Snapshot_time ->
    	io:format("OpParam: ~w, Actor: ~w , Commit_TS: ~w and Snapshot: ~w~n",[OpParam, Actor, Commit_ts, Snapshot]),	
    	{ok, NewSnapshot}= Type:update(OpParam, Actor, Snapshot),
    	update_snapshot(Type, NewSnapshot, Snapshot_time, Rest);
    true ->
	update_snapshot(Type, Snapshot, Snapshot_time, Rest)
    end.

update_snapshot_eager(_, Snapshot, []) ->
    Snapshot;
update_snapshot_eager(Type, Snapshot, [Op|Rest]) ->
    {OpParam,Actor}=Op,
    {ok, NewSnapshot}= Type:update(OpParam, Actor, Snapshot),
    update_snapshot_eager(Type, NewSnapshot, Rest).

%% @doc materialize a CRDT from its logged operations
%%	- First creates an empty CRDT
%%	- Second apply the corresponding logged operations
%%	- Finally, transform the CRDT state into its value.
%%	Input:	Type: The type of the CRDT
%%		SnapshotTime: Threshold for the operations to be applied.
%%		Ops: The list of operations to apply
%%	Output: The value of the CRDT after appliying the operations 
-spec materialize(Type::atom(), SnapshotTime::non_neg_integer(),Ops::list()) -> term().
materialize(Type, Snapshot_time, Ops) ->
    Init=create_snapshot(Type),
    Snapshot=update_snapshot(Type, Init, Snapshot_time, Ops),
    Type:value(Snapshot).

-ifdef(TEST).
    materializer_clockSI_test()->
    	GCounter = create_snapshot(riak_dt_gcounter),
	?assertEqual(0,riak_dt_gcounter:value(GCounter)),
	Ops = [{1,#operation{payload={increment, actor1, 4}}},{2,#operation{payload={increment, actor2, 5}}},{3,#operation{payload={{increment, 3}, actor1, 9}}},{4,#operation{payload={increment, actor3, 2}}}],
	GCounter2 = update_snapshot(riak_dt_gcounter, GCounter, 7, Ops),
	?assertEqual(3,riak_dt_gcounter:value(GCounter2)),
	?assertEqual(6,materialize(riak_dt_gcounter,9,Ops)).
-endif.
