-module(clockSI_materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_snapshot/3,
        update_snapshot_eager/3]).

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
%%		Ops: The list of operations to apply in causal order
%%	Output: The CRDT after appliying the operations 
-spec update_snapshot(Type::atom(), Snapshot::term(), SnapshotTime::non_neg_integer(),Ops::list()) -> term().
update_snapshot(_, Snapshot, _Snapshot_time, []) ->
    Snapshot;
update_snapshot(Type, Snapshot, Snapshot_time, [Op|Rest]) ->
    Payload = Op#operation.payload,
    Type = Payload#clocksi_payload.type,
    case is_op_in_snapshot(Payload#clocksi_payload.commit_time, Snapshot_time) of
	true -> 	    
	    case Payload#clocksi_payload.op_param of
		{merge, State} -> 
		    {ok, New_snapshot} = Type:merge(Snapshot, State),
		    update_snapshot(Type, New_snapshot, Snapshot_time, Rest);
		{Update, Actor} ->
		    {ok, New_snapshot}= Type:update(Update, Actor, Snapshot),
		    update_snapshot(Type, New_snapshot, Snapshot_time, Rest);
		Other -> lager:info("OP is ~p", [Other])
			
	    end;
	    %update_snapshot(Type, New_snapshot, Snapshot_time, Rest);
	false ->
	    update_snapshot(Type, Snapshot, Snapshot_time, Rest)
    end.

%% @doc Check whether an udpate is included in a snapshot
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%      Outptut: true or false
-spec is_op_in_snapshot({Dc::term(),Commit_time::non_neg_integer()}, SnapshotTime::orddict:orddict()) -> term().
is_op_in_snapshot({Dc, Commit_time}, Snapshot_time) ->
    case orddict:find(Dc, Snapshot_time) of
	{ok, Ts} ->
	    Commit_time =< Ts;
	error  ->
	    false %Snaphot hasnot seen any udpate from this DC
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
-spec get_snapshot(Type::atom(), SnapshotTime::non_neg_integer(),Ops::list()) -> term().
get_snapshot(Type, Snapshot_time, Ops) ->
    Init=create_snapshot(Type),
    update_snapshot(Type, Init, Snapshot_time, Ops).

-ifdef(TEST).
materializer_clockSI_test()->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Op1 = #clocksi_payload{key = abc, type = riak_dt_gcounter, op_param = {{increment,2}, actor1}, snapshot_time = [{1,0}], commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = riak_dt_gcounter, op_param = {{increment,1}, actor1}, snapshot_time = [{1,0}], commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = riak_dt_gcounter, op_param = {{increment,1}, actor1}, snapshot_time = [{1,0}], commit_time = {1, 3}, txid = 3},
    Op4 = #clocksi_payload{key = abc, type = riak_dt_gcounter, op_param = {{increment,2}, actor1}, snapshot_time = [{1,2}], commit_time = {1, 4}, txid = 4},

    Ops = [#operation{op_number = 1,payload =Op1},#operation{op_number = 2,payload = Op2},#operation{op_number = 3,payload = Op3},#operation{op_number = 4,payload = Op4}],
    GCounter2 = update_snapshot(riak_dt_gcounter, GCounter, orddict:from_list([{1,3}]), Ops),
    ?assertEqual(4,riak_dt_gcounter:value(GCounter2)),
    ?assertEqual(6,riak_dt_gcounter:value(get_snapshot(riak_dt_gcounter,orddict:from_list([{1,4}]),Ops))).

-endif.
