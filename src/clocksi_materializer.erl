-module(clocksi_materializer).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_snapshot/3,
         get_snapshot/4,
         update_snapshot_eager/3]).

%% @doc Creates an empty CRDT
%%      Input: Type: The type of CRDT to create
%%      Output: The newly created CRDT
-spec create_snapshot(Type::atom()) -> term().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies the operation of a list to a CRDT. Only the
%%      operations with smaller timestamp than the specified
%%      are considered. Newer ooperations are discarded.
%%      Input:	Type: The type of CRDT to create
%%      Snapshot: Current state of the CRDT
%%      SnapshotTime: Threshold for the operations to be applied.
%%      Ops: The list of operations to apply in causal order
%%      Output: The CRDT after appliying the operations
-spec update_snapshot(Type::atom(), Snapshot::term(),
                      SnapshotTime::vectorclock:vectorclock(),
                      Ops::[#clocksi_payload{}], TxId::term()) -> term().
update_snapshot(_, Snapshot, _Snapshot_time, [], _TxId) ->
    {ok, Snapshot};
update_snapshot(Type, Snapshot, Snapshot_time, [Op|Rest], TxId) ->
    Type = Op#clocksi_payload.type,
    case (is_op_in_snapshot(Op#clocksi_payload.commit_time, Snapshot_time)
          or (TxId =:= Op#clocksi_payload.txid)) of
        true ->
            case Op#clocksi_payload.op_param of
                {merge, State} ->
                    New_snapshot = Type:merge(Snapshot, State),
                    update_snapshot(Type, New_snapshot,
                                    Snapshot_time, Rest, TxId);
                {Update, Actor} ->
                    case Type:update(Update, Actor, Snapshot) of
                        {ok, New_snapshot} ->
                            update_snapshot(Type, New_snapshot,
                                            Snapshot_time, Rest, TxId);
                        {error, Reason} -> {error, Reason};
                        Other -> {error, Other}
                    end;
                Other -> lager:info("OP is ~p", [Other])
            end;
        false ->
            update_snapshot(Type, Snapshot, Snapshot_time, Rest, TxId)
    end.

%% @doc Check whether an udpate is included in a snapshot
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this update at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%      Outptut: true or false
-spec is_op_in_snapshot({Dc::term(),Commit_time::non_neg_integer()},
                        SnapshotTime::vectorclock:vectorclock()) -> boolean().
is_op_in_snapshot({Dc, Commit_time}, Snapshot_time) ->
    case vectorclock:get_clock_of_dc(Dc, Snapshot_time) of
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
-spec get_snapshot(Type::atom(), SnapshotTime::vectorclock:vectorclock(),
                   Ops::[#clocksi_payload{}]) -> term().
get_snapshot(Type, Snapshot_time, Ops) ->
    Init=create_snapshot(Type),
    update_snapshot(Type, Init, Snapshot_time, Ops, ignore).

-spec get_snapshot(Type::atom(), SnapshotTime::vectorclock:vectorclock(),
                   Ops::[#clocksi_payload{}], TxId :: #tx_id{}) -> term().
get_snapshot(Type,Snapshot_time,Ops,TxId) ->
    Init=create_snapshot(Type),
    update_snapshot(Type, Init, Snapshot_time, Ops, TxId).

-ifdef(TEST).
materializer_clockSI_test()->
    GCounter = create_snapshot(riak_dt_gcounter),
    ?assertEqual(0,riak_dt_gcounter:value(GCounter)),
    Op1 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,2}, actor1},
                           commit_time = {1, 1}, txid = 1},
    Op2 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {1, 2}, txid = 2},
    Op3 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,1}, actor1},
                           commit_time = {1, 3}, txid = 3},
    Op4 = #clocksi_payload{key = abc, type = riak_dt_gcounter,
                           op_param = {{increment,2}, actor1},
                           commit_time = {1, 4}, txid = 4},

    Ops = [Op1,Op2,Op3,Op4],
    {ok, GCounter2} = update_snapshot(riak_dt_gcounter,
                                      GCounter, vectorclock:from_list([{1,3}]),
                                      Ops, ignore),
    ?assertEqual(4,riak_dt_gcounter:value(GCounter2)),
    {ok, Gcounter3} = get_snapshot(riak_dt_gcounter,
                                   vectorclock:from_list([{1,4}]),Ops),
    ?assertEqual(6,riak_dt_gcounter:value(Gcounter3)).

-endif.
