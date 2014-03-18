-module(materializer).
-export([create_snapshot/1,
	 update_snapshot/3]).

create_snapshot(Type) ->
    Type:new().

update_snapshot(_, Snapshot, []) ->
    Snapshot;
update_snapshot(Type, Snapshot, [Op|Rest]) ->
    {OpParam, Actor} = Op,
    NewSnapshot= Type:update(OpParam, Actor, Snapshot),
    update_snapshot(Type, NewSnapshot, Rest).
