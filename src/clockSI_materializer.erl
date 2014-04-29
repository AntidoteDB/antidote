-module(clockSI_materializer).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_snapshot/1,
	 update_snapshot/4,
	 materialize/3]).

create_snapshot(Type) ->
    Type:new().

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
materialize(Type, Snapshot_time, Ops) ->
    Init=create_snapshot(Type),
    Snapshot=materializer:update_snapshot(Type, Init, Snapshot_time, Ops),
    Type:value(Snapshot).
    
    length_test() -> ?assert(length([1,2,3]) =:= 3).
   % length_bad_test() -> ?assert(length([1,2,3, 4]) =:= 3).
  % update_snapshot_wrong_test() -> fun () -> ?assert(true).
  % update_snapshot_ok_test() -> fun () -> ?assert(false).
   % update_snapshot(test, test, []).
