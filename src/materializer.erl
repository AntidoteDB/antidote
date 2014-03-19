-module(materializer).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_snapshot/1,
	 update_snapshot/3]).

create_snapshot(Type) ->
    Type:new().

update_snapshot(_, Snapshot, []) ->
    Snapshot;
update_snapshot(Type, Snapshot, [Op|Rest]) ->
    {_,#operation{payload=Payload}}=Op,
    {OpParam, Actor}=Payload,
    io:format("OpParam: ~w, Actor: ~w and Snapshot: ~w~n",[OpParam, Actor, Snapshot]),	
    {ok, NewSnapshot}= Type:update(OpParam, Actor, Snapshot),
    update_snapshot(Type, NewSnapshot, Rest).
    
    length_test() -> ?assert(length([1,2,3]) =:= 3).
    length_bad_test() -> ?assert(length([1,2,3, 4]) =:= 3).
  % update_snapshot_wrong_test() -> fun () -> ?assert(true).
  % update_snapshot_ok_test() -> fun () -> ?assert(false).
   % update_snapshot(test, test, []).
