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
    
    
    update_snapshot_test() -> fun () -> ?assert(1 + 1 =:= 2) end.
   % update_snapshot(test, test, []).
