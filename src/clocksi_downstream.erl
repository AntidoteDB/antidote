-module(clocksi_downstream).

-include("floppy.hrl").

-export([generate_downstream_op/1]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(#clocksi_payload{}) ->
    {ok, #clocksi_payload{}} | {error, atom()}.
generate_downstream_op(Update) ->
    Key = Update#clocksi_payload.key,
    Type =  Update#clocksi_payload.type,
    {Op, Actor} =  Update#clocksi_payload.op_param,
    SnapshotTime = Update#clocksi_payload.snapshot_time,
    case materializer_vnode:read(Key, Type, SnapshotTime) of
        {ok, Snapshot} ->
            {ok, NewState} = Type:update(Op, Actor, Snapshot),
            DownstreamOp = Update#clocksi_payload{op_param={merge, NewState}},
            lager:info("NewState: ~p DownstreamOp: ~p",
                       [NewState, DownstreamOp]),
            {ok, DownstreamOp};
        {error, Reason} ->
            lager:info("Error: ~p", [Reason]),
            {error, Reason}
    end.
