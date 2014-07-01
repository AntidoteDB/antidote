-module(clockSI_downstream).

-include("floppy.hrl").

-export([generate_downstream_op/1]).

generate_downstream_op(Update=#operation{payload = Payload}) ->                        
    Key = Payload#clocksi_payload.key,
    Type =  Payload#clocksi_payload.type,
    Op_param =  Payload#clocksi_payload.op_param,    
    Snapshot_time = Payload#clocksi_payload.snapshot_time,   
    case materializer_vnode:read(Key, Type, Snapshot_time) of
        {ok, Snapshot} ->
            {Op, Actor} = Op_param,
            {ok, Newstate} = Type:update(Op, Actor, Snapshot),
            lager:info("NewState => ~p ~n", [Newstate]),
            Downstream_op = Update#operation{payload = Payload#clocksi_payload{op_param={merge, Newstate}}},
            lager:info("Downstream Op = ~p ~n ", [Downstream_op]),
            {ok, Downstream_op};
        {error, Reason} -> 
            lager:info("Error ~p",[Reason]),
            {error, Reason};
        Other -> 
            lager:info("Other Error ~p",[Other]),
            {error, Other}
    end.
