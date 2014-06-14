-module(clockSI_downstream).

-include("floppy.hrl").

-export([generate_downstream_op/1]).

%% private methods
generate_downstream_op(Update=#operation{payload = Payload}) ->
    Key = Payload#clocksi_payload.key,
    Type =  Payload#clocksi_payload.type,
    Op_param =  Payload#clocksi_payload.op_param,    
    Snapshot_time = Payload#clocksi_payload.snapshot_time,   
    {ok, Ops} = floppy_rep_vnode:read(Key, Type),
                        lager:info("Downstream Generator reading key ~w, calling the materializer ~n", [Key]),	      
            ListofOps = [ Op || { _Key, Op } <- Ops ],  
            
    Snapshot = clockSI_materializer:get_snapshot(Type,Snapshot_time, ListofOps),
    lager:info("~p :Snapshot => ~p ~n", [?MODULE, Snapshot]), 
    {Op, Actor} = Op_param,
    {ok, Newstate} = Type:update(Op, Actor, Snapshot),
    lager:info("NewState => ~p ~n", [Newstate]),
    Downstream_op = Update#operation{payload = Payload#clocksi_payload{op_param={merge, Newstate}}},
    lager:info("Downstream Op = ~p ~n ", [Downstream_op]),
    Downstream_op.
    %% {ok, _OpId } = floppy_rep_vnode:append(Key, Downstream_op#operation.payload),
    %% {Dc_id, _Timestamp} = Payload#clocksi_payload.commit_time,
    %% %vectorclock:update_clock(Key, Dc_id, Timestamp),
    %% inter_dc_repl_vnode:propogate({Key, Downstream_op, Dc_id}),

	
    
