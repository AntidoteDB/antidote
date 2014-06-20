-module(clockSI_downstream).

-include("floppy.hrl").

-export([generate_downstream_op/1]).

generate_downstream_op(Update=#operation{payload = Payload}) ->                        
    Key = Payload#clocksi_payload.key,
    Type =  Payload#clocksi_payload.type,
    Op_param =  Payload#clocksi_payload.op_param,    
    Snapshot_time = Payload#clocksi_payload.snapshot_time,   
    case floppy_rep_vnode:read(Key, Type) of
        {ok, Ops} when is_list(Ops) ->    
            lager:info("Downstream Generator reading key ~w ~p, calling the materializer ~n", [Key, Ops]),	      
            ListofOps = [ Op || { _Key, Op } <- Ops ],             
            {ok, Snapshot} = clockSI_materializer:get_snapshot(Type,Snapshot_time, ListofOps, Payload#clocksi_payload.txid),
            lager:info("~p :Snapshot => ~p ~n", [?MODULE, Snapshot]), 
            {Op, Actor} = Op_param,
            {ok, Newstate} = Type:update(Op, Actor, Snapshot),
            lager:info("NewState => ~p ~n", [Newstate]),
            Downstream_op = Update#operation{payload = Payload#clocksi_payload{op_param={merge, Newstate}}},
            lager:info("Downstream Op = ~p ~n ", [Downstream_op]),
            {ok, Downstream_op};
        {ok, Ops} ->
            {error, Ops};
        {error, Reason} -> 
            lager:info("Error ~p",[Reason]),
            {error, Reason};
        Other -> 
            lager:info("Other Error ~p",[Other]),
            {error, Other}
    end.

	
    
