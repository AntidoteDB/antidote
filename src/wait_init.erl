-module(wait_init).

-export([wait_ready_nodes/1,
	 check_ready/1]).


wait_ready_nodes([]) ->
    true;
wait_ready_nodes([Node|Rest]) ->
    case check_ready(Node) of
	true ->
	    wait_ready_nodes(Rest);
	false ->
	    false
    end.

check_ready(Node) ->
    lager:info("Checking if node ~w is ready ~n", [Node]),
    case rpc:call(Node,clocksi_vnode,check_tables_ready,[]) of
	true ->
	    case rpc:call(Node,clocksi_readitem_fsm,check_servers_ready,[]) of
		true ->
		    case rpc:call(Node,materializer_vnode,check_tables_ready,[]) of
			true ->
			    lager:info("Node ~w is ready! ~n", [Node]),
			    true;
			false ->
			    lager:info("Node ~w is not ready ~n", [Node]),
			    false
		    end;
		false ->
		    lager:info("Checking if node ~w is ready ~n", [Node]),
		    false
	    end;
	false ->
	    lager:info("Checking if node ~w is ready ~n", [Node]),
	    false
    end.

    
