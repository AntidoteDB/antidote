%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(wait_init).

-export([wait_ready_nodes/1,
	 check_ready/1
        ]).

%% @doc This function takes a list of pysical nodes connected to the an
%% instance of the antidote distributed system.  For each of the phyisical nodes
%% it checks if all of the vnodes have been initialized, meaning ets tables
%% and gen_servers serving read have been started.
%% Returns true if all vnodes are initialized for all phyisical nodes,
%% false otherwise
wait_ready_nodes([]) ->
    true;
wait_ready_nodes([Node|Rest]) ->
    case check_ready(Node) of
	true ->
	    wait_ready_nodes(Rest);
	false ->
	    false
    end.

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a sinlge physical node instead of a list
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
