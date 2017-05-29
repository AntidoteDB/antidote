#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name start_bg_proc@127.0.0.1 -cookie antidote
-mode(compile).

%% This should be called like (e.g.): start_bg_processes.erl 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'
main(NodesListString) ->
    erlang:set_cookie(node(), antidote),
    Nodes =
        try
            lists:foldl(fun(NodeString, Acc) ->
                Node = list_to_atom(NodeString),
                lists:append([Node], Acc)
            end, [], NodesListString)
        catch
            _:_  ->
                bad_input_format
        end,
    case Nodes of
        bad_input_format ->
            usage();
        _->
            lists:foreach(fun (Node) -> rpc:call(Node, inter_dc_manager, start_bg_processes, [stable]) end, Nodes),
            io:format("~nSuccesfully sent  rpc:call(Node, inter_dc_manager, start_bg_processes, [stable]) end, Nodes) to nodes: ~w~n", [Nodes])
    end.

usage() ->
    io:format("This should be called like (e.g.): start_bg_processes.erl 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'"),
    halt(1).

-include_lib("eunit/include/eunit.hrl").