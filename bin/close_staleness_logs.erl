#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name close_staleness@127.0.0.1 -cookie antidote
-mode(compile).

%% This should be called like (e.g.): truncate_staleness_logs.erl 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'
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
%%            lists:foreach(fun (Node) -> rpc:call(Node, materializer_vnode, close_all_staleness_logs, []) end, Nodes),
            rpc:call(hd(Nodes), materializer_vnode, close_all_staleness_logs, []),
            io:format("~nSuccesfully sent  rpc:call(Node, materializer_vnode, close_all_staleness_logs, []) end, Nodes) to node: ~w~n", [hd(Nodes)])
    end.

usage() ->
    io:format("This should be called like (e.g.): close_all_staleness_logs.erl 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'"),
    halt(1).

-include_lib("eunit/include/eunit.hrl").