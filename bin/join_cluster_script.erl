#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name setup@127.0.0.1 -cookie antidote -mnesia debug verbose
-mode(compile).

main([_,_]) ->
%%    io:format("Number of nodes are ~w", [Nodes]),
%%    io:format("Host name is  ~w", [HostName]),
%%    NumNodes=5,
%%    HostName="mbp-alejandro.rsr.lip6.fr",
%%%%    NumNodes = list_to_integer(Nodes),
%%    ListOfNodes = lists:foldl(fun(I, Acc)->
%%        lists:append(Acc, ["antidote" ++ integer_to_list(I) ++ "@" ++ HostName]) end,
%%        [], lists:seq(1,NumNodes)),

%%    FirstNode = hd(ListOfNodes),
    Node1 = 'antidote1@mbp-alejandro.rsr.lip6.fr',
    Node2 = 'antidote2@mbp-alejandro.rsr.lip6.fr',
    Node3 = 'antidote3@mbp-alejandro.rsr.lip6.fr',
    Node4 = 'antidote4@mbp-alejandro.rsr.lip6.fr',
    Node5 = 'antidote5@mbp-alejandro.rsr.lip6.fr',
    Nodes = [Node1, Node2, Node3, Node4, Node5],
    rpc:call(Node1, test_utils,join_cluster,
        Nodes),
    io:format("Nodes are ~w", [Nodes]);
main(_) ->
    usage().

usage() ->
    io:format("usage: client_id fmk_node\n"),
    halt(1).

