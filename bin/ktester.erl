#! /usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name kstable-tester@127.0.0.1 -cookie antidote

-module(ktester).
-mode(compile).

-define(dbug, io:format("dbug ~p ~p...~n", [?FUNCTION_NAME, ?LINE])).
-define(tab, tabtab).
-define(node, 'antidote@127.0.0.1').


main(_A) ->
    {ok, connected} = antidote_connect(?node),
    DC1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC2 = {'antidote2@127.0.0.1', {1390, 186897, 698677}},
    DC3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    A = rpc:call(?node, vectorclock, new, []),
    AA = rpc:call(?node, vectorclock, set_clock_of_dc, [DC1, 1, A]),
    AAA = rpc:call(?node, vectorclock, set_clock_of_dc, [DC2, 4, AA]),
    AAAA = rpc:call(?node, vectorclock, set_clock_of_dc, [DC3, 0, AAA]),
    VC = rpc:call(?node, vectorclock, set_clock_of_dc, [DC4, 3, AAAA]),
    ets:new(?tab, [set, named_table]),
    ets:insert(?tab, {DC1, VC}),
    %io:format("~p ~n", ets:tab2list(?tab)),
    io:format("~p ~n", [ets:lookup(?tab, DC1)]),
    ok.

%% Connects
-spec antidote_connect(atom()) -> ok | {error, node_offline}.
antidote_connect(Node) ->
    case net_kernel:start([Node, longnames]) of
        {ok, _} ->
            ok;
        {error, {already_started,_}} ->
            ok;
        {error, {{already_started, _},_}} ->
            ok;
        {error, R} ->
            io:format("Error connecting ~p~n", [R]),
            halt(1)
    end,
    %% Hardcoded for simplicity sake
    erlang:set_cookie(Node, antidote),
    %% Redundant connection verification
    case net_adm:ping(Node) of
        pong -> %% We're good
            ok;
        Other -> %% Offline
            io:format("Can't connect to node ~p (return: ~p)! Aborting.~n",
                [Node, Other]),
            halt(1)
    end.
