#! /usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name kstable-tester@127.0.0.1 -cookie antidote

-module(ktester).
-mode(compile).

-define(dbug, io:format("dbug ~p ~p...~n", [?FUNCTION_NAME, ?LINE])).

main(_A) ->
    {ok, connected} = antidote_connect('antidote1@127.0.0.1'),
    io:format("Connected!\nBye!\n"),
    {ok, done}.

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
            io:format("Connected: ~p~n", [Node]),
            {ok, connected};
        Other -> %% Offline
            io:format("Can't connect to node ~p (return: ~p)! Aborting.~n",
                [Node, Other]),
            {error, node_offline},
            halt(1)
    end.
