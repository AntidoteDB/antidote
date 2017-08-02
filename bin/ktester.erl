#! /usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name kstable-tester@127.0.0.1 -cookie antidote

-module(ktester).
-mode(compile).

-define(dbug, io:format("dbug ~p ~p...~n", [?FUNCTION_NAME, ?LINE])).
-define(tab, tabtab).
-define(node, 'antidote@127.0.0.1').
-define(kstab, 2).

main(_A) ->
    antidote_connect(?node),
    ets:new(?tab, [set, named_table]),

    DC1_1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC1_2 = {'antidote2@127.0.0.1', {1390, 186897, 698677}},
    DC1_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC1_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC1 = [{DC1_1, 1}, {DC1_2, 4}, {DC1_3, 0}, {DC1_4, 3}],
    DC1_VC = rpc:call(?node, vectorclock, from_list, [DC1]),

    DC2_1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC2_2 = {'antidote2@127.0.0.1', {1390, 186897, 698677}},
    DC2_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC2_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC2 = [{DC2_1, 1}, {DC2_2, 5}, {DC2_3, 2}, {DC2_4, 4}],
    DC2_VC = rpc:call(?node, vectorclock, from_list, [DC2]),

    DC3_1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC3_2 = {'antidote2@127.0.0.1', {1390, 374891, 123674}},
    DC3_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC3_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC3 = [{DC3_1, 1}, {DC3_2, 5}, {DC3_3, 2}, {DC3_4, 4}],
    DC3_VC = rpc:call(?node, vectorclock, from_list, [DC3]),

    DC4_1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC4_2 = {'antidote2@127.0.0.1', {1390, 374891, 123674}},
    DC4_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC4_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC4 = [{DC4_1, 1}, {DC4_2, 5}, {DC4_3, 2}, {DC4_4, 4}],
    DC4_VC = rpc:call(?node, vectorclock, from_list, [DC4]),

    ets:insert(?tab, {DC1_1, DC1_VC}),
    ets:insert(?tab, {DC2_2, DC2_VC}),
    ets:insert(?tab, {DC3_3, DC3_VC}),
    ets:insert(?tab, {DC4_4, DC4_VC}),


    DC1_1_v = get_dc_vals(DC1_2, ets:tab2list(?tab)),
    io:format("Test: ~p~n", [DC1_1_v]),
    ok.

get_dc_vals(DC, [])

get_dc_vals(DC, All) ->
    AllTab = ets:tab2list(?tab),
    [First | Rest] = AllTab,
    {Dcid, Dico} = First,
    KeysInDico = dict:fetch_keys(Dico),
    dict:fetch(DC, Dico).



-spec k_vector() -> ok.
k_vector() ->
    List = ets:tab2list(?tab),
    io:format("List:~p~n", [List]), % Whole ets table as list
    [Head | T] = List,
    io:format("~nHead:~p~n", [Head]), % Whole ets table as list
    Keys = dict:fetch_keys(dict:from_list(Head)),
    io:format("~nKeys:~p~n", [Keys]), % Whole ets table as list
    HeadDict = dict:fetch(Keys, Head),
    ok.


%% Goes through the ets table, picks the value for Dcid at every vector
%% compiles it into a vector clock and returns it.
%% It should be called on every key in ets table to get the system-wide
%% k-stable vector for reads
%% @param list is result of ets:tab2list()
-spec dc_k_vector(any(), list()) -> ok.
dc_k_vector(Dcid, []) ->
    ok;
dc_k_vector(Dcid, [H | T]) ->
    %% H is {dcid, vector}
    {DC, Vec} = H,
    Val = dict:fetch(Dcid, H),
    io:format("Fetched ~p for ~p~n", [Val, DC]),
    [Val | dc_k_vector(Dcid, T)].

%% Lists foreach
foreach(F, [H | T]) ->
    F(H),
    foreach(F, T);
foreach(F, []) ->
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
