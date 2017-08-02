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

    %% Test 1, expected results
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
    DC3_2 = {'antidote2@127.0.0.1', {1390, 186897, 698677}},
    DC3_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC3_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC3 = [{DC3_1, 0}, {DC3_2, 5}, {DC3_3, 4}, {DC3_4, 12}],
    DC3_VC = rpc:call(?node, vectorclock, from_list, [DC3]),

    DC4_1 = {'antidote1@127.0.0.1', {1501, 537303, 598423}},
    DC4_2 = {'antidote2@127.0.0.1', {1390, 186897, 698677}},
    DC4_3 = {'antidote3@127.0.0.1', {1490, 186159, 768617}},
    DC4_4 = {'antidote4@127.0.0.1', {1590, 184597, 573977}},
    DC4 = [{DC4_1, 1}, {DC4_2, 0}, {DC4_3, 0}, {DC4_4, 12}],
    DC4_VC = rpc:call(?node, vectorclock, from_list, [DC4]),

    % DC IDs are unique
    ets:insert(?tab, {DC1_1, DC1_VC}),
    ets:insert(?tab, {DC1_2, DC2_VC}),
    ets:insert(?tab, {DC1_3, DC3_VC}),
    ets:insert(?tab, {DC1_4, DC4_VC}),

    Keys = [DC1_1, DC1_2, DC1_3, DC1_4],
    VersionMatrix = get_version_matrix(Keys),
    io:format("Version Matrix:~p~n", [VersionMatrix]),
    ok.



get_dc_vals(_, [], Acc) ->
    Acc;
get_dc_vals(Dc, [{_, Dico} | T], Acc) ->
    Val = case dict:find(Dc, Dico) of
              {ok, Value} ->
                  Value;
              error -> % DC not found, shouldn't happen
                  0
          end,
    get_dc_vals(Dc, T, [Val | Acc]).

get_version_matrix([]) ->
    ok;
get_version_matrix(DC_IDs) ->
    TabList = ets:tab2list(?tab),
    lists:foldl(fun(X, Acc) -> VC = get_dc_vals(X, TabList, []), [{X, VC} | Acc] end, [], DC_IDs).

%% Goes through the ets table, picks the value for Dcid at every vector
%% compiles it into a vector clock and returns it.
%% It should be called on every key in ets table to get the system-wide
%% k-stable vector for reads
%% @param list is result of ets:tab2list()
-spec dc_k_vector(any(), list()) -> ok.
dc_k_vector(Dcid, []) ->
    ok;
dc_k_vector(Dcid, [H | T]) ->
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
