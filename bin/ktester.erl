#! /usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name kstable-tester@127.0.0.1 -cookie antidote

-module(ktester).
-compile({parse_transform, lager_transform}).


-define(dbug, io:format("dbug ~p ~p...~n", [?FUNCTION_NAME, ?LINE])).
-define(tab, tabtab).
-define(node, 'antidote@127.0.0.1').
-define(kstab, 3).


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

main(_A) ->
    antidote_connect(?node),
    ets:new(?tab, [set, named_table]),

    DC1 = {'antidote_1', {1501, 537303, 598423}},
    DC2 = {'antidote_2', {1390, 186897, 698677}},
    DC3 = {'antidote_3', {1490, 186159, 768617}},
    DC4 = {'antidote_4', {1590, 184597, 573977}},
    ListVC_DC1 = [{DC1, 1}, {DC2, 4}, {DC3, 0}, {DC4, 3}],
    ListVC_DC2 = [{DC1, 1}, {DC2, 5}, {DC3, 2}, {DC4, 4}],
    ListVC_DC3 = [{DC1, 0}, {DC2, 5}, {DC3, 4}, {DC4, 12}],
    ListVC_DC4 = [{DC1, 1}, {DC2, 0}, {DC3, 0}, {DC4, 12}],
    DC1_VC = rpc:call(?node, vectorclock, from_list, [ListVC_DC1]),
    DC2_VC = rpc:call(?node, vectorclock, from_list, [ListVC_DC2]),
    DC3_VC = rpc:call(?node, vectorclock, from_list, [ListVC_DC3]),
    DC4_VC = rpc:call(?node, vectorclock, from_list, [ListVC_DC4]),

    % DC IDs are unique
    ets:insert(?tab, {DC1, DC1_VC}),
    ets:insert(?tab, {DC2, DC2_VC}),
    ets:insert(?tab, {DC3, DC3_VC}),
    ets:insert(?tab, {DC4, DC4_VC}),

    Keys = [DC1, DC2, DC3, DC4],
    VersionMatrix = get_version_matrix(Keys),
    KVector = get_k_vector(VersionMatrix),
    %% Expected results:
    %% Version Matrix:


    ExpectedVM = [{{antidote_4, {1590, 184597, 573977}}, [12, 12, 3, 4]},
        {{antidote_3, {1490, 186159, 768617}}, [4, 0, 0, 2]},
        {{antidote_2, {1390, 186897, 698677}}, [5, 0, 4, 5]},
        {{antidote_1, {1501, 537303, 598423}}, [0, 1, 1, 1]}],


    ExpectedKVect = [{{antidote_1, {1501, 537303, 598423}}, 1},
        {{antidote_2, {1390, 186897, 698677}}, 4},
        {{antidote_3, {1490, 186159, 768617}}, 0},
        {{antidote_4, {1590, 184597, 573977}}, 4}],


    ?assertEqual(ExpectedVM, VersionMatrix),
    %lager:info("Version matrix test passed!"),
    ?assertEqual(ExpectedKVect, KVector),
    %lager:info("k-vector test passed!"),

%% K-Vector (K=3): [1,4,0,4]
%% K-Vector (K=2): [1,5,2,12]
    %io:format("Version Matrix ~p~n~p stable vector ~p~n",
    %    [VersionMatrix, ?kstab, KVector]),
    pass.

%% Functions


%% Collects values for "Dc"
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


%% Builds version matrix for dcs specified in "DC_IDs"
-spec get_version_matrix([]) -> [] | {error, empty_list}.
get_version_matrix([]) ->
    {error, empty_list};
get_version_matrix(DC_IDs) ->
    TabList = ets:tab2list(?tab),
    lists:foldl(fun(X, Acc) ->
        VC = get_dc_vals(X, TabList, []),
        [{X, VC} | Acc] end,
        [], DC_IDs).


%% Builds k-stable vector for reads from version matrix
%% k is hardcoded for now
%-spec get_k_vector(
get_k_vector([]) ->
    {error, matrix_size};
get_k_vector(VerM) when length(VerM) >= ?kstab ->
    lists:foldl(fun(Row, Acc) ->
        {DC, VC} = Row,
        Sorted = lists:reverse(lists:sort(VC)),
        %io:format("Sorted VC ~p~n", [Sorted]),
        [{DC, lists:nth(?kstab, Sorted)} | Acc]
                end, [], VerM).



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
