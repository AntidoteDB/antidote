-module(replication_test).

-export([confirm/0, send_multiple_updates/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    N=6,
    [Nodes] = rt:build_clusters([N]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    replication_test(Nodes).

%% @doc replication_test: Test that check whether replication works
%% properly. Let's say, the replicas of the key is N3, N4, N5.
%%
%% First call `append/2' and then check if at least two replicas have 
%% applied that operation.
%%
%% Secondly, create a network partition ([N3, N4], [N5]) and checks
%% if `append/2'is successful, [N3, N4] both applied the operation.
%%
%% Finally, heal the network partition and create a new partition
%% ([N3], [N4, N5]) and checks if `read/2' from [N4, N5] returns the correct
%% value. Checking if N5 is repaired with the full set of operations is covered
%% by append_list_test.

%%  Input:  Nodes:  List of the nodes of the cluster
replication_test(Nodes) ->
    [N1|_] = Nodes,
    Key = key1,

    LogId = rpc:call(N1, log_utilities, get_logid_from_key, [Key]),
    Preflist = rpc:call(N1, log_utilities, get_apl_from_logid, [LogId, logging]),
    lager:info("LogId ~w: Preflist ~w",[LogId, Preflist]),
    NodesRep = [Node || {_Index, Node} <- Preflist],

    lager:info("Nodes that replicate ~w: ~w",[Key, NodesRep]),

    WriteResult = rpc:call(hd(NodesRep), floppy, append, [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),
    {ok, OpId} = WriteResult, 

    Payload = {payload, key1, increment, ucl},
    NumOfRecord1 = read_matches(LogId, Preflist, [{LogId, {operation, OpId, Payload}}]),
    ?assertMatch(true, NumOfRecord1 ==3),
    
    %% Second test
    Excluded1 = hd(lists:reverse(NodesRep)),
    Part1 = lists:filter(fun(Elem) -> Elem /= Excluded1 end, Nodes),   %%tl(lists:reverse(NodesRep)),

    PartInfo1 = rt:partition(Part1, [Excluded1]),

    NewOpIds = send_multiple_updates(hd(Part1), 4, [], Key),
    lager:info("Total: ~w",[NewOpIds]),

    OpIds = [OpId]++NewOpIds,
    Records = [{LogId, {operation, Id, Payload}}|| Id <- OpIds],
    NumOfRecord2 = read_matches(LogId, Preflist, Records),

    ?assertEqual(2, NumOfRecord2),

    Result2 = rpc:call(Excluded1, floppy, append, [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({error, _}, Result2),

    %% Third test
    ok = rt:heal(PartInfo1),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    Excluded2 = hd(NodesRep),
    Part2 = lists:filter(fun(Elem) -> Elem /= Excluded2 end, Nodes),   %%tl(lists:reverse(NodesRep)),
    lager:info("Part ~w", [Part2]),
    _PartInfo2 = rt:partition(Part2, [Excluded2]),

    lager:info("Cluster splitted"),
    Result3 = rpc:call(hd(Part2), floppy, read, [Key, riak_dt_gcounter]),
    lager:info("Value read: ~w",[Result3]),
    ?assertEqual(Result3, 6),
    pass.



read_matches(LogId, Preflist, Record) ->
    Results = [rpc:call(N, logging_vnode, read, [{I, N}, LogId])|| {I, N} <- Preflist],
    Ops = [Op ||{ok, {_,Op}} <- Results],
    SortedRecord = lists:sort(Record),
    NewSum = lists:foldl(fun(X, Sum) -> case lists:sort(X) of
                                    SortedRecord -> Sum+1;
                                    _ -> Sum
                             end
                end, 0, Ops),
    NewSum.



send_multiple_updates(Node, Total, OpIds, Key) ->
    case Total of
        0 ->
            OpIds;
        _ ->
            WriteResult = rpc:call(Node, floppy, append, [Key, riak_dt_gcounter, {increment, ucl}]),
            ?assertMatch({ok, _}, WriteResult),
            {ok, OpId} = WriteResult,
            send_multiple_updates(Node, Total - 1, OpIds++[OpId], Key)
    end.

