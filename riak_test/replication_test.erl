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
%% properly. 
%% The interface of clock_si is not used, as its log entry is complicated and
%% clock_si does not work if the first replica is partitioned from the others.
%%
%% Let's say, the replicas of the key is N3, N4, N5.
%%
%% First call `append/2' and then check if all three replicas have 
%% applied that operation.
%%
%% Secondly, create a network partition ([N3, N4], [N5]) and checks
%% if `append/2'is successful, [N3, N4] both applied the operation.
%%
%% Finally, heal the network partition and create a new partition
%% ([N3], [N4, N5]) and checks if `read/2' from [N4, N5] returns the correct
%% value. Then check if N4, N5 is repaired with the full set of operations.

%%  Input:  Nodes:  List of the nodes of the cluster
replication_test(Nodes) ->
    [N1|_] = Nodes,
    Key = key1,

    LogId = rpc:call(N1, log_utilities, get_logid_from_key, [Key]),
    Preflist = rpc:call(N1, log_utilities, get_preflist_from_key, [Key]),
    lager:info("LogId:~w Preflist:~w",[LogId, Preflist]),
    NodesRep = [Node || {_Index, Node} <- Preflist],

    lager:info("Nodes that replicate ~w: ~w",[Key, NodesRep]),

    %% First test: perform an append command to all three nodes replicating key1 and checks if all three nodes
    %% have the record. This proves that quorum-write works under no network partition.
    %% Originally, if the operation succeeds, at least two nodes will have the record in log. 
    %% Since all three nodes are running in the same physical and there is no possibility of message lost,
    %% we assume all three nodes will have the record. 
    WriteResult = rpc:call(hd(NodesRep), floppy_rep_vnode, append, [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),
    {ok, OpId} = WriteResult,

    Payload = {increment, ucl},
    NumOfRecord1 = read_matches(LogId, Preflist, [{LogId, {operation, OpId, Payload}}]),
    ?assertEqual(3, NumOfRecord1),

    %% Second test: partition the cluster to ([N1, N2, N3, N4, N6], [N5]). Perform four appends of key1 to
    %% N3. Check that two nodes out of [N3, N4, N5] have five operations in log. This proves that quorum-write
    %% works under network partition.
    %% Write to N5 and check the operation fails. However this still leaves the append operation record in N5. 
    Excluded1 = hd(lists:reverse(NodesRep)),
    Part1 = lists:filter(fun(Elem) -> Elem /= Excluded1 end, Nodes),
    PartInfo1 = rt:partition(Part1, [Excluded1]),

    NewOpIds = send_multiple_updates(hd(Part1), 4, [], Key),
    lager:info("Total: ~w",[NewOpIds]),

    OpIds = [OpId]++NewOpIds,
    Records = [{LogId, {operation, Id, Payload}}|| Id <- OpIds],
    NumOfRecord2 = read_matches(LogId, Preflist, Records),
    ?assertEqual(2, NumOfRecord2),

    Result2 = rpc:call(Excluded1, floppy_rep_vnode, append, [Key, riak_dt_gcounter, {increment, ucl}]),
    ?assertMatch({error, _}, Result2),

    %% Third test: heal the partition and partition the cluster to ([N1, N2, N4, N5, N6], [N3]).
    %% Perform quorum read (on N5) and check that the returned operation set contains all six operations.
    %% This proves that quorum-read successfully merges different operations from nodes.
    ok = rt:heal(PartInfo1),
    ok = rt:wait_for_cluster_service(Nodes, replication),

    Excluded2 = hd(NodesRep),
    Part2 = lists:filter(fun(Elem) -> Elem /= Excluded2 end, Nodes),   %%tl(lists:reverse(NodesRep)),
    _PartInfo2 = rt:partition(Part2, [Excluded2]),

    lager:info("Cluster splitted"),
    {ok, Result3} = rpc:call(hd(lists:reverse(NodesRep)), floppy_rep_vnode, read, [Key, riak_dt_gcounter]),
    lager:info("Value read: ~w",[Result3]),
    ?assertEqual(length(Result3), 6),

    %% Fourth test: the previous test will trigger read-repair for N4 and N5 and merges operations in these two
    %% nodes.  
    NodeTail = hd(lists:reverse(Preflist)),
    {I, N} = NodeTail,
    OpResult = rpc:call(N, logging_vnode, read, [{I, N}, LogId]),
    ?assertMatch({ok, _}, OpResult),
    {ok, {_, OpRecord}} = OpResult,
    NumOfRecord3 = read_matches(LogId, Preflist, OpRecord),
    ?assertEqual(2, NumOfRecord3),
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
            WriteResult = rpc:call(Node, floppy_rep_vnode, append, [Key, riak_dt_gcounter, {increment, ucl}]),
            ?assertMatch({ok, _}, WriteResult),
            {ok, OpId} = WriteResult,
            send_multiple_updates(Node, Total - 1, OpIds++[OpId], Key)
    end.

