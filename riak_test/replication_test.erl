-module(replication_test).

-export([confirm/0]).

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
%% value. Then check if N5 is repaired with the full set of operations due
%% to read_repair.

%%  Input:  Nodes:  List of the nodes of the cluster
replication_test(Nodes) ->
    [N1|_] = Nodes,
    Key = key1,

    LogId = rpc:call(N1, log_utilities, get_logid_from_key, [Key]),
    Preflist = rpc:call(N1, log_utilities, get_apl_from_logid, [LogId, logging]),
    lager:info("LogId ~w: Preflist ~w",[LogId, Preflist]),
    NodesRep = [Node || {_Index, Node} <- Preflist],
    
    lager:info("Nodes that replicate ~w: ~w",[Key, NodesRep]),

    WriteResult = rpc:call(hd(NodesRep), floppy, append, [Key, {increment, ucl}]),
    ?assertMatch({ok, _}, WriteResult),
    {ok, OpId} = WriteResult, 

    Payload = {payload, key1, increment, ucl},
    NumOfRecord1 = read_matches(LogId, Preflist, [{LogId, {operation, OpId, Payload}}]),
    ?assertMatch(true, NumOfRecord1 >=2),
    
    %% Second test
    Part1 = tl(lists:reverse(NodesRep)),
    Excluded1 = hd(lists:reverse(NodesRep)),

    _PartInfo1 = rt:partition(Part1, [Excluded1]),

    NewOpIds = send_multiple_updates(hd(Part1), 4, [], Key),
    lager:info("Total: ~w",[NewOpIds]),

    OpIds = NewOpIds++[OpId],
    Records = [{LogId, {operaion, Id, Payload}}|| Id <- OpIds],
    NumOfRecord2 = read_matches(LogId, Preflist, Records),
    ?assertMatch(true, NumOfRecord2 ==2),

    %% Third test
    %ok = rt:heal(PartInfo1),
    %ok = rt:wait_for_cluster_service(Nodes, replication),

    %Part2 = tl(NodesRep)
    %Excluded2 = hd(NodesRep),
    %PartInfo2 = rt:partition(Part2, [Excluded2]),

    %LogId = log_utilities:get_logid_from_key(Key)
    %Preflist = log_utilities:get_apl_from_logid(LogId, logging),
    %Results = [Result | Result = logging_vnode:dread([N], LogId) <- Preflist],
    %lists:foldl(),
        
    %Result = rpc:call(N1, floppy, read, [Key, riak_dt_gcounter]),
    %lager:info("Value read: ~w",[Result]),
    %?assertEqual(Result, Total + 1),
    pass.



read_matches(LogId, Preflist, Record) ->
    Results = [rpc:call(N, logging_vnode, read, [{I, N}, LogId])|| {I, N} <- Preflist],
    Ops = [Op ||{ok, {_,Op}} <- Results],
    SortedRecord = lists:sort(Record),
    lager:info("Record: ~w, Results, ~w", [Record, Ops]),
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
            WriteResult = rpc:call(Node, floppy, append, [Key, {increment, ucl}]),
            ?assertMatch({ok, _}, WriteResult),
            {ok, OpId} = WriteResult,
            send_multiple_updates(Node, Total - 1, OpIds++[OpId], Key)
    end.
