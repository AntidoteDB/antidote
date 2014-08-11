%% @doc replication_test: Test that check whether replication works
%% properly. Let's say, the replicas of the key is N3, N4, N5.
%%
%% First call `append/3' and then check if at least two replicas have 
%% applied that operation.
%%
%% Secondly, create a network partition ([N3, N4], [N5]) and checks
%% if `append/3'is successful, [N3, N4] both applied the operation.
%%
%% Finally, heal the network partition and create a new partition
%% ([N3], [N4, N5]) and checks if `read/2' from [N4, N5] returns the correct
%% value. Checking if N5 is repaired with the full set of operations is covered
%% by append_list_test.

%%  Input:  Nodes:  List of the nodes of the cluster
%%

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

    Payload = {payload, key1, riak_dt_gcounter, increment, ucl},
    NumOfRecord1 = read_matches(LogId, Preflist, [{LogId, {operation, OpId, Payload}}]),
    ?assertMatch(true, NumOfRecord1 >=2),
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
