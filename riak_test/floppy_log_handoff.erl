-module(floppy_log_handoff).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    run_test().

run_test() ->
    NTestItems    = 10,                                     %% How many test items to write/verify?
    NTestNodes    = 3,                                      %% How many nodes to spin up for tests?

    lager:info("Testing handoff (items ~p)", [NTestItems]),

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up..."),
    rt:setup_harness(dummy, dummy),

    lager:info("Spinning up test nodes"),
    Versions = [ current || _ <- lists:seq(1, NTestNodes)],
    [RootNode | TestNodes] = rt:deploy_nodes(Versions,[replication]),
    rt:wait_for_service(RootNode, replication),

    lager:info("Populating root node."),
    multiple_writes(RootNode, 1, NTestItems, ucl),

    %% Test handoff on each node:
    lager:info("Testing handoff for cluster."),
    lists:foreach(fun(TestNode) -> test_handoff(RootNode, TestNode, NTestItems) end, TestNodes),

    %% Prepare for the next call to our test (we aren't polite about it, it's faster that way):
    lager:info("Bringing down test nodes."),
    lists:foreach(fun(N) -> rt:brutal_kill(N) end, TestNodes),

    %% The "root" node can't leave() since it's the only node left:
    lager:info("Stopping root node."),
    rt:brutal_kill(RootNode),

    pass.

%% See if we get the same data back from our new nodes as we put into the root node:
test_handoff(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    rt:wait_for_service(NewNode, replication),

    lager:info("Joining new node with cluster."),
    rt:join(NewNode, RootNode),
    ?assertEqual(ok, rt:wait_until_nodes_ready([RootNode, NewNode])),
    rt:wait_until_no_pending_changes([RootNode, NewNode]),

    %% See if we get the same data back from the joined node that we added to the root node.
    %%  Note: systest_read() returns /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff:"),
    Results = multiple_reads(NewNode, 1, NTestItems),
    lager:info("The read data looks like: ~w", [Results]),
    ?assertEqual(0, length(Results)),
    lager:info("Data looks ok."). 

multiple_writes(Node, Start, End, Actor)->
    F = fun(N, Acc) ->
        case rpc:call(Node, floppy, append, [N, {{increment, N}, Actor}]) of
            {ok, _} ->
                Acc;
            Other ->
                [{N, Other} | Acc]
        end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).

multiple_reads(Node, Start, End) ->
    F = fun(N, Acc) ->
        case rpc:call(Node, floppy, read, [N, riak_dt_gcounter]) of
            error ->
                [{N, error} | Acc];
            Value ->
                case Value==N of
                    true ->
                        Acc;
                    false ->
                        [{N, {wrong_val, Value}} | Acc]
                end
        end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).
