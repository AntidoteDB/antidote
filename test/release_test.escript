#!/usr/bin/env escript

-define(ADDRESS, "localhost").

-define(PORT, 8087).

load(Dep) ->
    true = code:add_pathz(filename:dirname(escript:script_name()) ++ "/../_build/test/lib/" ++ Dep ++ "/ebin").

main(_) ->
    % load required code
    [load(Dep) || Dep <- ["riak_pb", "antidote_pb", "protobuffs"]],

    Key = <<"release_test_key">>,
    {ok, Pid} = try_connect(10),
    Bound_object = {Key, antidote_crdt_counter, <<"release_test_key_bucket">>},
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, {}),
    ok = antidotec_pb:update_objects(Pid, [{Bound_object, increment, 1}], TxId),
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    %% Read committed updated
    {ok, Tx2} = antidotec_pb:start_transaction(Pid, ignore, {}),
    {ok, [Val]} = antidotec_pb:read_objects(Pid, [Bound_object], Tx2),
    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx2),
    Value = antidotec_counter:value(Val),
    true = Value > 0,
    _Disconnected = antidotec_pb_socket:stop(Pid),
    io:format("Release is working, counter = ~p!~n", [Value]),
    ok.

try_connect(Tries) ->
     case antidotec_pb_socket:start(?ADDRESS, ?PORT) of
        {ok, Pid} ->
            {ok, Pid};
        Other when Tries > 0 ->
            io:format("Could not connect to Antidote: ~p~n", [Other]),
            timer:sleep(1000),
            io:format("Retrying to connect ...~n"),
            try_connect(Tries - 1);
        Other ->
            Other
     end.