#!/usr/bin/env escript

-define(ADDRESS, "localhost").

-define(PORT, 8087).

load(Dep) ->
    Path = filename:dirname(escript:script_name()) ++ "/../_build/test/lib/" ++ Dep ++ "/ebin",
    case code:add_pathz(Path) of
        true ->
            true;
        Err ->
            erlang:error({could_not_load, Path, Err})
    end.

main(_) ->
    % load required code
    [load(Dep) || Dep <- ["riak_pb", "antidote_pb", "protobuffs"]],

    % Try to read something:
    ok = test_transaction(20).

test_transaction(Tries) ->
    {ok, Pid} = try_connect(10),
    Key = <<"release_test_key">>,
    Bound_object = {Key, antidote_crdt_counter_pn, <<"release_test_key_bucket">>},
    io:format("Starting Test transaction~n"),
    case antidotec_pb:start_transaction(Pid, ignore, {}) of
        {error, Reason} when Tries > 0 ->
            io:format("Could not start transaction: ~p~n", [Reason]),
            timer:sleep(1000),
            io:format("Retrying to start transaction ...~n"),
            test_transaction(Tries - 1);
        {ok, Tx} ->
            io:format("Reading counter~n"),
            case antidotec_pb:read_objects(Pid, [Bound_object], Tx) of
                {error, Reason} when Tries > 0 ->
                    io:format("Could not read Counter: ~p~n", [Reason]),
                    timer:sleep(1000),
                    io:format("Retrying to start transaction ...~n"),
                    test_transaction(Tries - 1);
                {ok, [Val]} ->
                    io:format("Commiting transaction~n"),
                    {ok, _} = antidotec_pb:commit_transaction(Pid, Tx),
                    Value = antidotec_counter:value(Val),
                    true = Value >= 0,
                    _Disconnected = antidotec_pb_socket:stop(Pid),
                    io:format("Release is working!~n"),
                    ok
            end
    end.

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
