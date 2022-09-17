#!/usr/bin/env escript
%% -*- erlang -*-
%%! -sname log_dump -I ../../

-include("../include/antidote.hrl").
main([LogFile]) ->
    try
        read_log(LogFile)
    catch
        _:_ ->
            usage()
    end;
main(_) ->
    usage().

usage() ->
    io:format("usage: log_dump LOG_FILE_NAME\n"),
    halt(1).

read_log(LogFile) ->
    case
        disk_log:open([{name, log}, {file, LogFile}, {mode, read_only}])
    of
        {ok, Log} ->
            io:format("FILE: ~p~nINFO: ~p~n", [LogFile, disk_log:info(Log)]),
            read_log_records(Log);
        {repaired, Log, _, _} ->
            io:format("Repaired log~n", []),
            read_log_records(Log);
        {error, _Error} = E ->
            io:format("Error: ~p~n", [E]),
            E
    end.

read_log_records(Log) ->
    try print_log_records(Log)
    catch T:E:S ->
            io:format("~p:~p ~n~p~n", [T, E, S])
    after
            disk_log:close(Log)
    end.

print_log_records(Log) ->
    print_log_records(Log, start, #{}).

print_log_records(Log, Continuation, Cache) ->
    case disk_log:chunk(Log, Continuation) of
        eof ->
            io:format("EOF~n", []),
            {eof, []};
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewTerms} ->
            Cache1 = iterate_terms(NewTerms, Cache),
            print_log_records(Log, NewContinuation, Cache1);
         {NewContinuation, NewTerms, BadBytes} ->
            Cache1 = iterate_terms(NewTerms, Cache),
            case BadBytes > 0 of
                true ->
                    io:format("Badbytes: ~p:~p~n", [BadBytes, NewTerms]),
                    {error, bad_bytes};
                false ->
                    print_log_records(Log, NewContinuation, Cache1)
            end
    end.

iterate_terms([{LogId, Op} | Rest], Cache) ->
    Cache1 = maybe_print_log_id(LogId, Cache),
    Cache2 = iterate_term(Op, Cache1),

    iterate_terms(Rest, Cache2);
iterate_terms([], Cache) ->
    Cache.

iterate_term(#log_record{version = V, op_number = OpNumber,
                         bucket_op_number = BNumber, log_operation = Op}, Cache) ->
    Cache1 = maybe_print_node(OpNumber, Cache),
    io:format("|~s|", [printable(OpNumber)]),
    print_operation(Op, Cache1),
    Cache1.

print_operation(#log_operation{tx_id = TxId, op_type = OpType,
                              log_payload = Payload
                             }, _Cache) ->
    io:format("TX:~s|OP:~p|~n", [printable_tx(TxId), OpType]),
    print_payload(Payload).

print_payload(#prepare_log_payload{prepare_time = TM}) ->
    io:format(" prepare_time: ~p~n", [TM]);
print_payload(#commit_log_payload{commit_time = {DC, CT}, snapshot_time = ST}) ->
    io:format(" commit_time: ~p~n snapshot_time: ~p~n", [CT, ST]);
print_payload(#update_log_payload{} = R) ->
    Fields = record_info(fields, update_log_payload),
    [_| Values] = tuple_to_list(R),
    Zip = lists:zip(Fields, Values),
    lists:foreach(fun({K, undefined}) ->
                          ok;
                     ({K, V}) ->
                          io:format(" ~p = ~p~n", [K, V])
                  end, Zip);
print_payload(R) ->
    io:format(" ~p~n", [R]).


printable_tx(#tx_id{local_start_time = TM, server_pid = Pid}) ->
    io_lib:format("~p-~p", [TM, Pid]).

printable(#op_number{global = G, local = L}) ->
    io_lib:format("G:~p|L:~p", [G, L]);
printable(Op) ->
    Op.

%%------------------------------------------------------------------------------

maybe_print_log_id(LogId, #{log_id := LogId} = Cache) ->
    Cache;
maybe_print_log_id(LogId, Cache) ->
    io:format("LogId: ~p~n", [LogId]),
    Cache#{log_id => LogId}.

maybe_print_node(#op_number{node = N}, #{node := N} = Cache) ->
    Cache;
maybe_print_node(#op_number{node = {N, DC} = Node}, Cache) ->
    io:format("-------------------------------------------------------~n",[]),
    io:format("NODE: ~p DC: ~p~n~n", [N, DC]),
    Cache#{node => Node}.
