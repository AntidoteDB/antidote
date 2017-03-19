#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name merge_staleness@127.0.0.1 -cookie antidote
-mode(compile).

%% This should be called like (e.g.): process_staleness.erl Staleness_LOAD 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'
main(StringParams) ->
    io:format("got params: ~s", [StringParams]),

    ParamList = string:tokens(StringParams, "\n "),

    io:format("params list ~w", [ParamList]),


    OutputFile=lists:nth(1, ParamList),

    io:format("Output file:  ~w~n", [OutputFile]),

    FileNameList = lists:sublist(ParamList, 2, length(ParamList)),


StalenessTable = merge_staleness_files_into_table(FileNameList),
    processStalenessTable(StalenessTable, OutputFile).

-spec merge_staleness_files_into_table(string()) -> atom() | {error, term()}.
merge_staleness_files_into_table(DirectoryList) ->
    %%			io:format("got file list: ~p",[FileList]),
    StalenessTable = case ets:info(staleness_table) of
        undefined ->
            %table does not exist, create it
            ets:new(staleness_table, [duplicate_bag, named_table, public]);
        _ ->
            %empty the table
            io:format("DELETTING ALL ITEMS FROM STALENESS_TABLE"),
            true = ets:delete_all_objects(staleness_table),
            staleness_table
    end,
    lists:foreach(fun(Directory) ->
        FileList = file:list_dir(Directory),
        lists:foreach(fun(FileName) ->
            ok = file_to_table(FileName, start, StalenessTable, "StalenessLog")
        end, FileList)
    end, DirectoryList),
    StalenessTable.

-spec file_to_table(string(), start | disk_log:continuation(), atom(), string()) -> ok.
file_to_table(FileName, Continuation, TableName, FileKind)->
    %% add the directory and remove the .LOG extension.
    %%	io:format("merging file ~p from Continuation ~p ", [DirAndFileName, Continuation]),
    case (lists:sublist(FileName, 1, length("StalenessLog")) == FileKind) of
        true -> %% this is a staleness log file, process it
            DirAndFileName = "./data/" ++ lists:sublist(FileName, length(FileName)-4),
            LogStatus=case Continuation of
                start ->
                    io:format("openning file"),
                    State=disk_log:open([{name, DirAndFileName}]),
                    io:format("openning log returned: ~p",[State]),
                    State;
                _->
                    do_nothing
            end,
            case LogStatus of
                {error, _} ->
                    io:format("no such log file"),
                    disk_log:close(DirAndFileName),
                    ok;
                _->
                    case disk_log:chunk(DirAndFileName, Continuation) of
                        eof ->
                            io:format("closing file"),
                            disk_log:close(DirAndFileName),
                            ok;
                        {NextContinuation, List} ->
                            lists:foreach(fun(Info) ->
                                %%								io:format("inserting into table: ~p", [{Info, 1}]),
                                ets:insert(TableName, {Info,1})
                            end, List),
                            file_to_table(DirAndFileName, NextContinuation, TableName, FileKind)
                    end
            end;
        false -> %% nothing to do, not a staleness file.
            ok
    end.



usage() ->
    io:format("This should be called like (e.g.): process_staleness.erl 100-1-2-3 file1 file2"),
    halt(1).

-include_lib("eunit/include/eunit.hrl").

processStalenessTable(StalenessTable, OutputFileName) ->
    LoadString=lists:last(string:tokens(OutputFileName, "_")),
    {ok, FileHandler} = file:open(OutputFileName++".csv", [raw, write]),
    %% create header
    HeaderLine="Workload\tTotal\tSkew\tPrepared\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\t14\t15\n",
    file:write(FileHandler, HeaderLine),
    FullTableSize = ets:info(StalenessTable, size),
    ClockSkew = length(ets:lookup(staleness_table, clock_skew)),
    Prepared = length(ets:lookup(staleness_table, prepared)),
    TotalReads = FullTableSize - ClockSkew - Prepared,
    String = LoadString ++ "\t" ++ TotalReads ++ "\t" ++ ClockSkew ++ "\t" ++ Prepared,
        file:write(FileHandler, String),
    processStalenessTableInternal(FileHandler, StalenessTable, TotalReads, 0).

processStalenessTableInternal(FileHandler, StalenessTable, TotalReads, Number) ->
    NumberOfNumber = length(ets:lookup(staleness_table, Number)),
    case NumberOfNumber of
        0 ->
            file:write(FileHandler, "\n"),
            file:close(FileHandler);
        _ ->
            file:write(FileHandler, "\t" ++ NumberOfNumber),
            processStalenessTableInternal(FileHandler, StalenessTable, TotalReads, Number+1)
    end.



