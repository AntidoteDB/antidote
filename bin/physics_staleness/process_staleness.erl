#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name merge_staleness@127.0.0.1 -cookie antidote
-mode(compile).

%% This should be called like (e.g.): process_staleness.erl Staleness_LOAD 'antidote1@1.2.3.4' 'antidote2@5.6.7.8'
main([StringParams]) ->
    io:format("got params: ~s~n", [StringParams]),

    ParamList = string:tokens(StringParams, "\n "),

    io:format("params list ~p~n", [ParamList]),
    io:format("params list ~p~n", [length(ParamList)]),



    OutputFile=lists:nth(1, ParamList),

    io:format("Output file:  ~p~n", [OutputFile]),

    DirectoryList = lists:sublist(ParamList, 2, length(ParamList)),

    io:format("FileNameList:  ~p~n", [DirectoryList]),



    StalenessTable = merge_staleness_files_into_table(DirectoryList),
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
        {ok, FileList} = file:list_dir(Directory),
        io:format("Files in directory ~p:~n~p~n", [Directory, FileList]),

        lists:foreach(fun(FileName) ->
            ok = file_to_table(Directory++"/"++FileName, start, StalenessTable, "StalenessLog")
        end, FileList)

    end, DirectoryList),
    StalenessTable.

-spec file_to_table(string(), start | disk_log:continuation(), atom(), string()) -> ok.
file_to_table(FileName, Continuation, TableName, FileKind)->
    %%	io:format("merging file ~p from Continuation ~p ", [DirAndFileName, Continuation]),
    case (string:str(FileName, FileKind)) of
        0 -> %% nothing to do, not a staleness file.
            io:format("This is not a staleness file, nothing to do for: ~p~n",[FileName]),
            ok;
        _ -> %% this is a staleness log file, process it
            DirAndFileName = case lists:sublist(FileName, length(FileName)-3, length(FileName)) == ".LOG" of
                false ->
                    FileName;
                true -> %remove the .LOG extension
                    lists:sublist(FileName, length(FileName)-4)
            end,
            LogStatus=case Continuation of
                start ->
                    io:format("openning file~n"),
                    State=disk_log:open([{name, DirAndFileName}]),
                    io:format("openning log returned: ~p~n",[State]),
                    State;
                _->
                    do_nothing
            end,
            case LogStatus of
                {error, _} ->
                    io:format("no such log file~n"),
                    disk_log:close(DirAndFileName),
                    ok;
                _->
                    case disk_log:chunk(DirAndFileName, Continuation) of
                        eof ->
                            io:format("closing file~n"),
                            disk_log:close(DirAndFileName),
                            ok;
                        {NextContinuation, List} ->
                            lists:foreach(fun(Info) ->
%%                                								io:format("inserting into table: ~p", [{Info, 1}]),
                                ets:insert(TableName, {Info,1})
                            end, List),
                            file_to_table(DirAndFileName, NextContinuation, TableName, FileKind)
                    end
            end

    end.

-include_lib("eunit/include/eunit.hrl").

processStalenessTable(StalenessTable, OutputFileName) ->
    LoadString=string:sub_string(OutputFileName, 7), % remove the Stale- part from the directory
    {ok, FileHandler} = file:open(OutputFileName++".csv", [raw, write]),
    %% create header
    HeaderLine="Workload\tTotal\tSkew\tPrepared\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t11\t12\t13\t14\t15\n",
    file:write(FileHandler, HeaderLine),
    FullTableSize = ets:info(StalenessTable, size),
    ClockSkew = length(ets:lookup(staleness_table, clock_skew)),
    Prepared = length(ets:lookup(staleness_table, prepared)),
    TotalReads = FullTableSize - ClockSkew - Prepared,

    io:format("LoadString ~s~n", [LoadString]),
    io:format("FullTableSize ~p~n", [FullTableSize]),
    io:format("ClockSkew ~p~n", [ClockSkew]),
    io:format("Prepared ~p~n", [Prepared]),
    io:format("TotalReads ~p~n", [TotalReads]),

    String = LoadString ++ "\t" ++ integer_to_list(TotalReads) ++ "\t" ++ integer_to_list(ClockSkew) ++ "\t" ++ integer_to_list(Prepared),
    io:format("String ~p~n", [String]),

    file:write(FileHandler, String),
    FinalString=processStalenessTableInternal("", StalenessTable, TotalReads, 1, TotalReads),
    file:write(FileHandler, FinalString++"\n"),
    io:format("FinalString ~p~n", [FinalString]),

    file:close(FileHandler).

processStalenessTableInternal(InitString, StalenessTable, TotalReads, Number, ZeroCount) ->
    NumberOfNumber = length(ets:lookup(staleness_table, Number)),
    io:format("Number of  ~p: ~p~n", [Number, NumberOfNumber]),
    UpdatedZeroCount=ZeroCount - NumberOfNumber,
    case NumberOfNumber of
        0 ->
            integer_to_list(UpdatedZeroCount) ++ "\t" ++ InitString;
        _ ->
            String = InitString ++ "\t" ++ integer_to_list(NumberOfNumber),
            processStalenessTableInternal(String, StalenessTable, TotalReads, Number+1, UpdatedZeroCount)
    end.



