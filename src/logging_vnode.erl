-module(logging_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% TODO Refine types!

%% API
-export([start_vnode/1,
         dread/2,
         read/2,
         dappend/4,
         append_list/3,
         threshold_read/3]).

-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-record(state, {partition, logs_map}).

%% API
-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a `threshold read' asynchronous command to the Logs in
%%      `Preflist' From is the operation id form which the caller wants to
%%      retrieve the operations.  The operations are retrieved in inserted
%%      order and the From operation is also included.
-spec threshold_read(preflist(), key(), op_id()) -> term().
threshold_read(Preflist, Log, From) ->
    riak_core_vnode_master:command(Preflist,
                                   {threshold_read, Log, From},
                                   {fsm, undefined, self()},
                                   ?LOGGINGMASTER).

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist'
-spec dread(preflist(), key()) -> term().
dread(Preflist, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {read, Log},
                                   {fsm, undefined, self()},
                                   ?LOGGINGMASTER).

%% @doc Sends a `read' synchronous command to the Logs in `Node'
-spec read({partition(), node()}, key()) -> term().
read(Node, Log) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Log},
                                        ?LOGGINGMASTER).

%% @doc Sends an `append' asyncrhonous command to the Logs in `Preflist'
-spec dappend(preflist(), key(), op(), op_id()) -> term().
dappend(Preflist, Log, OpId, Payload) ->
    riak_core_vnode_master:command(Preflist,
                                   {append, Log, OpId, Payload},
                                   {fsm, undefined, self()},
                                   ?LOGGINGMASTER).

%% @doc Sends a `append_list' syncrhonous command to the Log in `Node'.
-spec append_list({partition(), node()}, key(), [op()]) -> term().
append_list(Node, Log, Ops) ->
    riak_core_vnode_master:sync_command(Node,
                                        {append_list, Log, Ops},
                                        ?LOGGINGMASTER).

%% @doc Opens the persistent copy of the Log.
%%      The name of the Log in disk is a combination of the the word
%%      `log' and the partition identifier.
init([Partition]) ->
    LogFile = string:concat(
            integer_to_list(
                log_utilities:get_partition(Partition)), "log"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    Preflists = lists:foldl(fun(X, Filtered) ->
                    case preflist_member(Partition, X) of
                        true ->
                            lists:append(Filtered, [X]);
                        false ->
                            Filtered
                    end
                    end, [], GrossPreflists),
    lager:info("preflists: ~p", [Preflists]),
    case open_logs(LogFile, Preflists, dict:new()) of
        {error, Reason} ->
            {error, Reason};
        Map ->
            {ok, #state{partition=Partition, logs_map=Map}}
    end.

%% @doc Read command: Returns the operations logged for Key
%%	    Input: The id of the log to be read
%%      Output: {ok, {vnode_id, Operations}} | {error, Reason}
handle_command({read, LogId}, _Sender,
               #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, LogId) of
        {ok, Log} ->
            case get_log(Log, LogId) of
                [] ->
                    {reply, {ok,{{Partition, node()}, []}}, State};
                [H|T] ->
                    {reply, {ok,{{Partition, node()}, [H|T]}}, State};
                {error, Reason}->
                    {reply, {error, Reason}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Threshold read command: Returns the operations logged for Key
%%      from a specified op_id-based threshold.
%%
%%      Input:  From: the oldest op_id to return
%%              LogId: Identifies the log to be read
%%      Output: {vnode_id, Operations} | {error, Reason}
%%
handle_command({threshold_read, LogId, From}, _Sender,
               #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, LogId) of
        {ok, Log} ->
            case get_log(Log, LogId) of
                [] ->
                    {reply, {ok,{{Partition, node()}, []}}, State};
                [H|T] ->
                    Operations = threshold_prune([H|T], From),
                    {reply, {ok,{{Partition, node()}, Operations}}, State};
                {error, Reason}->
                    {reply, {error, Reason}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Repair command: Appends the Ops to the Log
%%      Input:  LogId: Indetifies which log the operations have
%%              to be appended to.
%%              Ops: Operations to append
%%      Output: ok | {error, Reason}
%%
handle_command({append_list, LogId, Ops}, _Sender,
               #state{logs_map=Map}=State) ->
    Result = case get_log_from_map(Map, LogId) of
        {ok, Log} ->
            F = fun(Operation, Acc) ->
                    #operation{op_number=OpId, payload=Payload} = Operation,
                    case insert_operation(Log, LogId, OpId, Payload) of
                        {ok, _}->
                            Acc;
                        {error, Reason} ->
                            [{error, Reason}|Acc]
                    end
                end,
            lists:foldl(F, [], Ops);
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Result, State};

%% @doc Append command: Appends a new op to the Log of Key
%%      Input:  LogId: Indetifies which log the operation has to be
%%              appended to.
%%              Payload of the operation
%%              OpId: Unique operation id
%%      Output: {ok, {vnode_id, op_id}} | {error, Reason}
%%
handle_command({append, LogId, OpId, Payload}, _Sender,
               #state{logs_map=Map, partition=Partition}=State) ->
    case get_log_from_map(Map, LogId) of
        {ok, Log} ->
            case insert_operation(Log, LogId, OpId, Payload) of
                {ok, OpId} ->
                    {reply, {ok, {{Partition, node()}, OpId}}, State};
                {error, Reason} ->
                    {reply, {error, {{Partition, node()}, Reason}}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       #state{logs_map=Map}=State) ->
    F = fun({Key, Operation}, Acc) -> FoldFun(Key, Operation, Acc) end,
    Acc= join_logs(dict:to_list(Map), F, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{logs_map=Map}=State) ->
    {LogId, #operation{op_number=OpId, payload=Payload}} = binary_to_term(Data),
    case get_log_from_map(Map, LogId) of
        {ok, Log} ->
            {ok, _OpId} = insert_operation(Log, LogId, OpId, Payload),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{logs_map=Map}) ->
    LogIds = dict:fetch_keys(Map),
    case no_elements(LogIds, Map) of
        true ->
            {true, State};
        false ->
            {false, State}
    end.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================%%
%% Internal Functions %%
%%====================%%

%% @doc no_elements: checks whether any of the logs contains any data
%%      Input:  LogIds: Each logId is a preflist that represents one log
%%              Map: the dictionary that relates the preflist with the
%%              actual log
%%      Return: true if all logs are empty. false if at least one log
%%              contains data.
%%
-spec no_elements(LogIds::[[Partition::non_neg_integer()]], Map::dict()) -> boolean().
no_elements([], _Map) ->
    true;
no_elements([LogId|Rest], Map) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
            case dets:first(Log) of
                '$end_of_table' ->
                    no_elements(Rest, Map);
                _ ->
                    false
            end;
        error ->
            lager:info("Preflist to map return: no_log_for_preflist: ~w~n",
                       [LogId]),
            {error, no_log_for_preflist}
    end.

%% @doc threshold_prune: returns the operations that are not older than 
%%      the specified op_id
%%      Assump: The operations are retrieved in the order of insertion
%%              If the order of insertion was Op1 -> Op2 -> Op4 -> Op3, 
%%              the expected list of operations would be:
%%              [Op1, Op2, Op4, Op3]
%%      Input:  Operations: Operations to filter
%%              From: Oldest op_id to return
%%              Filtered: List of filetered operations
%%      Return: The filtered list of operations
%%
-spec threshold_prune([op()], atom()) -> [op()].
threshold_prune([], _From) ->
    [];
threshold_prune([{_LogId, Operation}=H|T], From) ->
    case Operation#operation.op_number =:= From of
        true ->
            [H|T];
        false ->
            threshold_prune(T, From)
    end.

%% @doc open_logs: open one log per partition in which the vnode is primary
%%      Input:  LogFile: Partition concat with the atom log
%%                      Preflists: A list with the preflist in which
%%                                 the vnode is involved
%%                      Initial: Initial log identifier. Non negative
%%                               integer. Consecutive ids for the logs. 
%%                      Map: The ongoing map of preflist->log. dict()
%%                           type.
%%      Return:         LogsMap: Maps the  preflist and actual name of
%%                               the log in the system. dict() type.
%%
-spec open_logs(string(), [preflist()], dict()) -> dict() | {error, reason()}.
open_logs(_LogFile, [], Map) ->
    Map;
open_logs(LogFile, [Next|Rest], Map)->
    PartitionList = log_utilities:remove_node_from_preflist(Next),
    PreflistString = string:join(
            lists:map(fun erlang:integer_to_list/1, PartitionList), "-"),
    LogId = LogFile ++ "--" ++ PreflistString,
    LogPath = filename:join(
                app_helper:get_env(riak_core, platform_data_dir), LogId),
    case dets:open_file(LogId, [{file, LogPath}, {type, bag}]) of
        {ok, Log} ->
            lager:info("Opened log: ~p", [LogId]),
            Map2 = dict:store(PartitionList, Log, Map),
            open_logs(LogFile, Rest, Map2);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc get_log_from_map: abstracts the get function of a key-value store
%%              currently using dict
%%      Input:  Map:  dict that representes the map
%%              LogId:  identifies the log.
%%      Return: The actual name of the log
%%
-spec get_log_from_map(dict(), [integer()]) ->
    {ok, term()} | {error, no_log_for_preflist}.
get_log_from_map(Map, LogId) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
            {ok, Log};
        error ->
            lager:info("no_log_for_preflist: ~w", [LogId]),
            {error, no_log_for_preflist}
    end.

%% @doc join_logs: Recursive fold of all the logs stored in the vnode
%%      Input:  Logs: A list of pairs {Preflist, Log}
%%                      F: Function to apply when floding the log (dets)
%%                      Acc: Folded data
%%      Return: Folded data of all the logs.
%%
-spec join_logs([{preflist(), log()}], fun(), term()) -> term().
join_logs([], _F, Acc) ->
    Acc;
join_logs([{_Preflist, Log}|T], F, Acc) ->
    JointAcc = dets:foldl(F, Acc, Log),
    join_logs(T, F, JointAcc).

%% @doc insert_operation: Inserts an operation into the log only if the 
%%      OpId is not already in the log
%%      Input:
%%          Log: The identifier log the log where the operation will be 
%%               inserted
%%          LogId: Log identifier to which the operation belongs.
%%          OpId: Id of the operation to insert
%%          Payload: The payload of the operation to insert
%%      Return: {ok, OpId} | {error, Reason}
%%
-spec insert_operation(log(), [integer()], op_id(), payload()) ->
    {ok, op_id()} | {error, reason()}.
insert_operation(Log, LogId, OpId, Payload) ->
    case dets:match(Log, {LogId, #operation{op_number=OpId, payload='_'}}) of
        [] ->
            Result = dets:insert(Log, {LogId, #operation{op_number=OpId,
                                                         payload=Payload}}),
            case Result of
                ok ->
                    {ok, OpId};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason};
        _ ->
            {ok, OpId}
    end.

%% @doc get_log: Looks up for the operations logged in a particular log
%%    Input:  Log: Table identifier of the log
%%            LogId: Identifier of the log
%%    Return: List of all the logged operations
%%
-spec get_log(log(), [integer()]) -> [op()] | {error, atom()}.
get_log(Log, LogId) ->
    dets:lookup(Log, LogId).

%% @doc preflist_member: Returns true if the Partition identifier is
%%              part of the Preflist
%%      Input:  Partition: The partition identifier to check
%%              Preflist: A list of pairs {Partition, Node}
%%      Return: true | false
%%
-spec preflist_member(partition(), preflist()) -> boolean().
preflist_member(Partition,Preflist) ->
    lists:any(fun({P, _}) -> P =:= Partition end, Preflist).

-ifdef(TEST).

%% @doc Testing threshold_prune works as expected
thresholdprune_test() ->
    Operations = [{log1, #operation{op_number=op1}},
                  {log1, #operation{op_number=op2}},
                  {log1, #operation{op_number=op3}},
                  {log1, #operation{op_number=op4}},
                  {log1, #operation{op_number=op5}}],
    Filtered = threshold_prune(Operations, op3),
    ?assertEqual([{log1, #operation{op_number=op3}},
                  {log1, #operation{op_number=op4}},
                  {log1, #operation{op_number=op5}}], Filtered).

%% @doc Testing threshold_prune works even when there is no matching
%%      op_id.
thresholdprune_notmatching_test() ->
    Operations = [{log1, #operation{op_number=op1}},
                  {log1, #operation{op_number=op2}},
                  {log1, #operation{op_number=op3}},
                  {log1, #operation{op_number=op4}},
                  {log1, #operation{op_number=op5}}],
    Filtered = threshold_prune(Operations, op6),
    ?assertEqual([], Filtered).

%% @doc Testing get_log_from_map works in both situations, when the key
%%      is in the map and when the key is not in the map
get_log_from_map_test() ->
    Dict = dict:new(),
    Dict2 = dict:store([floppy1, c], value1, Dict),
    Dict3 = dict:store([floppy2, c], value2, Dict2),
    Dict4 = dict:store([floppy3, c], value3, Dict3),
    Dict5 = dict:store([floppy4, c], value4, Dict4),
    ?assertEqual({ok, value3}, get_log_from_map(Dict5, [floppy3,c])),
    ?assertEqual({error, no_log_for_preflist}, get_log_from_map(Dict5, [floppy5, c])).

%% @doc Testing that preflist_member returns true when there is a
%%      match.
preflist_member_true_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(true, preflist_member(partition1, Preflist)).

%% @doc Testing that preflist_member returns false when there is no
%%      match.
preflist_member_false_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(false, preflist_member(partition5, Preflist)).

-endif.
