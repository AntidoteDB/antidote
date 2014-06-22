-module(logging_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% TODO Refine types!
-type preflist() :: [{integer(), node()}] .
-type key() :: term().
-type log() :: term().
-type reason() :: term().

%% API
-export([start_vnode/1,
         dread/2,
         dappend/4,
         append_list/2,
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
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a `threshold read' asyncrhonous command to the Logs in `Preflist'
%%	From is the operation id form which the caller wants to retrieve the operations.
%%	The operations are retrieved in inserted order and the From operation is also included.
threshold_read(Preflist, Key, From) ->
    riak_core_vnode_master:command(Preflist, {threshold_read, Key, From, Preflist}, {fsm, undefined, self()},?LOGGINGMASTER).

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist' 
dread(Preflist, Key) ->
    riak_core_vnode_master:command(Preflist, {read, Key, Preflist}, {fsm, undefined, self()},?LOGGINGMASTER).

%% @doc Sends an `append' asyncrhonous command to the Logs in `Preflist' 
dappend(Preflist, Key, Op, OpId) ->
    riak_core_vnode_master:command(Preflist, {append, Key, Op, OpId, Preflist},{fsm, undefined, self()}, ?LOGGINGMASTER).

%% @doc Sends a `append_list' syncrhonous command to the Log in `Node'.
append_list(Node, Ops) ->
    riak_core_vnode_master:sync_command(Node,
                                        {append_list, Ops},
                                        ?LOGGINGMASTER).

%% @doc Opens the persistent copy of the Log.
%%	The name of the Log in disk is a combination of the the word `log' and
%%	the partition identifier.
init([Partition]) ->
    LogFile = string:concat(integer_to_list(Partition), "log"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    Preflists = lists:foldl(fun(X, Filtered) -> 
                                    case preflist_member(Partition, X) of 
                                        true ->
                                            lists:append(Filtered,[X]);
                                        false ->
                                            Filtered
                                    end
                            end, [], GrossPreflists),
    case open_logs(LogFile, Preflists, 1, dict:new()) of
        {error, Reason} ->
            {error, Reason};
        Map ->
            {ok, #state{partition=Partition, logs_map=Map}}
    end.

%% @doc Read command: Returns the operations logged for Key
%%	Input: Key of the object to read
%%	Output: {vnode_id, Operations} | {error, Reason}
handle_command({read, Key, Preflist}, _Sender, #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, Preflist) of
        {ok, Log} ->
            case lookup_operations(Log, Key) of
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

%% @doc Threshold read command: Returns the operations logged for Key from a specified op_id-based threshold
%%	Input:  Key of the object to read
%%		From: the oldest op_id to return
%%	Output: {vnode_id, Operations} | {error, Reason}
handle_command({threshold_read, Key, From, Preflist}, _Sender, #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, Preflist) of
        {ok, Log} ->
            case lookup_operations(Log, Key) of
                [] ->
                    {reply, {ok,{{Partition, node()}, []}}, State};
                [H|T] ->
                    Operations =  [H|T],
                    Operations2 = threshold_prune(Operations, From),
                    {reply, {ok,{{Partition, node()}, Operations2}}, State};
                {error, Reason}->
                    {reply, {error, Reason}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Repair command: Appends the Ops to the Log
%%	Input: Ops: Operations to append
%%	Output: ok | {error, Reason}
%%TODO: fix this due to the new log-per-partition modification
handle_command({append_list, Ops}, _Sender, #state{logs_map=Map}=State) ->	
    Result = dets:insert_new(Map, Ops),
    {reply, Result, State};

%% @doc Append command: Appends a new op to the Log of Key
%%	Input:	Key of the object
%%		Payload of the operation
%%		OpId: Unique operation id	      	
%%	Output: {ok, op_id} | {error, Reason}
handle_command({append, Key, Payload, OpId, Preflist}, _Sender,
               #state{logs_map=Map, partition = Partition}=State) ->
    case get_log_from_map(Map, Preflist) of
        {ok, Log} ->
            case insert_operation(Log, Key, OpId, Payload) of
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

    {Key, #operation{op_number=OpId, payload=Payload}} = binary_to_term(Data),
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, ?N, replication),
    lager:info("Receiving data from handoff ~w~n",[node()]),
    case get_log_from_map(Map, Preflist) of
        {ok, Log} ->
            Response = insert_operation(Log, Key, OpId, Payload),
            {reply, Response, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

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
%% @doc threshold_prune: returns the operations that are not onlder than the specified op_id
%%  Assump:	The operations are retrieved in the order of insertion
%%			If the order of insertion was Op1 -> Op2 -> Op4 -> Op3, the expected list of operations would be: [Op1, Op2, Op4, Op3]
%%	Input:	Operations: Operations to filter
%%			From: Oldest op_id to return
%%			Filtered: List of filetered operations
%%	Return:	The filtered list of operations
-spec threshold_prune(Operations::list(), From::atom()) -> list().
threshold_prune([], _From) -> [];
threshold_prune([Next|Rest], From) ->
    case Next#operation.op_number == From of
        true ->
            [Next|Rest];
        false ->
            threshold_prune(Rest, From)
    end.

%% @doc open_logs: open one log per partition in which the vnode is primary
%%	Input:	LogFile: Partition concat with the atom log
%%			Preflists: A list with the preflist in which the vnode is involved
%%			Initial: Initial log identifier. Non negative integer. Consecutive ids for the logs. 
%%			Map: The ongoing map of preflist->log. dict() type.
%%	Return:	LogsMap: Maps the  preflist and actual name of the log in the system. dict() type.
-spec open_logs(LogFile::string(), [preflist()], N::non_neg_integer(), Map::dict()) -> LogsMap::dict() | {error,reason()}.
open_logs(_LogFile, [], _Initial, Map) -> Map;
open_logs(LogFile, [Next|Rest], Initial, Map)->
    LogId = string:concat(LogFile, integer_to_list(Initial)),
    LogPath = filename:join(
                app_helper:get_env(riak_core, platform_data_dir), LogId),
    case dets:open_file(LogId, [{file, LogPath}, {type, bag}]) of
        {ok, Log} ->
            Map2 = dict:store(remove_node_from_preflist(Next), Log, Map),
            open_logs(LogFile, Rest, Initial+1, Map2);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc remove_node_from_preflist:  From each element of the input preflist, the node identifier is removed
%%      Input:  Preflist: list of pairs {Partition, Node}
%%      Return: List of Partition identifiers
-spec remove_node_from_preflist(preflist()) -> [integer()].
remove_node_from_preflist(Preflist) ->
    F = fun(Elem, Acc) ->
                {P,_} = Elem,
                lists:append(Acc, [P])
        end,
    lists:foldl(F, [], Preflist).

%% @doc	get_log_from_map:	abstracts the get function of a key-value store
%%							currently using dict
%%		Input:	Map:	dict that representes the map
%%				Preflist:	The key to search for.
%%		Return:	The actual name of the log
-spec get_log_from_map(dict(), preflist()) -> {ok, term()} | {error, no_log_for_preflist}.
get_log_from_map(Map, FullPreflist) ->
    Preflist = remove_node_from_preflist(FullPreflist),
    lager:info("Preflist to map: ~w~n",[Preflist]),
    case dict:find(Preflist, Map) of
        {ok, Value} ->
            lager:info("Preflist to map return: ~w~n",[Value]),
            {ok, Value};
        error ->
            lager:info("Preflist to map return: no_log_for_preflist~n"),
            {error, no_log_for_preflist}
    end.

%% @doc	join_logs: Recursive fold of all the logs stored in the vnode
%%		Input:	Logs: A list of pairs {Preflist, Log}
%%				F: Function to apply when floding the log (dets)
%%				Acc: Folded data
%%		Return: Folded data of all the logs.
-spec join_logs(Map::[{preflist(), log()}], F::fun(), Acc::term()) -> term().
join_logs([], _F, Acc) -> Acc;
join_logs([Element|Rest], F, Acc) ->
    {_Preflist, Log} = Element,
    JointAcc = dets:foldl(F, Acc, Log),
    join_logs(Rest, F, JointAcc).

%% @doc	insert_operation: Inserts an operation into the log only if the OpId is not already in the log
%%		Input:	Log: The identifier log the log where the operation will be inserted
%%				Key: Key to which the operation belongs.
%%				OpId: Id of the operation to insert
%%				Payload: The payload of the operation to insert
%%		Return:	{ok, OpId} | {error, Reason}
-spec insert_operation(log(), key(), OpId::{Number::non_neg_integer(), node()}, Payload::term()) -> {ok, {Number::non_neg_integer(), node()}} | {error, reason()}.
insert_operation(Log, Key, OpId, Payload) ->
    case dets:match(Log, {Key, #operation{op_number=OpId, payload='$1'}}) of
        [] ->
            Result = dets:insert(Log,{Key, #operation{op_number=OpId,payload=Payload}}),
            Response = case Result of
                           ok ->
                               {ok, OpId};
                           {error, Reason} ->
                               {error, Reason}
                       end,
            Response;
        {error, Reason} ->
            {error, Reason};
        _ ->
            {ok, OpId}
    end.

%% @doc lookup_operations: Looks up for the operations logged for a particular key
%%		Input:	Log: Identifier of the log
%%		        Key: Key to shich the operation belongs
%%		Return:	List of all the logged operations or error  
%% TODO Fix type spec!
-spec lookup_operations(log(), key()) -> [tuple()] | {error, reason()}.
lookup_operations(Log, Key) ->
    dets:lookup(Log, Key).


%% @doc preflist_member: Returns true if the Partition identifier is part of the Preflist
%%      Input:  Partition: The partition identifier to check
%%              Preflist: A list of pairs {Partition, Node}
%%      Return: true | false
-spec preflist_member(partition(), preflist()) -> boolean().
preflist_member(Partition,Preflist) ->
    lists:any(fun({P,_}) -> P == Partition end, Preflist).

-ifdef(TEST).

%% @doc Testing threshold_prune 
thresholdprune_test() ->
    Operations = [#operation{op_number=op1},#operation{op_number=op2},#operation{op_number=op3},#operation{op_number=op4},#operation{op_number=op5}],
    Filtered = threshold_prune(Operations,op3),
    ?assertEqual([#operation{op_number=op3},#operation{op_number=op4},#operation{op_number=op5}],Filtered).

%% @doc Testing threshold_prune works even when there is no matching op_id
thresholdprune_notmatching_test() ->
    Operations = [#operation{op_number=op1},#operation{op_number=op2},#operation{op_number=op3},#operation{op_number=op4},#operation{op_number=op5}],
    Filtered = threshold_prune(Operations,op6),
    ?assertEqual([],Filtered).

%% @doc Testing remove_node_from_preflist
remove_node_from_preflist_test()->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual([partition1, partition2, partition3], remove_node_from_preflist(Preflist)).

%% @doc Testing get_log_from_map works in both situations, when the key is in the map and when the key is not in the map
get_log_from_map_test() ->
    Dict = dict:new(),
    Dict2 = dict:store([floppy1, c], value1, Dict),
    Dict3 = dict:store([floppy2, c], value2, Dict2),
    Dict4 = dict:store([floppy3, c], value3, Dict3),
    Dict5 = dict:store([floppy4, c], value4, Dict4),
    ?assertEqual({ok, value3}, get_log_from_map(Dict5, [{floppy3,x},{c, x}])),
    ?assertEqual({error, no_log_for_preflist}, get_log_from_map(Dict5, [{floppy5,x}, {c, x}])).

%% @doc Testing that preflist_member returns true when there is a match
preflist_member_true_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(true, preflist_member(partition1, Preflist)).

%% @doc Testing that preflist_member returns false when there is no match
preflist_member_false_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(false, preflist_member(partition5, Preflist)).

-endif.
