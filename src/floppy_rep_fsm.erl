%% @doc The coordinator for stat write operations.  This example will
%%      show how to properly replicate your data in Riak Core by making
%%      use of the _preflist_.

-module(floppy_rep_fsm).

-behavior(gen_fsm).

-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([prepare/2,
         execute/2,
         wait_append/2,
         wait_read/2,
         repair_rest/2]).

%% The threshold of number of error messages for read/write. When error
%% message reaches this limit, the operation can not succeed.
-define(W_ERROR_THRESHOLD, ?N-?NUM_W+1).
-define(R_ERROR_THRESHOLD, ?N-?NUM_R+1).

-record(state, {from :: pid(),
                operation :: atom(),
                log_id :: log_id(),
                key :: key(),
                type :: type(),
                payload :: payload(),
                readresult,
                error_msg = [],
                preflist :: preflist(),
                num_to_ack = 0 :: non_neg_integer(),
                opid :: op_id(),
                node_ops}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Operation, Key, Type, Payload, OpId) ->
    gen_fsm:start_link(?MODULE, {From, Operation, Key, Type, Payload, OpId}, []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init({From, Operation, Key, Type, Payload, OpId}) ->
    SD = #state{from=From,
                operation=Operation,
                key=Key,
                type=Type,
                payload=Payload,
                opid=OpId,
                readresult=[],
                node_ops=[]},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    LogId = log_utilities:get_logid_from_key(Key),
    Preflist = log_utilities:get_preflist_from_key(Key),
    {next_state, execute, SD0#state{log_id=LogId, preflist=Preflist}, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{operation=Operation,
                            log_id=LogId,
                            payload=Payload,
                            preflist=Preflist,
                            from=From,
                            opid=OpId}) ->
    lager:info("Executing operation: ~p", [Operation]),
    case Operation of
        append ->
            logging_vnode:dappend(Preflist, LogId, OpId, Payload),
            {next_state, wait_append, SD0#state{num_to_ack=?NUM_W},
             ?COMM_TIMEOUT};
        read ->
            logging_vnode:dread(Preflist, LogId),
            {next_state, wait_read, SD0#state{num_to_ack=?NUM_R},
             ?COMM_TIMEOUT};
        read_from ->
            %% Payload identifies the op_id from which operations are returned
            logging_vnode:read_from(Preflist, LogId, Payload),
            SD1 = SD0#state{num_to_ack=?NUM_R},
            {next_state, wait_read, SD1, ?COMM_TIMEOUT};
        _ ->
            floppy_coord_fsm:finish_op(From, error, wrong_command),
            {stop, normal, SD0}
    end.

%% @doc Waits for W write reqs to respond.
wait_append({ok, {_Node, OpId}},
            SD=#state{from=From, num_to_ack=NumToAck}) ->
    case NumToAck of
        1 ->
            lager:debug("All replies received."),
            floppy_coord_fsm:finish_op(From, ok, OpId),
            {stop, normal, SD};
        _ ->
            {next_state, wait_append, SD#state{num_to_ack=NumToAck-1},
             ?COMM_TIMEOUT}
    end;

wait_append({error, Reason}, SD=#state{from=From, error_msg=ErrorMsg}) ->
    ErrorMsg1 = [Reason|ErrorMsg],
    case length(ErrorMsg1) of
        ?W_ERROR_THRESHOLD ->
            floppy_coord_fsm:finish_op(From, error, ErrorMsg1),
            {stop, normal, SD};
        _ ->
            {next_state, wait_append, SD#state{error_msg=ErrorMsg},
             ?COMM_TIMEOUT}
    end;

wait_append(timeout, SD=#state{from=From}) ->
    lager:info("Timeout triggered on append!"),
    floppy_coord_fsm:finish_op(From, error, quorum_unreachable),
    {stop, normal, SD}.

%% @doc Waits for R read reqs to respond and union returned operations.
%%      Then send vnodes operations if they haven't seen some.
%%      Finally reply the unioned operation list to the coord fsm.
wait_read({ok, {Node, Result}}, SD=#state{from=From,
                                          node_ops=NodeOps,
                                          log_id=LogId,
                                          readresult=ReadResult,
                                          num_to_ack=NumToAck}) ->
    NodeOps1 = lists:append([{Node, Result}], NodeOps),
    Result1 = union_ops(ReadResult, [], Result),
    case NumToAck of
        1 ->
            repair(NodeOps1, Result1, LogId),
            floppy_coord_fsm:finish_op(From, ok, Result1),
            {next_state, repair_rest, SD#state{num_to_ack=?N-?NUM_R,
                                               node_ops=NodeOps1,
                                               readresult=Result1},
             ?COMM_TIMEOUT};
        _ ->
            {next_state, wait_read, SD#state{num_to_ack=NumToAck-1,
                                             node_ops=NodeOps1,
                                             readresult=Result1},
             ?COMM_TIMEOUT}
    end;

wait_read({error, Reason}, SD=#state{from=From, error_msg=ErrorMsg}) ->
    ErrorMsg1 = [Reason|ErrorMsg],
    lager:info("Error in performing read operation: ~p", [Reason]),
    case length(ErrorMsg1) of
        ?R_ERROR_THRESHOLD ->
            floppy_coord_fsm:finish_op(From, error, ErrorMsg1),
            {stop, normal, SD};
        _ ->
            {next_state, wait_read, SD#state{error_msg=ErrorMsg},
             ?COMM_TIMEOUT}
    end;

wait_read(timeout, SD=#state{from=From}) ->
    lager:info("Timeout triggered on read!"),
    floppy_coord_fsm:finish_op(From, error, quorum_unreachable),
    {stop, normal, SD}.

%% @doc Keeps waiting for read replies from vnodes after replying to
%%      coord fsm.
%%      Do read repair if any of them haven't seen some ops.
repair_rest(timeout,
            SD=#state{node_ops=NodeOps, readresult=ReadResult,
                      log_id=LogId}) ->
    lager:debug("FSM: read repair timeout! Start repair anyway~n"),
    repair(NodeOps, ReadResult, LogId),
    {stop, normal, SD};

repair_rest({ok, {Node, Result}},
            SD=#state{num_to_ack=NumToAck, node_ops=NodeOps,
                      readresult=ReadResult, log_id=LogId}) ->
    NodeOps1 = lists:append([{Node, Result}], NodeOps),
    Result1 = union_ops(ReadResult, [], Result),
    case NumToAck of
        1 ->
            repair(NodeOps, ReadResult, LogId),
            {stop, normal, SD};
        _ ->
            {next_state, repair_rest, SD#state{num_to_ack=NumToAck-1,
                                               node_ops=NodeOps1,
                                               readresult=Result1},
             ?COMM_TIMEOUT}
    end;

repair_rest({error, Reason}, SD) ->
    lager:info("Error in read repair: ~p", [Reason]),
    {next_state, repair_rest, SD, ?COMM_TIMEOUT}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop ,badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, _StateData) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Find operations that are in List2 but not in List1
-spec find_diff_ops(list(),list()) -> list().
find_diff_ops(List1, List2) ->
    lists:filter(fun(X)-> lists:member(X, List1) == false end, List2).

%% @doc Make a union of operations in L1 and L2
-spec union_ops([{_,operation()}],[{_,operation()}],[{_,operation()}]) ->
    [{_,operation()}].
union_ops(L1, L2, []) ->
    lists:append(L1, L2);
union_ops(L1, L2, [Op|T]) ->
    {_, #operation{op_number=OpId}} = Op,
    L3 = remove_dup(L1, OpId,[]),
    L4 = lists:append(L2, [Op]),
    union_ops(L3, L4, T).

%% @doc Send logging vnodes with operations that they havn't seen
-spec repair([{node(),[operation()]}],[operation()], log_id()) -> ok.
repair([], _, _) ->
    ok;
repair([H|T], FullOps, LogId) ->
    {Node, Ops} = H,
    DiffOps = find_diff_ops(Ops, FullOps),
    case DiffOps /= [] of
       true ->
            logging_vnode:append_list(Node, LogId, DiffOps),
            repair(T, FullOps, LogId);
       false ->
            repair(T, FullOps, LogId)
    end.

%% @doc Remove duplicate ops with OpId from a list (the first param)
%%      and return a list with only one operation with OpId
-spec remove_dup([{_,operation()}],op_id(),[{_,operation()}]) -> [{_,operation()}].
remove_dup([], _OpId, Set2) ->
    Set2;
remove_dup([H|T], OpId, Set2) ->
    {_, #operation{op_number=OpNum}} = H,
    Set3 = case OpNum /= OpId of
        true ->
            lists:append(Set2, [H]);
        false ->
            Set2
    end,
    remove_dup(T, OpId, Set3).

-ifdef(TEST).
union_test()->
    Result = union_ops([{nothing, {operation,{1,node1},op1}}, {nothing,{operation,{2,node1},op2}}],[],[{nothing,{operation,{2,node1},op3}}, {nothing,{operation,{2,node2},op4}}]),
    ?assertEqual(lists:sort(Result), lists:sort([{nothing,{operation,{1,node1},op1}}, {nothing,{operation,{2,node1},op3}}, {nothing,{operation,{2,node2},op4}}])),
    Result1 = union_ops([{nothing, {operation,{1,node1},op1}}], [], [{nothing,{operation,{1,node2},op2}}, {nothing, {operation,{2,node2},op3}}]),
    ?assertEqual(lists:sort(Result1), lists:sort([{nothing, {operation,{1,node1},op1}}, {nothing,{operation,{1,node2},op2}}, {nothing,{operation,{2,node2},op3}}])),
    Result2 = union_ops([{nothing, {operation,{1,node1},op1}}], [], [{nothing,{operation,{1,node1},op3}}]),
    ?assertEqual(length(Result2), 1).
-endif.
