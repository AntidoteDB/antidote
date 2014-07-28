%% @doc The coordinator for stat write operations.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(floppy_rep_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, wait_append/2, wait_read/2, repair_rest/2 ]).

%% The threshold of number of error messages for read/write. When error
%% message reaches this limit, the operation can not succeed.
-define(W_ERROR_THRESHOLD, ?N-?NUM_W+1).
-define(R_ERROR_THRESHOLD, ?N-?NUM_R+1).

-record(state, {
          from :: pid(),
          op :: op(),
          key :: key(),
          param = undefined :: term() | undefined,
          readresult,
          error_msg = [],
          preflist :: preflist(),
          num_to_ack = 0 :: non_neg_integer(),
          opid :: op_id(),
          node_ops}).

-type state() :: #state{}.
%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(pid(), op(), key(), term(), op_id()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link(From, Op,  Key, Param, OpId) ->
    gen_fsm:start_link(?MODULE, {From, Op, Key, Param, OpId}, []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
-spec init({pid(), op(), key(), term(), op_id()}) -> {ok,prepare,state(),non_neg_integer()}.
init({From, Op,  Key, Param, OpId}) ->
    SD = #state{from=From,
                op=Op,
                key=Key,
                param=Param,
                opid= OpId,
                readresult=[],
                node_ops =[]},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
-spec prepare(timeout, state()) -> {next_state,execute,state(),0}.
prepare(timeout, SD0=#state{key=Key}) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, ?N, logging),
    NewPref = [Node|| {Node,_} <- Preflist ],
    SD = SD0#state{preflist=NewPref},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
-spec execute(timeout,state()) -> {next_state,wait_append | wait_read,state(), 500} | {stop,normal,state()}.
execute(timeout, SD0=#state{op=Op,
                            key=Key,
                            param=Param,
                            preflist=Preflist,
                            from=From,
                            opid=OpId}) ->
    case Op of
        append ->
            lager:info("FSM: Replicate append ~n"),
            logging_vnode:dappend(Preflist, Key, Param, OpId),
            SD1 = SD0#state{num_to_ack=?NUM_W},
            {next_state, wait_append, SD1, ?COMM_TIMEOUT};
        read ->
            lager:info("FSM: Replication read ~n"),
            logging_vnode:dread(Preflist, Key),
            SD1 = SD0#state{num_to_ack=?NUM_R},
            {next_state, wait_read, SD1, ?COMM_TIMEOUT};
        _ ->
            lager:info("FSM: Wrong command ~n"),
            floppy_coord_fsm:finish_op(From, error, wrong_command),
            %%reply message to user
            {stop, normal, SD0}
    end.


%% @doc Waits for W write reqs to respond.
-spec wait_append({ok, {node(),term()}} | {error,reason()} | timeout,state()) -> {stop,normal,state()} | {next_state,wait_append,state(),500}.
wait_append({ok,{_Node, Result}}, SD=#state{op=Op, from= From, key=Key, num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            lager:info("FSM: Finish collecting replies for ~w ~n", [Op]),
            floppy_coord_fsm:finish_op(From, ok, {Key, Result}),
            {stop, normal, SD};
        _ ->
            lager:info("FSM: Keep collecting replies~n"),
            {next_state, wait_append, SD#state{num_to_ack= NumToAck-1 }, ?COMM_TIMEOUT}
    end;

wait_append({error, Reason}, SD=#state{from=From, error_msg=ErrorMsg}) ->
    ErrorMsg1 = [Reason|ErrorMsg],
    lager:info("FSM: Error in append: ~w", [Reason]),
    case length(ErrorMsg1) of
        ?W_ERROR_THRESHOLD ->
            floppy_coord_fsm:finish_op(From, error, ErrorMsg1),
            {stop, normal, SD};
        _ ->
            {next_state, wait_append, SD#state{error_msg=ErrorMsg}, ?COMM_TIMEOUT}
    end;

wait_append(timeout, SD=#state{from=From}) ->
    lager:info("FSM: Error in append: timeout"),
    floppy_coord_fsm:finish_op(From, error, quorum_unreachable),
    {stop, normal, SD}.

%% @doc Waits for R read reqs to respond and union returned operations.
%% Then send vnodes operations if they haven't seen some.
%% Finally reply the unioned operation list to the coord fsm.
-spec wait_read({ok,{node(),term()}} | {error,reason()} | timeout, state()) -> {next_state, repair_rest|wait_read,state(),500} | {stop,normal,state()}. 
wait_read({ok,{Node, Result}}, SD=#state{op=Op, from= From, node_ops = NodeOps, key=Key, readresult= ReadResult , num_to_ack= NumToAck}) ->
    NodeOps1 = lists:append([{Node, Result}], NodeOps),
    Result1 = union_ops(ReadResult, [], Result),
    %%lager:info("FSM: Get reply ~w ~n, unioned reply ~w ~n",[Result,Result1]),
    case NumToAck of
        1 ->
            lager:info("FSM: Finish reading for ~w ~w ~n", [Op, Key]),
            repair(NodeOps1, Result1),
            floppy_coord_fsm:finish_op(From, ok, {Key, Result1}),
            {next_state, repair_rest, SD#state{num_to_ack = ?N-?NUM_R, node_ops=NodeOps1, readresult = Result1}, ?COMM_TIMEOUT};
        _ ->
            lager:info("FSM: Keep collecting replies~n"),
            {next_state, wait_read, SD#state{num_to_ack= NumToAck-1, node_ops= NodeOps1, readresult = Result1}, ?COMM_TIMEOUT}
    end;

wait_read({error, Reason}, SD=#state{from=From, error_msg=ErrorMsg}) ->
    ErrorMsg1 = [Reason|ErrorMsg],
    lager:info("FSM: Error in read: ~w", [Reason]),
    case length(ErrorMsg1) of
        ?R_ERROR_THRESHOLD ->
            floppy_coord_fsm:finish_op(From, error, ErrorMsg1),
            {stop, normal, SD};
        _ ->
            {next_state, wait_read, SD#state{error_msg=ErrorMsg}, ?COMM_TIMEOUT}
    end;

wait_read(timeout, SD=#state{from=From}) ->
    lager:info("FSM: Error in read: timeout"),
    floppy_coord_fsm:finish_op(From, error, quorum_unreachable),
    {stop, normal, SD}.

%% @doc Keeps waiting for read replies from vnodes after replying to coord fsm.
%% Do read repair if any of them haven't seen some ops.
-spec repair_rest(timeout | {ok,{node(),term()}} | {error,reason()}, state()) -> {next_state,repair_rest,state(),500} | {stop,normal,state()}.
repair_rest(timeout, SD= #state{node_ops=NodeOps, readresult = ReadResult}) ->
    lager:info("FSM: read repair timeout! Start repair anyway~n"),
    repair(NodeOps, ReadResult),
    {stop, normal, SD};

repair_rest({ok, {Node, Result}}, SD=#state{num_to_ack = NumToAck, node_ops = NodeOps, readresult = ReadResult}) ->
    NodeOps1 = lists:append([{Node, Result}], NodeOps),
    Result1 = union_ops(ReadResult, [], Result),
    case NumToAck of
        1 ->
            lager:info("FSM: Finish collecting replies, start read repair ~n"),
            repair(NodeOps, ReadResult),
            {stop, normal, SD};
        _ ->
            lager:info("FSM: Keep collecting replies~n"),
            {next_state, repair_rest, SD#state{num_to_ack= NumToAck-1, node_ops= NodeOps1, readresult = Result1}, ?COMM_TIMEOUT}
    end;

repair_rest({error, Reason}, SD) ->
    lager:info("FSM: Error in read repair: ~w", [Reason]),
    {next_state, repair_rest, SD, ?COMM_TIMEOUT}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _StateName, _StateData) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Find operations that are in List2 but not in List1
-spec find_diff_ops(list(),list()) -> list().
find_diff_ops(List1, List2) ->
    lists:filter(fun(X)-> lists:member(X, List1) == false end , List2).

%% @doc Make a union of operations in L1 and L2
%% TODO: Possible to simplify?
%% FIXME: Add missing tests! And make sure that the tests also deal with corner cases, e.g. empty lists.
-spec union_ops([{_,operation()}],[{_,operation()}],[{_,operation()}]) -> [{_,operation()}].
union_ops(L1, L2, []) ->
    lists:append(L1, L2);
union_ops(L1, L2, [Op|T]) ->
    {_, #operation{op_number= OpId}} = Op,
    L3 = remove_dup(L1, OpId,[]),
    L4 = lists:append(L2, [Op]),
    union_ops(L3, L4, T).

%% @doc Send logging vnodes with operations that they havn't seen
-spec repair([{node(),[operation()]}],[operation()]) -> ok.
repair([], _) ->
    ok;
repair([H|T], FullOps) ->
    {Node, Ops} = H,
    DiffOps = find_diff_ops(Ops, FullOps),
    case DiffOps /= [] of
       true ->
            logging_vnode:append_list(Node, DiffOps),
            repair(T, FullOps);
       false ->
            repair(T, FullOps)
    end.

%% @doc Remove duplicate ops with OpId from a list (the first param)
%% and return a list with only one operation with OpId
-spec remove_dup([{_,operation()}],op_id(),[{_,operation()}]) -> [{_,operation()}].
remove_dup([], _OpId, Set2) ->
    Set2;
remove_dup([H|T], OpId, Set2) ->
    {_, #operation{op_number= OpNum}} = H,
    Set3 = case OpNum /= OpId of
              true -> lists:append(Set2, [H]);
              false -> Set2
           end,
    remove_dup(T, OpId, Set3).


-ifdef(TEST).
union_test()->
    Result = union_ops([{nothing, {operation,1,dwaf}}, {nothing, {operation,2,fasd}}],[],[{nothing, {operation,1, dwaf}}, {nothing, {operation,3, dafds}}]),
    ?assertEqual(Result, [{nothing, {operation, 2, fasd}}, {nothing, {operation,1, dwaf}}, {nothing, {operation, 3, dafds}}]),
    Result1 = union_ops([{nothing, {operation,2,fasd}}], [], [{nothing, {operation,1, dwaf}}, {nothing, {operation,3, dafds}}]),
    ?assertEqual(Result1, [{nothing, {operation, 2, fasd}}, {nothing, {operation,1, dwaf}}, {nothing, {operation, 3, dafds}}]),
    Result2 = union_ops([{nothing, {operation,2,fasd}}], [], [{nothing, {operation,2, fasd}}]),
    ?assertEqual(Result2, [{nothing, {operation, 2, fasd}}]).
-endif.
