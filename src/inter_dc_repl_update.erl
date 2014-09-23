-module(inter_dc_repl_update).

-include("inter_dc_repl.hrl").
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init_state/1, enqueue_update/2, process_queue/1]).

init_state(Partition) ->
    {ok, #recvr_state{lastRecvd = orddict:new(), %% stores last OpId received
                      lastCommitted = orddict:new(),
                      recQ = orddict:new(),
                      dcs = [1,2],
                      partition=Partition}
    }.

enqueue_update({Key,
                Payload= #operation{op_number = OpId, payload = LogRecord}, FromDC},
               State = #recvr_state{lastRecvd = LastRecvd, recQ = RecQ}) ->
    lager:info("Process key"),
    LastRecvdNew = set(FromDC, OpId, LastRecvd),
    lager:info("enquing ~p",[Key]),
    RecQNew = enqueue(FromDC, {Key,LogRecord}, RecQ),
    lager:info("Op Enqueued ~p",[Payload]),
    lager:info("NewQ ~p",[RecQNew]),
    {ok, State#recvr_state{lastRecvd = LastRecvdNew, recQ = RecQNew}}.

%% Process one update from Q for each DC each Q.
%% This method must be called repeatedly
%% inorder to process all updates
process_queue(State=#recvr_state{recQ = RecQ}) ->
    NewState = orddict:fold(
                 fun(K, V, Res) ->
                         process_q_dc(K, V, Res)
                 end, State, RecQ),
    {ok, NewState}.

%% private functions

%%Takes one update from DC queue, checks whether its depV is satisfied and apply the update locally.
process_q_dc(Dc, DcQ, StateData=#recvr_state{lastCommitted = LastCTS,
                                             partition = Partition}) ->
    case queue:is_empty(DcQ) of
        false ->
            {Key, LogRecord} = queue:get(DcQ),
            Payload = LogRecord#log_record.op_payload,
            CommitTime = Payload#clocksi_payload.commit_time,
            SnapshotTime = vectorclock:set_clock_of_dc(
                             Dc, 0, Payload#clocksi_payload.snapshot_time),
            {Dc, Ts} = CommitTime,
            %% Check for dependency of operations and write to log
            {ok, LC} = vectorclock:get_clock(Partition),
            Localclock = vectorclock:set_clock_of_dc(Dc, 0, LC),
            lager:info(" Localclock of recvr ~p",[Localclock]),
            case orddict:find(Dc, LastCTS) of  % Check for duplicate
                {ok, CTS} ->
                    if Ts >= CTS ->
                            check_and_update(SnapshotTime, Localclock, Key, LogRecord,
                                             Dc, DcQ, Ts, StateData ) ;
                       true ->
                            %% TODO: Not right way check duplicates
                            lager:info("Duplicate request"),
                            {ok, NewState} = finish_update_dc(
                                               Dc, DcQ, CTS, StateData),
                            %%Duplicate request, drop from queue
                            NewState
                    end;
                _ ->
                    check_and_update(SnapshotTime, Localclock, Key, Payload,
                                     Dc, DcQ, Ts, StateData)

            end;
        true ->
            StateData
    end.

check_and_update(SnapshotTime, Localclock, Key, LogRecord, Dc, DcQ, Ts, StateData = #recvr_state{partition = Partition} ) ->
    Payload = LogRecord#log_record.op_payload,
    case check_dep(SnapshotTime, Localclock) of
        true ->
            case LogRecord#log_record.op_type of
                noop -> %% Heartbeat
                    lager:debug("Heartbeat received");
                _ ->
                    lager:info("Updating to materializer"),
                    ok = materializer_vnode:update(Key, Payload)
                    %%TODO add error handling if append failed
            end,

            {ok, NewState} = finish_update_dc(
                               Dc, DcQ, Ts, StateData),
            {ok, _} = vectorclock:update_clock(Partition, Dc, Ts),
            riak_core_vnode_master:command({Partition, node()}, {process_queue},
                                           inter_dc_recvr_vnode_master),
            NewState;
        false ->
            lager:info("Dep not satisfied ~p", [Payload]),
            StateData
    end.

finish_update_dc(Dc, DcQ, Cts,
                 State=#recvr_state{lastCommitted = LastCTS, recQ = RecQ}) ->
    DcQNew = queue:drop(DcQ),
    RecQNew = set(Dc, DcQNew, RecQ),
    LastCommNew = set(Dc, Cts, LastCTS),
    {ok, State#recvr_state{lastCommitted = LastCommNew, recQ = RecQNew}}.

%% Checks depV against the committed timestamps
check_dep(DepV, Localclock) ->
    lager:info("Compare ~p  >  ~p",[Localclock , DepV]),
    Result = vectorclock:ge(Localclock, DepV),
    lager:info("Compare result ~p",[Result]),
    Result.

%%Set a new value to the key.
set(Key, Value, Orddict) ->
    orddict:update(Key, fun(_Old) -> Value end, Value, Orddict).

%%Put a value to the Queue corresponding to Dc in RecQ orddict
enqueue(Dc, Data, RecQ) ->
    case orddict:find(Dc, RecQ) of
        {ok, Q} ->
            Q2 = queue:in(Data, Q),
            set(Dc, Q2, RecQ);
        error -> %key does not exist
            Q = queue:new(),
            Q2 = queue:in(Data,Q),
            set(Dc, Q2, RecQ)
    end.

%% -ifdef(TEST).
%% get_first(Dc, RecQ) ->
%%     case orddict:find(Dc, RecQ) of
%%         {ok, DcQ} ->
%%             case queue:is_empty(DcQ) of
%%                 false ->
%%                     {Key, {Op, Ts, DepV}} = queue:get(DcQ),
%%                     {ok, {Key, {Op, Ts, DepV}}};
%%                 true ->
%%                     {ok, null}
%%             end;
%%         error ->
%%             {error, queue_not_found}
%%     end.

%% %% Eunit Tests


%% init_update_test_() ->
%%     {ok, State1} = init_state(1),
%%     Payload1 = {{increment,a}, 1, {0,0}},
%%     {ok, State2} = enqueue_update({a, Payload1, 1}, State1),
%%     RecQ1 = State2#recvr_state.recQ,

%%     Payload2 = {{decrement,a}, 1, {0,0}},
%%     {ok, State3} = enqueue_update({a, Payload2, 1}, State2),
%%     RecQ2 = State3#recvr_state.recQ,

%%     Payload3 = {{increment,b},2,{0,0}},
%%     {ok, State4} = enqueue_update({a, Payload3, 2}, State3),
%%     RecQ3 = State4#recvr_state.recQ,
%%     [?_assert({ok, {a,Payload1}} =:= get_first(1, RecQ1)),
%%      ?_assert({ok, {a,Payload1}} =:= get_first(1, RecQ2)),
%%      ?_assert({ok, {a,Payload3}} =:= get_first(2, RecQ3))].


%% finish_update_dc_test_() ->
%%     {ok, State1 } = init_state(1),
%%     Payload1 = {{increment,a},1,{0,0}},
%%     Dc = 1,
%%     Ts = 1,
%%     {ok, State2} = enqueue_update({a, Payload1, Dc}, State1),
%%     {ok,DcQ} = orddict:find(Dc, State2#recvr_state.recQ),
%%     {ok, State3} = finish_update_dc(Dc, DcQ, Ts, State2),
%%     RecQ = State3#recvr_state.recQ,
%%     [?_assert({ok,null} =:= get_first(Dc,RecQ))] .

%% check_dep_test_() ->
%%     DepV = vectorclock:from_list([{1,4}, {2,3}, {3,1}, {4,2}]),
%%     LastCTS1 = vectorclock:from_list([{1,3},{2,2},{3,0},{4,0}]),
%%     LastCTS2 = vectorclock:from_list([{1,4},{2,2},{3,1},{4,2}]),
%%     LastCTS3 = vectorclock:from_list([{1,4},{2,3},{3,1},{4,3}]),
%%     LastCTS4 = vectroclock:from_list([{1,4}]),
%%     [ ?_assert(check_dep(DepV, LastCTS1) =:= false),
%%       ?_assert(check_dep(DepV, LastCTS2) =:= false),
%%       ?_assert(check_dep(DepV, LastCTS3) =:= true),
%%       ?_assert(check_dep(DepV, LastCTS4) =:= false)
%%     ].

%% -endif.
