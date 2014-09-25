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
                Payload= #operation{op_number = OpId, payload = LogRecord},
                FromDC},
               State = #recvr_state{lastRecvd = LastRecvd, recQ = RecQ}) ->
    LastRecvdNew = set(FromDC, OpId, LastRecvd),
   RecQNew = enqueue(FromDC, {Key,LogRecord}, RecQ),
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
            case orddict:find(Dc, LastCTS) of  % Check for duplicate
                {ok, CTS} ->
                    if Ts >= CTS ->
                            check_and_update(SnapshotTime, Localclock,
                                             Key, LogRecord,
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
                    check_and_update(SnapshotTime, Localclock, Key, LogRecord,
                                     Dc, DcQ, Ts, StateData)

            end;
        true ->
            StateData
    end.

check_and_update(SnapshotTime, Localclock, Key, LogRecord,
                 Dc, DcQ, Ts,
                 StateData = #recvr_state{partition = Partition} ) ->
    Payload = LogRecord#log_record.op_payload,
    case check_dep(SnapshotTime, Localclock) of
        true ->
            case LogRecord#log_record.op_type of
                noop -> %% Heartbeat
                    lager:debug("Heartbeat received");
                _ ->
                    ok = materializer_vnode:update(Key, Payload),
                    lager:debug("Update from remote DC applied:",[payload])
                    %%TODO add error handling if append failed
            end,

            {ok, NewState} = finish_update_dc(
                               Dc, DcQ, Ts, StateData),
            {ok, _} = vectorclock:update_clock(Partition, Dc, Ts),
            %%vectorclock:calculate_stable_snapshot(Partition, Dc, Ts),
            riak_core_vnode_master:command(
              [{Partition,node()}], calculate_stable_snapshot,
              vectorclock_vnode_master),
            riak_core_vnode_master:command({Partition, node()}, {process_queue},
                                           inter_dc_recvr_vnode_master),
            NewState;
        false ->
            lager:debug("Dep not satisfied ~p", [Payload]),
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
    Result = vectorclock:ge(Localclock, DepV),
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
