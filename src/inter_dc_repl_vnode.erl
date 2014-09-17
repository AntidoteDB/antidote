-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
         %%API begin
         %%trigger/2,
         trigger/1,
         sync_clock/2,
         get_update/3,
         %%API end
         init/1,
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

-record(state, {partition,
                last_op=empty}).

-define(RETRY_TIME, 5000).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% public API
trigger(IndexNode, Key) ->
    riak_core_vnode_master:command(IndexNode, {trigger,Key},
                                   inter_dc_repl_vnode_master).
trigger(Key) ->
     LogId = log_utilities:get_logid_from_key(Key),
     Preflist = log_utilities:get_preflist_from_logid(LogId),
     IndexNode = hd(Preflist),
     trigger(IndexNode, Key).

sync_clock(Partition, Clock) ->
    %Preflist = log_utilitities:get_preflist_from_partition(Partition),
    %Indexnode = hd(Preflist),
    riak_core_vnode_master:command({Partition, node()}, {sync_clock, Clock},
                                   inter_dc_repl_vnode_master).

%%TODO: Not implemented
get_update(DcId, FromOp, Partition) ->
    Preflist = log_utilitities:get_preflist_from_partition(Partition),
    Indexnode = hd(Preflist),
    riak_core_vnode_master:command(Indexnode, {get_update, FromOp, DcId},
                                   inter_dc_repl_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    {ok, #state{partition=Partition}}.

handle_command({sync_clock, Clock}, _Sender, State) ->
    prepare_and_send_ops([],Clock, State),
    {reply, ok, State};
handle_command({trigger,Key}, _Sender, State=#state{partition=Partition,
                                                     last_op=Last}) ->
    {ok, Clock} = vectorclock:get_clock_by_key(Key),
    case Last of
        empty ->
            case floppy_rep_vnode:read(Key, riak_dt_gcounter) of
                {ok, Ops} ->
                    OpDone = prepare_and_send_ops(Ops,Clock,State);
                {error, _Reason} ->
                    OpDone = Last,
                    timer:sleep(?RETRY_TIME),
                    trigger({Partition, node()}, Key)
            end;
        _ ->
            case floppy_rep_vnode:read_from(Key, riak_dt_gcounter, Last) of
                {ok, Ops} ->
                    OpDone = prepare_and_send_ops(Ops,Clock,State);
                {error, _Reason} ->
                    OpDone = Last,
                    timer:sleep(?RETRY_TIME),
                    trigger({Partition, node()})
            end
    end,
    %trigger({Partition, node()})
    {reply, ok, State#state{last_op=OpDone}}.

%% handle_command({get_update, FromOp, ReqDc}, _Sender, State=#state{partition=Partition}) ->
%%     {ok, Clock} = vectorclock:get_clock(Partition),
%%     case FromOp of
%%         empty ->
%%             case floppy_rep_vnode:read(Key, riak_dt_gcounter) of
%%                 {ok, Ops} ->
%%                     OpDone = prepare_and_send_ops(Ops,Clock,ReqDc,State);
%%                 {error, _Reason} ->
%%                     lager:debug("Error reading from flopp_rep")
%%             end;
%%         _ ->
%%             case floppy_rep_vnode:read_from(Key, riak_dt_gcounter, FromOp) of
%%                 {ok, Ops} ->
%%                     OpDone = prepare_and_send_ops(Ops, Clock, State);
%%                 {error, _Reason} ->
%%                     lager:debug("Error reading from flopp_rep")
%%             end
%%     end,
%%     {reply, ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

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

%% Filter Ops to the form understandable by recvr and propagate
prepare_and_send_ops(Ops, Clock, _State = #state{partition=Partition,
                                              last_op=LastOpId}) ->
    case Ops of
        %% if empty, there are no updates
        [] ->
            %% get current vectorclock of node
            %% propogate vectorclock to other DC
            DcId = dc_utilities:get_my_dc_id(),
            {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, Clock),
            Op = #clocksi_payload{key = Partition,
                                  commit_time={DcId, LocalClock},
                                  snapshot_time = vectorclock:from_list([])
                                 },
            Payload=#operation{payload = #log_record
                               {op_type=noop, op_payload=Op}},
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, [Payload]}) of
                done ->
                    Done = LastOpId;
                Other ->
                    Done = LastOpId,
                    lager:info(
                      "Propagation error. Reason: ~p",[Other])
            end;
            %Done = LastOpId; %%TODO:
        _ ->
            Downstreamops = filter_downstream(Ops),
            lager:info("Ops to replicate ~p",[Downstreamops]),
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, Downstreamops}) of
                ok ->
                    Done = get_last_opid(Ops, LastOpId);
                _ ->
                    Done = LastOpId
            end
    end,
    lager:info("Reset lastopid to ~p",[Done]),
    Done.

filter_downstream(Ops) ->
    DcId = dc_utilities:get_my_dc_id(),
    lists:filtermap(fun({_LogId, Operation}) ->
                            Op = Operation#operation.payload,
                            case Op#log_record.op_type of
                                downstreamop ->
                                    DownOp = Op#log_record.op_payload,
                                    case DownOp#clocksi_payload.commit_time of
                                        {DcId,_Time} ->
                                            %% Op is committed in this DC
                                            {true, Operation};
                                        _ -> {false}
                                    end;
                                _ ->
                                    false
                            end
                    end, Ops).

get_last_opid(Ops, Last) ->
    case Ops of
        [] -> Last;
        [_H|_T] ->
            {_LogId, Op} = lists:last(Ops),
            Op#operation.op_number
    end.
