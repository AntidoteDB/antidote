-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
         %%API begin
         %%trigger/2,
         trigger/1,
         sync_clock/2,
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
                dcid,
                last_op=empty}).

-define(RETRY_TIME, 5000).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% public API
trigger(IndexNode, Key) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {trigger, Key},
                                        inter_dc_repl_vnode_master).
trigger(Key) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    trigger(IndexNode, Key).

sync_clock(Partition, Clock) ->
    riak_core_vnode_master:sync_command({Partition, node()}, {sync_clock, Clock},
                                   inter_dc_repl_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    DcId=dc_utilities:get_my_dc_id(),
    {ok, #state{partition=Partition,dcid=DcId}}.

handle_command({sync_clock, Clock}, _Sender, State) ->
    prepare_and_send_ops([],Clock, State),
    {reply, ok, State};
handle_command({trigger,Key}, _Sender, State=#state{partition=Partition,
                                                    last_op=Last}) ->
    {ok, Clock} = vectorclock:get_clock_by_key(Key),
    case Last of
        empty ->
            LogId = log_utilities:get_logid_from_key(Key),
            case logging_vnode:read({Partition, node()},LogId) of
                {ok, Ops} ->
                    OpDone = prepare_and_send_ops(Ops,Clock,State);
                {error, _Reason} ->
                    OpDone = Last
            end;
        _ ->
            LogId = log_utilities:get_logid_from_key(Key),
            case logging_vnode:read_from({Partition, node()}, LogId, Last) of
                {ok, Ops} ->
                    OpDone = prepare_and_send_ops(Ops,Clock,State);
                {error, _Reason} ->
                    OpDone = Last
            end
    end,
                                                %trigger({Partition, node()})
    {reply, ok, State#state{last_op=OpDone}}.

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
                                                 last_op=LastOpId,
                                                 dcid=DcId}) ->
    {ok, DCs} = inter_dc_manager:get_dcs(),
    case Ops of
        %% if empty, there are no updates
        [] ->
            %% get current vectorclock of node
            %% propogate vectorclock to other DC
            {ok, LocalClock} = vectorclock:get_clock_of_dc(DcId, Clock),
            Op = #clocksi_payload{key = Partition,
                                  commit_time={DcId, LocalClock},
                                  snapshot_time = Clock
                                 },
            Payload=#operation{payload = #log_record
                               {op_type=noop, op_payload=Op}},
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, [Payload]}, DCs) of
                ok ->
                    Done = LastOpId;
                Other ->
                    Done = LastOpId,
                    lager:info(
                      "Propagation error. Reason: ~p",[Other])
            end;
        _ ->
            Downstreamops = filter_downstream(Ops),
            lager:error("X1 propagation ~p",[Downstreamops]),
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, Downstreamops}, DCs) of
                ok ->
                    Done = get_last_opid(Ops, LastOpId);
                _ ->
                    Done = LastOpId
            end
    end,
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
                                        _ -> false
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
