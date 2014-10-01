-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
         %%API begin
         %%trigger/2,
         %% trigger/1,
         %% sync_clock/2,
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
                last_op=empty,
                reader}).

-define(REPL_PERIOD, 5000).

start_vnode(I) ->
    {ok, Pid} = riak_core_vnode_master:get_vnode_pid(I, ?MODULE),
    riak_core_vnode:send_command(Pid, trigger),
    {ok, Pid}.

%% riak_core_vnode call backs
init([Partition]) ->
    DcId = dc_utilities:get_my_dc_id(),
    {ok, Reader} = clocksi_transaction_reader:init(Partition),
    {ok, #state{partition=Partition,
                dcid=DcId,
                reader = Reader}}.

handle_command(trigger, _Sender, State=#state{partition=Partition,
                                              reader=Reader}) ->
    {NewReaderState, Transactions} =
        clocksi_transaction_reader:get_next_transactions(Reader),
    {ok, DCs} = inter_dc_manager:get_dcs(),
    case Transactions of
        [] ->
            %% Send heartbeat
            Heartbeat = [#operation
                         {payload =
                              #log_record{op_type=noop, op_payload = Partition}
                         }],
            DcId = dc_utilities:get_my_dc_id(),
            {ok, Clock} = vectorclock:get_clock(Partition),
            Time = clocksi_transaction_reader:get_prev_stable_time(NewReaderState),
            TxId = 0,
            %% Receiving DC treats hearbeat like a transaction
            %% So wrap heartbeat in a transaction structure
            Transaction = {TxId, {DcId, Time}, Clock, Heartbeat},
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, [Transaction]}, DCs) of
                ok ->
                    NewReader = NewReaderState;
                _ ->
                    NewReader = NewReaderState
            end;
        [_H|_T] ->
            case inter_dc_communication_sender:propagate_sync(
                   {replicate, Transactions}, DCs) of
                ok ->
                    NewReader = NewReaderState;
                _ ->
                    NewReader = Reader
            end
    end,
    timer:sleep(?REPL_PERIOD),
    riak_core_vnode:send_command(self(), trigger),
    {reply, ok, State#state{reader=NewReader}}.

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
