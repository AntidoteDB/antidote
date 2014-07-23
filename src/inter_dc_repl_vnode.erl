-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
         %API begin
         trigger/1,
         %API end
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
trigger(IndexNode) ->
    riak_core_vnode_master:command(IndexNode, {trigger}, inter_dc_repl_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    {ok, #state{partition=Partition}}.

handle_command({trigger}, _Sender, State=#state{partition=Partition, last_op=Last}) ->
    Log = log_utilities:get_logid_from_partition(Partition),
    case Last of
        empty ->
            case floppy_rep_vnode:read(Log) of
                {ok, Ops} ->
                    Last2 = inter_dc_repl:propagate_sync(Ops);
                {error, nothing} ->
                    Last2 = Last,
                    timer:sleep(?RETRY_TIME),
                    trigger({Partition, node()})
            end;
        _ ->
            case floppy_rep_vnode:threshold_read(Log, Last) of
                {ok, Ops} ->
                    Last2 = inter_dc_repl:propagate_sync(Ops);
                {error, nothing} ->
                    Last2 = Last,
                    timer:sleep(?RETRY_TIME),
                    trigger({Partition, node()})
            end
    end,
    {noreply, State#state{last_op=Last2}}.

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
