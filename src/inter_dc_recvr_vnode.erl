-module(inter_dc_recvr_vnode).
-behaviour(riak_core_vnode).
-include("inter_dc_repl.hrl").
-include("floppy.hrl").

-export([start_vnode/1,
                                                %API begin
         store_updates/1,
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

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% public API

store_updates(Updates) ->
    Operation = hd(Updates),
    Logrecord = Operation#operation.payload,
    Payload = Logrecord#log_record.op_payload,
    Op_type = Logrecord#log_record.op_type,
    CommitTime = Payload#clocksi_payload.commit_time,
    {DcId, _Time} = CommitTime,
    case Op_type of 
        noop ->
            Key = Payload#clocksi_payload.key,
            LogId = log_utilities:get_logid_from_partition(Key);
        _ -> 
            Key = Payload#clocksi_payload.key,
            LogId = log_utilities:get_logid_from_key(Key)
    end,
    Preflist = log_utilities:get_preflist_from_logid(LogId),
    Indexnode = hd(Preflist),
    lists:foreach(fun(Update) ->
                           store_update(Indexnode, Key, Update, DcId)
                  end, Updates),
    riak_core_vnode_master:command(Indexnode, {process_queue},
                                   inter_dc_recvr_vnode_master),
    {ok, done}.

%% --------------------
%% Sends update to be replicated to the vnode
%% Args: Key,
%%       Payload contains Operation Timestamp and DepVector for causality tracking
%%       FromDC = DC_ID
%% --------------------
store_update(Node, Key, Logrecord, DcId) ->
    riak_core_vnode_master:sync_command(Node,
                                        {store_update, Key, Logrecord, DcId},
                                        inter_dc_recvr_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    StateFile = string:concat(integer_to_list(Partition), "replstate"),
    Path = filename:join(
             app_helper:get_env(riak_core, platform_data_dir), StateFile),
    case dets:open_file(StateFile, [{file, Path}, {type, set}]) of
        {ok, StateStore} ->
            case dets:lookup(StateStore, recvr_state) of
                %%If file already exists read previous state from it.
                [{recvr_state, State}] -> {ok, State};
                [] ->
                    {ok, State } = inter_dc_repl_update:init_state(Partition),
                    {ok, State#recvr_state{statestore = StateStore}};
                Error -> Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% process one replication request from other Dc. Update is put in a queue for each DC.
%% Updates are expected to recieve in causal order.
handle_command({store_update, Key, Payload, Dc}, _Sender, State) ->
    lager:info(" processing update of ~p",[Key]),
    {ok, NewState} = inter_dc_repl_update:enqueue_update(
                       {Key, Payload, Dc}, State),
    dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {reply, ok, NewState};

handle_command({process_queue}, _Sender, State) ->
    {ok, NewState} = inter_dc_repl_update:process_queue(State),
    dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {noreply, NewState}.

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
