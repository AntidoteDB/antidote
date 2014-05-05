-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/4,
	 update_data_item/4,
	 prepare/2,
	 commit/2,
	 abort/2,
  	 notify_commit/2,
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

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition,log, pending}).


%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
    
read_data_item(Node, Tx, Key, Type) ->
    riak_core_vnode_master:sync_command(Node, {read_data_item, Tx, Key, Type}, ?CLOCKSIMASTER).

update_data_item(Node, Tx, Key, Op) ->
    riak_core_vnode_master:sync_command(Node, {update_data_item, Tx, Key, Op}, ?CLOCKSIMASTER).

prepare(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {prepare, Tx}, ?CLOCKSIMASTER).

commit(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {commit, Tx}, ?CLOCKSIMASTER).

abort(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {abort, Tx}, ?CLOCKSIMASTER).

notify_commit(Node, Tx) ->
    riak_core_vnode_master:sync_command(Node, {notify_commit, Tx}, ?CLOCKSIMASTER).

init([Partition]) ->
    % It generates a dets file to store active transactions.
    TxLogFile = string:concat(integer_to_list(Partition), "clockSI_tx"),
    TxLogPath = filename:join(
            app_helper:get_env(riak_core, platform_data_dir), TxLogFile),
    case dets:open_file(TxLogFile, [{file, TxLogPath}, {type, bag}]) of
        {ok, TxLog} ->
	    Pendings=pendings,
	    ets:new(Pendings, [bag, named_table, public]),
            {ok, #state{partition=Partition, log=TxLog, pending=Pendings}};
        {error, Reason} ->
            {error, Reason}
    end.

handle_command({read_data_item, Tx, Key, Type}, Sender, #state{partition= Partition, log=_Log}=State) ->
    Vnode={Partition, node()},
    clockSI_readitem_fsm:start_link(Vnode, Sender, Tx, Key, Type),
    {no_reply, State};

handle_command({update_data_item, Tx, Key, Op}, Sender, #state{log=_Log}=State) ->
    clockSI_updateitem_fsm:start_link(Sender, Tx, Key, Op),
    {no_reply, State};

handle_command({prepare, Tx}, _Sender, #state{log=Log}=State) ->
    case certification_check(Tx,Log) of
    true ->
	%TODO: Log T.writeset and T.origin
	NewTx=Tx#tx{state=prepared, prepare_time=now_milisec(erlang:now())},
	{reply, NewTx#tx.prepare_time, State};
    false ->
	%This does not match Ale's coordinator. We have to discuss it. It also expect Node.
	{reply, abort, State}
    end;

handle_command({commit, Tx}, _Sender, #state{log=_Log, pending=Pendings}=State) ->
    %TODO: log commit time.
    Processes=ets:lookup(Pendings, Tx#tx.id),
    notify_all(Processes, {committed, Tx#tx.id}),
    {reply, done, State};

handle_command({abort, _Tx}, _Sender, #state{log=_Log}=State) ->
    %TODO: abort tx
    {no_reply, State};

handle_command({notify_commit, TXs}, Sender, #state{pending=Pendings}=State) ->
    %TODO: Check if anyone is already commited 
    add_list_to_pendings(TXs, Sender, Pendings),
    {no_reply, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

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

%Internal functions
now_milisec({MegaSecs,Secs,MicroSecs}) ->
        (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

add_list_to_pendings([], _Sender, _Pendings) -> done;
add_list_to_pendings([Next|Rest], Sender, Pendings) ->
    ets:insert(Pendings, {Next, Sender}),
    add_list_to_pendings(Rest, Sender, Pendings).
notify_all([],_Reply) -> done;
notify_all([Next|Rest],Reply) -> 
    riak_core_vnode:reply(Next, Reply),
    notify_all(Rest, Reply).
certification_check(_Tx,_Log) -> true.
