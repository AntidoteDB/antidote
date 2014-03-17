-module(logging_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
	 %API begin
	 get/2,
	 update/3,
	 create/3,
	 prune/3,
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

-record(state, {partition, log, objects, lclock}).
-record(operation, {opNumber, payload}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

get(Preflist, Key) ->
    riak_core_vnode_master:sync_command(PrefList, {get, Key}, ?MASTER)).

update(Preflist, Key, Op) ->
    riak_core_vnode_master:sync_command(PrefList, {update, Key, Op}, ?MASTER)).

create(Preflist, Key, Type) ->
    riak_core_vnode_master:sync_command(PrefList, {create, Key, Type}, ?MASTER)).

prune(Preflist, Key, Until) ->
    riak_core_vnode_master:sync_command(PrefList, {create, Key, Until}, ?MASTER)).

init([Partition]) ->
    FileLog = filename:join(app_helper:get_env(riak_core, platform_data_dir),
                         "log"),
    {ok, Log} = dets:open_file(partition_log, [{file, FileLog}, {type, bag}]),
    FileStore = filename:join(app_helper:get_env(riak_core, platform_data_dir),
                         "store"),
    {ok, Objects} = dets:open_file(partition_objects, [{file, FileStore}, {type, set}]),
    {ok, #state { partition=Partition, log=Log, objects= Objects }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({create, Key, Type}, _Sender, State) ->
    case dets:lookup(Objects, Key) of
    [] ->
	NewSnapshot=materializer:create_snapshot(Type),
	dets:insert(Objects, {Key, {Type, NewSnapshot}}),
	{reply, {ok, null}},
    [Snapshot] ->
	{reply, {error, key_in_use}};
    {error, Reason}->
	{reply, {error, Reason}}
    end;

handle_command({get, Key}, _Sender, Statei#state{log=Log, objects=Objects) ->
    case dets:lookup(Objects, Key) of
    [] ->
	{reply, {error, key_never_created}},
    [{Type, Snapshot}] ->
	case dets:lookup(Log, Key) of
	[] ->
	    {reply, {ok, Snapshot}},
	[H|T] ->
	    NewSnapshot=materializer:update_snapshot(Type, Snapshot,[H|T]),
	    dets:insert(Objects, {Key, {Type, NewSnapshot}}),
	    dets:delete(Log, Key),
	    {reply, {ok, NewSnapshot}};
	{error, Reason}->
	    {reply, {error, Reason}}
	end;
    {error, Reason}->
	{reply, {error, Reason}}
    end;

handle_command({update, Key, Payload}, _Sender, State#state{log=Log}) ->
    %Should we return key_never_created?
    OpId= generate_op_id(),
    dets:insert(Log, {Key, #operation{opNumber=OpId, payload=Payload}}),
    {reply, {ok, OpId}, State};

handle_command({prune, Key, UntilOp}, _Sender, State#state(log=Log)) ->
    prune(Log, Key, UntilOp),
    {reply, {ok, State#state.partition}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

generate_op_id(Current)->
    {Current + 1, node()}.
	
prune(Log, Key, OpId)->
    dets:select_delete(Log, ets:fun2ms(fun({Key, #operation{opNumber=Op}}) when Op<=OpId -> true end)).

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
