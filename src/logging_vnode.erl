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
    riak_core_vnode_master:sync_command(Preflist, {get, Key}, ?LOGGINGMASTER).

update(Preflist, Key, Op) ->
    riak_core_vnode_master:sync_command(Preflist, {update, Key, Op}, ?LOGGINGMASTER).

create(Preflist, Key, Type) ->
    riak_core_vnode_master:sync_command(Preflist, {create, Key, Type}, ?LOGGINGMASTER).

prune(Preflist, Key, Until) ->
    riak_core_vnode_master:sync_command(Preflist, {prune, Key, Until}, ?LOGGINGMASTER).

init([Partition]) ->
    FileLog = filename:join(app_helper:get_env(riak_core, platform_data_dir),
                         "log"),
    {ok, Log} = dets:open_file(partition_log, [{file, FileLog}, {type, bag}]),
    FileStore = filename:join(app_helper:get_env(riak_core, platform_data_dir),
                         "store"),
    {ok, Objects} = dets:open_file(partition_objects, [{file, FileStore}, {type, set}]),
    {ok, #state { partition=Partition, log=Log, objects= Objects }}.

handle_command({create, Key, Type}, _Sender, #state{objects=Objects}=State) ->
    io:format("Key: ~w, Type: ~w~n",[Key, Type]),
    case dets:lookup(Objects, Key) of
    [] ->
	NewSnapshot=materializer:create_snapshot(Type),
	dets:insert(Objects, {Key, {Type, NewSnapshot}}),
	{reply, {ok, null}, State};
    [_] ->
	{reply, {error, key_in_use}, State};
    {error, Reason}->
	{reply, {error, Reason}, State}
    end;

handle_command({get, Key}, _Sender, #state{log=Log, objects=Objects}=State) ->
    case dets:lookup(Objects, Key) of
    [] ->
	{reply, {error, key_never_created}, State};
    [{_, {Type,Snapshot}}] ->
    	io:format("It reaches this point. Type: ~w, Snapshot: ~w~n",[Type, Snapshot]),
	case dets:lookup(Log, Key) of
	[] ->
	    Value=Type:value(Snapshot),
	    {reply, {ok, Value}, State};
	[H|T] ->
	    NewSnapshot=materializer:update_snapshot(Type, Snapshot,[H|T]),
	    dets:insert(Objects, {Key, {Type, NewSnapshot}}),
	    dets:delete(Log, Key),
	    Value=Type:value(NewSnapshot),
	    {reply, {ok, Value}, State};
	{error, Reason}->
	    {reply, {error, Reason}, State}
	end;
    {error, Reason}->
	{reply, {error, Reason}, State}
    end;

handle_command({update, Key, Payload}, _Sender, #state{log=Log, lclock=LC}=State) ->
    %Should we return key_never_created?
    OpId= generate_op_id(LC),
    dets:insert(Log, {Key, #operation{opNumber=OpId, payload=Payload}}),
    {reply, {ok, OpId}, State#state{lclock=OpId}};

%handle_command({prune, Key, UntilOp}, _Sender, #state{log=Log}=State) ->
handle_command({prune, _, _}, _Sender, State) ->
%    do_prune(Log, Key, UntilOp),
    {reply, {ok, State#state.partition}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

generate_op_id(Current)->
    {Current + 1, node()}.
	
%do_prune(Log, Key, OpId)->
%    Ms = ets:fun2ms(fun({Key, #operation{opNumber=Op}}) when Op=<OpId -> true end),
%    dets:select_delete(Log, Ms).

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
