%% floppy_rep_vnode: coordinates an operation to be performed 
%% to the replication group of that object

-module(floppy_rep_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

%% API
-export([start_vnode/1,
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


-export([
	append/2,
	read/1,
	operate/5
        ]).

%%------------------------
%% Data type: state
%%   partition: an integer (default is undefined).
%%   lclock: an integer (default is undefined).
%%------------------------
-record(state, {partition,lclock}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition=Partition,lclock=0}}.

%% @doc Function: append/2 
%% Purpose: Start a fsm to coordinate the `append' operation to be performed in the object's replicaiton group
%% Args: Key of the object and operation parameters
%% Returns: {ok, Result} if success; {error, timeout} if operation failed.
append(LogId, Payload) ->
    {ok,_Pid} = floppy_coord_sup:start_fsm([self(), append, LogId, Payload]),
    receive
        {ok, Result} ->
	        lager:info("Append completed!~w~n",[Result]),
	        {ok, Result}
        after 5000 ->
	        lager:info("Append failed!~n"),
	        {error, timeout}
    end.

%% @doc Function: read/2 
%% Purpose: Start a fsm to `read' from the replication group of the object specified by Key
%% Args: Key of the object and its type, which should be supported by the riak_dt.
%% Returns: {ok, Ops} if succeeded, Ops is the union of operations; {error, nothing} if operation failed.
read(LogId) ->
    lager:info("Read ~w", [LogId]),
    {ok,_Pid}=floppy_coord_sup:start_fsm([self(), read, LogId, noop]),
    receive
        {ok, Ops} ->
	        lager:info("Read completed!~n"),
	        {ok, Ops}
        after 5000 ->
	        lager:info("Read failed!~n"),
	        {error, nothing}
    end.

%% @doc Function: operate/5
%% Purpose: Handles `read' or `append' operations. Tne vnode must be in the replication group
%% of the corresponding key. 
operate(Preflist, ToReply, Type, LogId, Payload) ->
    	riak_core_vnode_master:command(Preflist,
                                   {operate, ToReply, Type, LogId, Payload},
                                   ?REPMASTER).

%% @doc Handles `read' or `append' operations. 
%% If the operation is `append', generate a unique id for this operation, in form of {integer, nodeid}.
%% If the operation is `read', there is no such need.
%% Then start a rep fsm to perform quorum read/append.  
handle_command({operate, ToReply, Type, LogId, Payload}, _Sender, #state{partition=Partition,lclock=LC}) ->
    case Type  of
        append ->
      	    OpId = generate_op_id(LC);
	    read  ->
	        OpId = current_op_id(LC);
	    _ ->
	        lager:info("RepVNode: Wrong operations!~w~n", [Type]),
	        OpId = current_op_id(LC)
    end,
    {NewClock,_} = OpId,
    lager:info("RepVNode: Start replication, clock: ~w~n",[NewClock]),
    floppy_rep_sup:start_fsm([ToReply, Type, LogId, Payload, OpId]),
    {noreply, #state{lclock=NewClock, partition= Partition}};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
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

%% private
generate_op_id(Current) ->
    {Current + 1, node()}.

current_op_id(Current) ->
    {Current, node()}.    


