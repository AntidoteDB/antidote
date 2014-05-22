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
	read/2,
	append_clockSI/2,
	read_clockSI/2,
	handleOp/5
        ]).


-record(state, {partition,lclock}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition=Partition,lclock=0}}.

%% @doc Start a fsm to coordinate the `append' operation to be performed in the object's replicaiton group
append(Key, Op) ->
    io:format("Append ~w ~w ~n", [Key, Op]),
    floppy_coord_sup:start_fsm([self(), append, Key, Op]),
    receive
        {_, Result} ->
	    io:format("Append completed!~w~n",[Result]),
	    {ok, Result}
    after 5000 ->
	    io:format("Append failed!~n"),
	    {error, timeout}
    end.

%% @doc Start a fsm to `read' from the replication group of the object specified by Key
read(Key, Type) ->
    io:format("Read ~w ~w ~n", [Key, Type]),
    floppy_coord_sup:start_fsm([self(), read, Key, noop]),
    receive
        {_, Ops} ->
	    io:format("Read completed!~n"),
	    {ok,Ops}
    after 5000 ->
	    io:format("Read failed!~n"),
	    {error, nothing}
    end.

%% @doc Handles `read' or `append' operations. Tne vnode must be in the replication group
%% of the corresponding key. 
handleOp(Preflist, ToReply, Op, Key, Param) ->
    	riak_core_vnode_master:command(Preflist,
                                   {operate, ToReply, Op, Key, Param},
                                   ?REPMASTER).

%% @doc Handles `read' or `append' operations. 
%% If the operation is `append', generate a unique id for this operation, in form of {integer, nodeid}.
%% If the operation is `read', there is no such need.
%% Then start a rep fsm to perform quorum read/append.  
handle_command({operate, ToReply, Op, Key, Param}, _Sender, #state{partition=Partition,lclock=LC}) ->
      case Op of append ->
      	OpId = generate_op_id(LC);
	read  ->
	OpId = current_op_id(LC);
	_ ->
	io:format("RepVNode: Wrong operations!~w~n", [Op]),
	OpId = current_op_id(LC)
      end,
      {NewClock,_} = OpId,
      io:format("RepVNode: Start replication, clock: ~w~n",[NewClock]),
      floppy_rep_sup:start_fsm([ToReply, Op, Key, Param, OpId]),
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


