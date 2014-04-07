-module(floppy_rep_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

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
	handle/5
        ]).


-record(state, {partition,lclock}).
%-record(value, {queue, lease, crdt}).



handle(Preflist, ToReply, Op, Key, Param) ->
    io:format("Rep_vnode:Handle ~w ~n",[Preflist]),
    %{A1,A2,A3} = now(),
    %random:seed(A1, A2, A3),
    %Rand = random:uniform(2),
    %io:format("**********Rand ~w********** ~n",[Rand]),
    %if Rand == 2 ->
	%io:format("Proceed request~n"),
    	riak_core_vnode_master:command(Preflist,
                                   {operate, ToReply, Op, Key, Param},
                                   ?REPMASTER).
     %  true ->
%	 io:format("Do nothing~n"),
%	 {ok}
 %   end.

%%%TODO: How can fsm reply message to vnode?
%fsm_reply(From, ReqID, Key, Value) ->
%    io:format("Rep_vnode:reply ~w ~n",[From]),
%    gen_server:call(From, {fsm_reply, ReqID, Key, Value}).
    %riak_core_vnode_master:command(Preflist,
    %                               {fsm_reply, ReqID, Key, Value},
    %                               ?REPMASTER).

%fsm_try(From, Nothing) ->
%    io:format("Trying..."),
%    gen_server:call(From, {fsm_try, Nothing}).



start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state{partition=Partition,lclock=0}}.

handle_command({operate, ToReply, Op, Key, Param}, _Sender, #state{partition=Partition,lclock=LC}) ->
      io:format("Node starts replication~n."),
      OpId = generate_op_id(LC),
      {NewClock,_} = OpId,
      floppy_rep_sup:start_fsm([ToReply, Op, Key, Param, OpId]),
      {noreply, #state{lclock=NewClock, partition= Partition}};

%handle_command({put, Key, Value}, _Sender, State) ->
%    D0 = dict:erase(Key, State#state.kv),
%    D1 = dict:append(Key, Value, D0),
%    {reply, {'put return', dict:fetch(Key, D1)}, #state{partition=State#state.partition, kv= D1} };
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

%handle_call({fsm_reply, ReqID, Key, Value}, _Sender, State)->
%      io:format("Got reply from FSM~n ~w ~w ~w ~n", [ReqID, Key, Value]),
%      {noreply, State};

%handle_call({fsm_try, Nothing}, _Sender, State) ->
%	io:format("Fsm try! ~w", [Nothing]),
%	{noreply, State}.



generate_op_id(Current) ->
    {Current + 1, node()}.

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


