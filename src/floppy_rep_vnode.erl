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


-record(state, {partition}).
%-record(value, {queue, lease, crdt}).



handle(Preflist, Op, ReqID, Key, Param) ->
    io:format("Rep_vnode:Handle ~w ~n",[Preflist]),
    riak_core_vnode_master:sync_command(Preflist,
                                   {operate, Op, ReqID, Key, Param},
                                   ?REPMASTER).

%read(Preflist, ReqID, Vclock, Key) ->
%    riak_core_vnode_master:command(Preflist,
%                                   {read, ReqID, Vclock, Key},
%                                   {fsm, undefined, self()},
%
%                                   ?REPMASTER).


start_vnode(I) ->
    io:format("rep vnode started~n"),
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    io:format("rep vnode inited~n"),
    {ok, #state{partition=Partition}}.

handle_command({operate, Op, ReqID, Key, Param}, _Sender, _State) ->
      io:format("Node starts replication~n."),
      floppy_rep_sup:start_fsm([ReqID, self(), Op, Key, Param]);

%handle_command({put, Key, Value}, _Sender, State) ->
%    D0 = dict:erase(Key, State#state.kv),
%    D1 = dict:append(Key, Value, D0),
%    {reply, {'put return', dict:fetch(Key, D1)}, #state{partition=State#state.partition, kv= D1} };
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


