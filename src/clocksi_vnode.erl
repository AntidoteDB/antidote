-module(clocksi_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/3,
	 update_data_item/4,
	 prepare/2,
	 commit/2,
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

-record(state, {partition}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read_data_item(Node, Transaction, Key) ->
    riak_core_vnode_master:sync_command(Node, {read_data_item, Transaction, Key}, ?CLOCKSIMASTER).

update_data_item(Node, Transaction, Key, Op) ->
    riak_core_vnode_master:sync_command(Node, {update_data_item, Transaction, Key, Op}, ?CLOCKSIMASTER).

prepare(Node, Transaction) ->
    riak_core_vnode_master:sync_command(Node, {prepare, Transaction}, ?CLOCKSIMASTER).

commit(Node, Transaction) ->
    riak_core_vnode_master:sync_command(Node, {commit, Transaction}, ?CLOCKSIMASTER).

init([Partition]) ->
    {ok, #state { partition=Partition }}.

%handle_command({read_data_item, Transaction, Key}, _Sender, State) ->
%    {T_State, T_ST} = Transaction,
    
    

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
