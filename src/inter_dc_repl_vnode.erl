-module(inter_dc_repl_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").

-export([start_vnode/1,
     %API begin
     propogate/1,
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
propogate({Key,Op, Dc}) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    [{Indexnode,_Type} | _Preflist] = riak_core_apl:get_primary_apl(DocIdx, ?N, interdcreplication),
    riak_core_vnode_master:command(Indexnode, {propogate, {Key,Op, Dc}}, inter_dc_repl_vnode_master).

%% riak_core_vnode call backs
init([_Partition]) ->
    {ok, []}.

handle_command({propogate, {Key, Op, Dc}}, _Sender, _State) ->
    inter_dc_repl:propogate_sync({Key, Op, Dc}),
    {noreply, []}.

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
