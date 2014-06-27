-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         read/3,
         update/2]).

-export([init/1,
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

-record(state, {partition, cache}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read(Key, Type, Snapshot_time) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref, {read, Key, Type, Snapshot_time}, materializer_vnode_master).

update(Key, DownstreamOp) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref, {update, Key, DownstreamOp}, materializer_vnode_master).

init([Partition]) -> 
    Cache = ets:new(cache, [bag]),
    {ok,#state{partition = Partition, cache = Cache}}.

handle_command({read, Key, Type, Snapshot_time}, _Sender, State = #state{cache= Cache}) ->     
    case ets:lookup(Cache, Key) of 
        Operations when is_list(Operations) ->
            %% Operations is intthe order which it is inserted
            lager:info(" Operations ~p", Operations),
            ListofOps = filter_ops(Operations),
            {ok, Snapshot} = clockSI_materializer:get_snapshot(Type, Snapshot_time, ListofOps),
            lager:info("Snapshot ~p", Snapshot),
            %% TODO: Store Snapshots in Cache
            {reply, {ok, Snapshot}, State};
        _ ->  
            %% TODO: If there is any error, cache might be corrupted. So reconstruct cache from the log
            {reply, {error, read_cache_failed}, State} 
    end;

handle_command({update, Key, DownstreamOp}, _Sender, State = #state{cache = Cache})->
    case floppy_rep_vnode:append(Key, DownstreamOp) of
        {ok, _} -> 
            case ets:insert(Cache, {Key, DownstreamOp}) of
                true ->  {reply, ok, State};
                _ -> {reply, {error, writing_to_cache_failed}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State} 
    end;    

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {noreply, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

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

filter_ops(Ops) ->
    %% TODO: Filter out only downstream update operations from log 
    [ Op || { _Key, Op } <- Ops ].
