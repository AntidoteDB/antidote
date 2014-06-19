-module(clockSI_downstream_generator_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(dstate, {partition, preflist, last_opid}).

-export([start_vnode/1,
         trigger/1]).

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

%% @doc Notify downstream_generator_vnode that an update has been logged. 
%%      Downstream_generator_vnode then reads the updates from log, generate downstream op
%%      and write to persistent log
%%      input: Key for which update has been logged
trigger(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, clockSI_downstream_generator),
    [{NewPref,_}] = Preflist,
    ok = riak_core_vnode_master:sync_command(NewPref, {trigger}, clockSI_downstream_generator_vnode_master).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    Preflists = lists:foldl(fun(X, Filtered) -> 
                                    case is_primary_preflist(Partition, X) of 
                                        true ->
                                            X;
                                        false ->
                                            Filtered
                                    end
                            end, [], GrossPreflists),
    lager:info("Downstream vnode init"),
    {ok, #dstate{partition = Partition, preflist = Preflists, last_opid = 0}}.

%% @doc Read client update operations, generate downstream operations and store it to persistent log.
handle_command({trigger}, _Sender, State=#dstate{partition = Partition,preflist = Preflist, last_opid = Last_opid}) ->
    %% read next operation from logging_vnode 
    Node = {Partition, node()},                                                
    case Last_opid of 
        0 ->
            {ok,{_vnodeId, FullOps}} = riak_core_vnode_master:sync_command(Node, {read, clientops, Preflist}, ?LOGGINGMASTER);
        _ ->
            {ok, {_vnodeId, AllOps}} = riak_core_vnode_master:sync_command(Node, {read, clientops, Preflist}, ?LOGGINGMASTER),
            FullOps = get_operations_from(AllOps,Last_opid)
    end,  
    lager:info("Operations ~p", FullOps),
    Ops = [ Update || {_Key, Update} <- FullOps, Update#operation.op_number /= Last_opid],
    lager:info("~p: Read operations ~p ~n",[?MODULE, Ops]),
    %% generate downstream operations from the client operations
    %% write downstream operation to logging_vnode   
    LastWrittenOpId = process_updates(Ops, Last_opid),
    {reply, ok, State#dstate{last_opid = LastWrittenOpId}};

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

%% @doc preflist_member: Returns true if the Partition identifier is primary partition of the preflist
%%      Input:  Partition: The partidion identifier to check
%%              Preflist: A list of pairs {Partition, Node}
%%      Return: true | false
-spec is_primary_preflist(Partition::non_neg_integer(), Preflist::[{Index::integer(), Node::term()}]) -> true | false.
is_primary_preflist(Partition,[First|_Rest]) ->
    {PartitionB, _} = First,
    PartitionB==Partition.

%% @doc process_updates: Process updates one by one and generate downstream operations for each.   
%%      Input: List of operations  :: [#operation{op_number, #clocksi_payload}]
%%      Return: Last processed opid :: term()
-spec process_updates(Updates::[Update::#operation{}], OpId::term()) -> term().
process_updates([], LastOpId) ->
    LastOpId;
process_updates([Update | Rest], LastprocessedOpId) ->
    case clockSI_downstream:generate_downstream_op(Update) of
        {ok, #operation{op_number = OpId, payload = NewOp}} ->
            Key = NewOp#clocksi_payload.key,                                         
            case floppy_rep_vnode:append(Key, NewOp) of
                {ok, Result} ->
                    lager:info("~n Downstream op generated and written to log ~p ~p ~n",[NewOp, Result]),            
                    process_updates(Rest, OpId);
                {error, Reason} ->
                    lager:info("~p: Error writing to log : ~p ",[?MODULE, Reason]),
                    LastprocessedOpId         
            end;
        {error, Reason} ->
            lager:info("Couldnot generate Downstream ~p",[Reason]),
            LastprocessedOpId 
    end.

-spec get_operations_from(Operations::list(), From::atom()) -> list().
get_operations_from([], _From) -> [];
get_operations_from([Next|Rest], From) ->
    {_Key, Op} = Next,
    case Op#operation.op_number == From of
        true ->
            [Next|Rest];
        false ->
            get_operations_from(Rest, From)
    end.
