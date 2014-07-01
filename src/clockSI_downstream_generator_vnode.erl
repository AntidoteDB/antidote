-module(clockSI_downstream_generator_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(dstate, {partition, preflist, last_commit_time, pending_operations}).

-export([start_vnode/1,
         trigger/2]).

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
trigger(Key, WriteSet) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, clockSI_downstream_generator),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:command([NewPref], {trigger, WriteSet, self()}, clockSI_downstream_generator_vnode_master),
    receive 
        {ok, trigger_received} ->
            ok
    after 100 ->
            {error, timeout}
    end.     


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
    {ok, #dstate{partition = Partition, preflist = Preflists, last_commit_time = 0, pending_operations = []}}.

%% @doc Read client update operations, generate downstream operations and store it to persistent log.
handle_command({trigger, WriteSet, From}, _Sender, 
               State=#dstate{partition = Partition,preflist = _Preflist, last_commit_time = Last_commit_time, pending_operations = Pending}) ->

    Pending_operations = add_to_pending_operations(Pending, WriteSet),
    lager:info("Pending Operations ~p",[Pending_operations]),
    From ! {ok, trigger_received},
    Node = {Partition, node()}, %% Send ack to caller and continue processing

    Stable_time = get_stable_time(Node),
    lager:info("Take updates before time : ~p",[Stable_time]),
    Sorted_ops = filter_operations(Pending_operations, Stable_time, Last_commit_time),
    lager:info("Generate downstream for ~p",[Sorted_ops]),
    {Remaining_operations, LastProcessedTime} = lists:foldl( fun(X, {Ops, LCTS}) ->
                                                                     case process_update(X) of
                                                                         {ok, Commit_time} ->
                                                                             %% Remove this operation from pending_operations
                                                                             New_pending = lists:delete(X, Ops),
                                                                             {New_pending, Commit_time};
                                                                         {error, _Reason} ->
                                                                             {Ops, LCTS} 
                                                                     end
                                                             end, {Pending_operations, Last_commit_time}, Sorted_ops),
    vectorclock:update_clock(Partition, 1, Stable_time),   
    {reply, ok, State#dstate{last_commit_time = LastProcessedTime, pending_operations = Remaining_operations}};

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

process_update(Update) ->
    case clockSI_downstream:generate_downstream_op(Update) of
        {ok, #operation{payload = NewOp}} ->
            Key = NewOp#clocksi_payload.key,            
            case materializer_vnode:update(Key, #operation{payload = NewOp}) of
                ok ->
                    {_Dcid, Commit_time} = NewOp#clocksi_payload.commit_time,
                    {ok, Commit_time};
                {error, Reason} ->
                    {error, Reason} 
            end;          
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc - Return smallest snapshot time of active transactions. No new updates with smaller timestamp will occur in future.
get_stable_time(Node) ->
    lager:info("In get_stable_time"),
    {ok, ActiveTxns} = riak_core_vnode_master:sync_command(Node, {get_active_txns}, ?CLOCKSIMASTER),
    lager:info("Active txns before filtering"),
    lists:foldl(fun({_TxId, SnapshotTime}, MinTime) ->
                        case MinTime > SnapshotTime of
                            true ->
                                SnapshotTime;
                            false ->
                                MinTime
                        end
                end,
                now_milisec(erlang:now()),
                ActiveTxns).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.

%% @doc Select updates committed before Time and sort them in timestamp order.
filter_operations(Ops, Before, After) ->
    %% Remove operations which are already processed
    UnprocessedOps = lists:filtermap( fun(Update) ->
                                              Payload = Update#operation.payload,
                                              {_Dcid, Commit_time} = Payload#clocksi_payload.commit_time,
                                              Commit_time > After
                                      end,
                                      Ops),
    lager:info("Unprocessed Ops ~p after ~p",[UnprocessedOps, After]),

    %% remove operations which are not safer to process now, because there could be other operations with lesser timestamps
    FilteredOps = lists:filtermap( fun(Update) ->
                                           Payload = Update#operation.payload,
                                           {_Dcid, Commit_time} = Payload#clocksi_payload.commit_time,
                                           Commit_time < Before
                                   end,
                                   UnprocessedOps),

    lager:info("Filter operations: ~p",[FilteredOps]),
    %% Sort operations in timestamp order
    SortedOps = lists:sort( fun( Update1, Update2) ->
                                    Payload1 = Update1#operation.payload,
                                    Payload2 = Update2#operation.payload,
                                    {_Dcid1, Time1} = Payload1#clocksi_payload.commit_time,
                                    {_Dcid1, Time2} = Payload2#clocksi_payload.commit_time,
                                    Time1 < Time2
                            end,
                            FilteredOps),
    SortedOps.

add_to_pending_operations(Pending, WriteSet) -> 
    lager:info("Writeset : ~p",[WriteSet]),
    case WriteSet of 
    {TxId, Updates, CommitTime} ->
            lists:foldl( fun(Update, Operations) -> 
                                 {_,{Key,{Op,Actor}}} = Update,
                                 Vec_snapshot_time = dict:from_list([{1, TxId#tx_id.snapshot_time}]),
                                 NewOp = #clocksi_payload{key = Key, type = riak_dt_pncounter, op_param = {Op, Actor}, actor = Actor, snapshot_time = Vec_snapshot_time, commit_time = {1,CommitTime}, txid = TxId},
                                 lists:append(Operations,[#operation{payload = NewOp}])
                         end,
                         Pending, Updates);
        _  -> Pending
    end.

