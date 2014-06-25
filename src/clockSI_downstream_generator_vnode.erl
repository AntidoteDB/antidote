-module(clockSI_downstream_generator_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(dstate, {partition, preflist, last_commit_time}).

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
    riak_core_vnode_master:command([NewPref], {trigger}, clockSI_downstream_generator_vnode_master).

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
    {ok, #dstate{partition = Partition, preflist = Preflists, last_commit_time = 0}}.

%% @doc Read client update operations, generate downstream operations and store it to persistent log.
handle_command({trigger}, _Sender, State=#dstate{partition = Partition,preflist = Preflist, last_commit_time = Last_commit_time}) ->
    %% read next operation from logging_vnode 
    Node = {Partition, node()}, 
    Stable_time = get_stable_time(Node),
    lager:info("Take updates before time : ~p",[Stable_time]),
    
    {ok,{_vnodeId, FullOps}} = riak_core_vnode_master:sync_command(Node, {read, clientops, Preflist}, ?LOGGINGMASTER), 
    lager:info("Operations ~p", FullOps),
    Ops = [ Update || {_Key, Update} <- FullOps ], %, Update#operation.op_number /= Last_opid],
    Sorted_ops = filter_operations(Ops, Stable_time, Last_commit_time),
    lager:info(" Read operations ~p ~n",[Sorted_ops]),
    %% generate downstream operations from the client operations
    %% write downstream operation to logging_vnode   
    Commit_time = process_updates(Sorted_ops, Last_commit_time),
    vectorclock:update_clock(Node, 1, Stable_time), %TODO: Check whether this is correct
    {reply, ok, State#dstate{last_commit_time = Commit_time}};

handle_command({get_active_txns}, _Sender, State=#dstate{partition = Partition,preflist = _Preflist})->
    Node = {Partition, node()},
    {ok, ActiveTxns} = riak_core_vnode_master:sync_command(Node, {get_active_txns}, ?CLOCKSIMASTER),
    {reply, {ok, ActiveTxns}, State};

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
process_updates([], LastCommitTime) ->
    LastCommitTime;
process_updates([Update | Rest], LastCommitTime) ->
    case clockSI_downstream:generate_downstream_op(Update) of
        {ok, #operation{op_number = _OpId, payload = NewOp}} ->
            Key = NewOp#clocksi_payload.key,                                         
            case floppy_rep_vnode:append(Key, NewOp) of
                {ok, Result} ->
                    lager:info("~n Downstream op generated and written to log ~p ~p ~n",[NewOp, Result]), 
                    {_Dcid, CommitTime} = NewOp#clocksi_payload.commit_time,
                    process_updates(Rest, CommitTime);
                {error, Reason} ->
                    lager:info("~p: Error writing to log : ~p ",[?MODULE, Reason]),
                    LastCommitTime       
            end;
        {error, Reason} ->
            lager:info("Couldnot generate Downstream ~p",[Reason]),
            LastCommitTime 
    end.

%% -spec get_operations_from(Operations::list(), From::atom()) -> list().
%% get_operations_from([], _From) -> [];
%% get_operations_from([Next|Rest], From) ->
%%     {_Key, Op} = Next,
%%     case Op#operation.op_number == From of
%%         true ->
%%             [Rest];
%%         false ->
%%             get_operations_from(Rest, From)
%%     end.

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

    %% remove operations which are not safer to process now, because there could be other operations with lesser timestamps
    FilteredOps = lists:filtermap( fun(Update) ->
                                           Payload = Update#operation.payload,
                                           {_Dcid, Commit_time} = Payload#clocksi_payload.commit_time,
                                           Commit_time < Before
                                   end,
                                   UnprocessedOps),

    lager:info("Filter operations: ~p",FilteredOps),
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
                          
    
