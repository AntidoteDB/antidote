%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(logging_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         asyn_read/2,
         read/2,
         asyn_append/3,
         append/3,
	 append_group/3,
         asyn_read_from/3,
         read_from/3]).

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
         handle_info/2,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-record(state, {partition, logs_map, clock, senders_awaiting_ack,
               last_read}).

%% API
-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a `threshold read' asynchronous command to the Logs in
%%      `Preflist' From is the operation id form which the caller wants to
%%      retrieve the operations.  The operations are retrieved in inserted
%%      order and the From operation is also included.
-spec asyn_read_from(preflist(), key(), op_id()) -> term().
asyn_read_from(Preflist, Log, From) ->
    riak_core_vnode_master:command(Preflist,
                                   {read_from, Log, From},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc synchronous read_from operation
-spec read_from({partition(), node()}, log_id(), op_id()) -> term().
read_from(Node, LogId, From) ->
    case riak_core_vnode_master:sync_command(Node,
                                        {read_from, LogId, From},
                                        ?LOGGING_MASTER) of
        {ok, []} ->
            {ok, []};
        {ok, [H|T]} ->
            {ok, [H|T]};
        {error, Reason} ->
            {error, Reason}
    end.                            

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist'
-spec asyn_read(preflist(), key()) -> term().
asyn_read(Preflist, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {read, Log},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc Sends a `read' synchronous command to the Logs in `Node'
-spec read({partition(), node()}, key()) -> term().
read(Node, Log) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Log},
                                        ?LOGGING_MASTER).

%% @doc Sends an `append' asyncrhonous command to the Logs in `Preflist'
asyn_append(Preflist, Log, Payload) ->
    riak_core_vnode_master:command(Preflist,
                                   {append, Log, Payload},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc synchronous append operation
append(IndexNode, LogId, Payload) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append, LogId, Payload},
                                        ?LOGGING_MASTER,
                                        infinity).

append_group(IndexNode, LogId, Payload) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append_group, LogId, Payload},
                                        ?LOGGING_MASTER,
                                        infinity).


%% @doc Opens the persistent copy of the Log.
%%      The name of the Log in disk is a combination of the the word
%%      `log' and the partition identifier.
init([Partition]) ->
    LogFile = integer_to_list(Partition),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    GrossPreflists = riak_core_ring:all_preflists(Ring, ?N),
    Preflists = lists:foldl(fun(X, Filtered) ->
                                    case preflist_member(Partition, X) of
                                        true ->
                                            lists:append(Filtered, [X]);
                                        false ->
                                            Filtered
                                    end
                            end, [], GrossPreflists),
    case open_logs(LogFile, Preflists, dict:new()) of
        {error, Reason} ->
            {error, Reason};
        Map ->
            {ok, #state{partition=Partition,
                        logs_map=Map,
                        clock=0,
                        senders_awaiting_ack=dict:new(),
                        last_read=start}}
    end.

%% @doc Read command: Returns the operations logged for Key
%%          Input: The id of the log to be read
%%      Output: {ok, {vnode_id, Operations}} | {error, Reason}
handle_command({read, LogId}, _Sender,
               #state{partition=Partition, logs_map=Map}=State) ->
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
           {Continuation, Ops} = 
                case disk_log:chunk(Log, start) of
                    {C, O} -> {C,O};
                    {C, O, _} -> {C,O};
                    eof -> {eof, []}
                end,
            case Continuation of
                error -> {reply, {error, Ops}, State};
                eof -> {reply, {ok, Ops}, State#state{last_read=start}};
                _ -> {reply, {ok, Ops}, State#state{last_read=Continuation}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Threshold read command: Returns the operations logged for Key
%%      from a specified op_id-based threshold.
%%
%%      Input:  From: the oldest op_id to return
%%              LogId: Identifies the log to be read
%%      Output: {vnode_id, Operations} | {error, Reason}
%%
handle_command({read_from, LogId, _From}, _Sender,
               #state{partition=Partition, logs_map=Map, last_read=Lastread}=State) ->
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            ok = disk_log:sync(Log),
            {Continuation, Ops} = 
                case disk_log:chunk(Log, Lastread) of
                    {error, Reason} -> {error, Reason};
                    {C, O} -> {C,O};
                    {C, O, _} -> {C,O};
                    eof -> {eof, []}
                end,
            case Continuation of
                error -> {reply, {error, Ops}, State};
                eof -> {reply, {ok, Ops}, State};
                _ -> {reply, {ok, Ops}, State#state{last_read=Continuation}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% @doc Append command: Appends a new op to the Log of Key
%%      Input:  LogId: Indetifies which log the operation has to be
%%              appended to.
%%              Payload of the operation
%%              OpId: Unique operation id
%%      Output: {ok, {vnode_id, op_id}} | {error, Reason}
%%
handle_command({append, LogId, Payload}, _Sender,
               #state{logs_map=Map,
                      clock=Clock,
                      partition=Partition,
                      senders_awaiting_ack=_SendersAwaitingAck0}=State) ->
    OpId = generate_op_id(Clock),
    {NewClock, _Node} = OpId,
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            case insert_operation(Log, LogId, OpId, Payload) of
                {ok, OpId} ->
                    {reply, {ok, OpId}, State#state{clock=NewClock}};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;


handle_command({append_group, LogId, PayloadList}, _Sender,
               #state{logs_map=Map,
                      clock=Clock,
                      partition=Partition,
                      senders_awaiting_ack=_SendersAwaitingAck0}=State) ->
    {ErrorList, SuccList, _NNC} = lists:foldl(fun(Payload, {AccErr, AccSucc,NewClock}) ->
						      OpId = generate_op_id(NewClock),
						      {NewNewClock, _Node} = OpId,
						      case get_log_from_map(Map, Partition, LogId) of
							  {ok, Log} ->
							      case insert_operation(Log, LogId, OpId, Payload) of
								  {ok, OpId} ->
								      {AccErr, AccSucc ++ [OpId], NewNewClock};
								  {error, Reason} ->
								      {AccErr ++ [{reply, {error, Reason}, State}], AccSucc,NewNewClock}
							      end;
							  {error, Reason} ->
							      {AccErr ++ [{reply, {error, Reason}, State}], AccSucc,NewNewClock}
						      end
					      end, {[],[],Clock}, PayloadList),
    case ErrorList of
	[] ->
	    [SuccId|_T] = SuccList,
	    {NewC, _Node} = lists:last(SuccList),
	    {reply, {ok, SuccId}, State#state{clock=NewC}};
	[Error|_T] ->
	    %%Error
	    {reply, Error, State}
    end;


handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       #state{logs_map=Map}=State) ->
    F = fun({Key, Operation}, Acc) -> FoldFun(Key, Operation, Acc) end,
    Acc = join_logs(dict:to_list(Map), F, Acc0),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{partition=Partition, logs_map=Map}=State) ->
    {LogId, #operation{op_number=OpId, payload=Payload}} = binary_to_term(Data),
    case get_log_from_map(Map, Partition, LogId) of
        {ok, Log} ->
            %% Optimistic handling; crash otherwise.
            {ok, _OpId} = insert_operation(Log, LogId, OpId, Payload),
            ok = disk_log:sync(Log),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{logs_map=Map}) ->
    LogIds = dict:fetch_keys(Map),
    case no_elements(LogIds, Map) of
        true ->
            {true, State};
        false ->
            {false, State}
    end.

delete(State) ->
    {ok, State}.

handle_info({sync, Log, LogId},
            #state{senders_awaiting_ack=SendersAwaitingAck0}=State) ->
    case dict:find(LogId, SendersAwaitingAck0) of
        {ok, Senders} ->
            _ = case dets:sync(Log) of
                ok ->
                    [riak_core_vnode:reply(Sender, {ok, OpId}) || {Sender, OpId} <- Senders];
                {error, Reason} ->
                    [riak_core_vnode:reply(Sender, {error, Reason}) || {Sender, _OpId} <- Senders]
            end,
            ok;
        _ ->
            ok
    end,
    SendersAwaitingAck = dict:erase(LogId, SendersAwaitingAck0),
    {ok, State#state{senders_awaiting_ack=SendersAwaitingAck}}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================%%
%% Internal Functions %%
%%====================%%

%% @doc no_elements: checks whether any of the logs contains any data
%%      Input:  LogIds: Each logId is a preflist that represents one log
%%              Map: the dictionary that relates the preflist with the
%%              actual log
%%      Return: true if all logs are empty. false if at least one log
%%              contains data.
%%
-spec no_elements([log_id()], dict()) -> boolean().
no_elements([], _Map) ->
    true;
no_elements([LogId|Rest], Map) ->
    case dict:find(LogId, Map) of
        {ok, Log} -> 
            case disk_log:chunk(Log, start) of
                eof ->
                    no_elements(Rest, Map);
                _ ->
                    false
            end;
        error ->
            {error, no_log_for_preflist}
    end.

%% @doc open_logs: open one log per partition in which the vnode is primary
%%      Input:  LogFile: Partition concat with the atom log
%%                      Preflists: A list with the preflist in which
%%                                 the vnode is involved
%%                      Initial: Initial log identifier. Non negative
%%                               integer. Consecutive ids for the logs.
%%                      Map: The ongoing map of preflist->log. dict()
%%                           type.
%%      Return:         LogsMap: Maps the  preflist and actual name of
%%                               the log in the system. dict() type.
%%
-spec open_logs(string(), [preflist()], dict()) -> dict() | {error, reason()}.
open_logs(_LogFile, [], Map) ->
    Map;
open_logs(LogFile, [Next|Rest], Map)->
    PartitionList = log_utilities:remove_node_from_preflist(Next),
    PreflistString = string:join(
                       lists:map(fun erlang:integer_to_list/1, PartitionList), "-"),
    LogId = LogFile ++ "--" ++ PreflistString,
    LogPath = filename:join(
                app_helper:get_env(riak_core, platform_data_dir), LogId),
    case disk_log:open([{name, LogPath}]) of
        {ok, Log} ->
            Map2 = dict:store(PartitionList, Log, Map),
            open_logs(LogFile, Rest, Map2);
        {repaired, Log, _, _} ->
            lager:info("Repaired log ~p", [Log]),
            Map2 = dict:store(PartitionList, Log, Map),
            open_logs(LogFile, Rest, Map2);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc get_log_from_map: abstracts the get function of a key-value store
%%              currently using dict
%%      Input:  Map:  dict that representes the map
%%              LogId:  identifies the log.
%%      Return: The actual name of the log
%%
-spec get_log_from_map(dict(), partition(), log_id()) ->
                              {ok, log()} | {error, no_log_for_preflist}.
get_log_from_map(Map, _Partition, LogId) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
           {ok, Log};
        error ->
            {error, no_log_for_preflist}
    end.

%% @doc join_logs: Recursive fold of all the logs stored in the vnode
%%      Input:  Logs: A list of pairs {Preflist, Log}
%%                      F: Function to apply when floding the log (dets)
%%                      Acc: Folded data
%%      Return: Folded data of all the logs.
%%
-spec join_logs([{preflist(), log()}], fun(), term()) -> term().
join_logs([], _F, Acc) ->
    Acc;
join_logs([{_Preflist, Log}|T], F, Acc) ->
    JointAcc = fold_log(Log, start, F, Acc),
    join_logs(T, F, JointAcc).

fold_log(Log, Continuation, F, Acc) ->
    case  disk_log:chunk(Log,Continuation) of 
        eof ->
            Acc;
        {Next,Ops} ->
            NewAcc = lists:foldl(F, Acc, Ops),
            fold_log(Log, Next, F, NewAcc)
    end.


%% @doc insert_operation: Inserts an operation into the log only if the
%%      OpId is not already in the log
%%      Input:
%%          Log: The identifier log the log where the operation will be
%%               inserted
%%          LogId: Log identifier to which the operation belongs.
%%          OpId: Id of the operation to insert
%%          Payload: The payload of the operation to insert
%%      Return: {ok, OpId} | {error, Reason}
%%
-spec insert_operation(log(), log_id(), op_id(), payload()) ->
                              {ok, op_id()} | {error, reason()}.
insert_operation(Log, LogId, OpId, Payload) ->
    Result =disk_log:log(Log, {LogId, #operation{op_number=OpId, payload=Payload}}),
    case Result of
        ok ->
            {ok, OpId};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc preflist_member: Returns true if the Partition identifier is
%%              part of the Preflist
%%      Input:  Partition: The partition identifier to check
%%              Preflist: A list of pairs {Partition, Node}
%%      Return: true | false
%%
-spec preflist_member(partition(), preflist()) -> boolean().
preflist_member(Partition,Preflist) ->
    lists:any(fun({P, _}) -> P =:= Partition end, Preflist).

generate_op_id(Current) ->
    {Current + 1, node()}.

-ifdef(TEST).

%% @doc Testing get_log_from_map works in both situations, when the key
%%      is in the map and when the key is not in the map
get_log_from_map_test() ->
    Dict = dict:new(),
    Dict2 = dict:store([antidote1, c], value1, Dict),
    Dict3 = dict:store([antidote2, c], value2, Dict2),
    Dict4 = dict:store([antidote3, c], value3, Dict3),
    Dict5 = dict:store([antidote4, c], value4, Dict4),
    ?assertEqual({ok, value3}, get_log_from_map(Dict5, undefined,
            [antidote3,c])),
    ?assertEqual({error, no_log_for_preflist}, get_log_from_map(Dict5,
            undefined, [antidote5, c])).

%% @doc Testing that preflist_member returns true when there is a
%%      match.
preflist_member_true_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(true, preflist_member(partition1, Preflist)).

%% @doc Testing that preflist_member returns false when there is no
%%      match.
preflist_member_false_test() ->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual(false, preflist_member(partition5, Preflist)).

-endif.
