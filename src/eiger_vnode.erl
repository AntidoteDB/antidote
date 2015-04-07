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
-module(eiger_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EIGER_MASTER, eiger_vnode_master).

-export([start_vnode/1,
         read_key/2,
         read_key_time/3,
         prepare/4,
         commit/4,
         coordinate_tx/3,
         get_clock/1,
         update_clock/2,
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

-ignore_xref([start_vnode/1]).

-record(state, {partition,
                min_pendings=dict:new() :: dict(),
                buffered_reads=dict:new() :: dict(),
                pending=dict:new() :: dict(),
                clock=0 :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).


read_key(Node, Key) ->
    riak_core_vnode_master:command(Node,
                                   {read_key, Key},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

read_key_time(Node, Key, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {read_key_time, Key, Clock},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

prepare(Node, UId, Clock, Keys) ->
    riak_core_vnode_master:command(Node,
                                   {prepare, UId, Clock, Keys},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

commit(Node, UId, Updates, Clock) ->
    riak_core_vnode_master:command(Node,
                                   {commit, UId, Updates, Clock},
                                   {fsm, undefined, self()},
                                   ?EIGER_MASTER).

coordinate_tx(Node, Updates, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                                        {coordinate_tx, Updates, Debug},
                                        ?EIGER_MASTER,
                                        infinity).

get_clock(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                        get_clock,
                                        ?EIGER_MASTER,
                                        infinity).

update_clock(Node, Clock) ->
    riak_core_vnode_master:sync_command(Node,
                                        {update_clock, Clock},
                                        ?EIGER_MASTER,
                                        infinity).
%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    {ok, #state{partition=Partition}}.

%% @doc starts a read_fsm to handle a read operation.
handle_command({read_key, Key}, _Sender,
               #state{clock=Clock, min_pendings=MinPendings}=State) ->
    case dict:find(Key, MinPendings) of
        {ok, _Min} ->
            {reply, {Key, empty, empty, Clock}, State};
        error ->
            do_read(Key, latest, State)
    end;

handle_command({read_key_time, Key, Time}, Sender,
               #state{clock=Clock0, buffered_reads=BufferedReads0, min_pendings=MinPendings}=State) ->
    Clock = max(Clock0, Time),
    case dict:find(Key, MinPendings) of
        {ok, Min} ->
            case Min =< Time of
                true ->
                    case dict:find(Key, BufferedReads0) of
                        {ok, Orddict0} ->
                            Orddict = orddict:store(Time, Sender, Orddict0);
                        error ->
                            Orddict0 = orddict:new(),
                            Orddict = orddict:store(Time, Sender, Orddict0)
                    end,
                    BufferedReads = dict:store(Key, Orddict, BufferedReads0),
                    {noreply, State#state{clock=Clock, buffered_reads=BufferedReads}};
                false ->
                    do_read(Key, Time, State#state{clock=Clock})
            end;
        error ->
            do_read(Key, Time, State#state{clock=Clock})
    end;

handle_command({coordinate_tx, Updates, Debug}, Sender, #state{partition=Partition}=State) ->
    Vnode = {Partition, node()},
    {ok, _Pid} = eiger_updatetx_coord_fsm:start_link(Vnode, Sender, Updates, Debug),
    {noreply, State};

handle_command({prepare, UId, CoordClock, Keys}, _Sender, #state{clock=Clock0, pending=Pending0, min_pendings=MinPendings0}=State) ->
    Clock = max(Clock0, CoordClock) + 1,
    {Pending, MinPendings} = lists:foldl(fun(Key, {P0, MP0}) ->
                                            P = dict:append(Key, {Clock, UId}, P0),
                                            MP = case dict:find(Key, MP0) of
                                                    {ok, _Min} ->
                                                        MP0;
                                                    _ ->
                                                        dict:store(Key, Clock, MP0)
                                                 end,
                                            {P, MP}    
                                        end, {Pending0, MinPendings0}, Keys),
    lager:info("Preparing tx at ~p for keys: ~p", [Clock, Keys]),
    {reply, {prepared, Clock}, State#state{clock=Clock, pending=Pending, min_pendings=MinPendings}};

handle_command({commit, UId, Updates, CommitClock}, _Sender, State0=#state{clock=Clock0}) ->
    Clock = max(Clock0, CommitClock),
    case update_keys(Updates, UId, CommitClock, State0) of
        {ok, State} ->
            {reply, {committed, CommitClock}, State#state{clock=Clock}};
        {error, Reason} ->
            {reply, {error, Reason}, State0#state{clock=Clock}}
    end;

handle_command(get_clock, _Sender, S0=#state{clock=Clock}) ->
    {reply, {ok, Clock}, S0};

handle_command({update_clock, NewClock}, _Sender, S0=#state{clock=Clock0}) ->
    Clock =  max(Clock0, NewClock),
    {reply, ok, S0#state{clock=Clock}};

handle_command(_Message, _Sender, State) ->
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

update_keys([], _UId, _CommitTime, State) ->
    {ok, State};

update_keys([Update|Rest], UId, CommitTime, State0=#state{pending=Pending0,
                                                         min_pendings=MinPendings0,
                                                         clock=Clock,
                                                         buffered_reads=BufferedReads0}) ->
    {Key, Value} = Update,
    case eiger_materializer_vnode:update(Key, Value, CommitTime) of
        ok ->
            List0 = dict:fetch(Key, Pending0),
            {List, PrepareTime} = delete_pending_entry(List0, UId, []),
            case List of
                [] ->
                    Pending = dict:erase(Key, Pending0),
                    MinPendings = dict:erase(Key, MinPendings0),
                    case dict:find(Key, BufferedReads0) of
                        {ok, Orddict0} ->
                            lists:foreach(fun({Time, Client}) ->
                                            {reply, Reply, _S} = do_read(Key, Time, State0),
                                            riak_core_vnode:reply(Client, Reply)
                                          end, Orddict0),
                            BufferedReads=dict:erase(Key, BufferedReads0),
                            State=State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                        error ->
                            State=State0#state{pending=Pending, min_pendings=MinPendings}
                    end;
                _ ->
                    Pending = dict:store(Key, List, Pending0),
                    case dict:fetch(Key, MinPendings0) < PrepareTime of
                        true ->
                            State=State0#state{pending=Pending};
                        false ->
                            Times = [PT || {_UId, PT} <- List],
                            Min = lists:min(Times),
                            MinPendings =  dict:store(Key, Min, MinPendings0),
                            case dict:find(Key, BufferedReads0) of
                                {ok, Orddict0} ->
                                    case handle_pending_reads(Orddict0, CommitTime, Key, Clock) of
                                        [] ->
                                            BufferedReads = dict:erase(Key, BufferedReads0);
                                        Orddict ->
                                            BufferedReads = dict:store(Key, Orddict, BufferedReads0)
                                    end,
                                    State=State0#state{pending=Pending, min_pendings=MinPendings, buffered_reads=BufferedReads};
                                error ->
                                    State=State0#state{pending=Pending, min_pendings=MinPendings}
                            end
                    end
            end,
            update_keys(Rest, UId, CommitTime, State);
        {error, Reason} ->
            {error, Reason}
    end.
    
delete_pending_entry([], _UId, List) ->
    {List, not_found};

delete_pending_entry([Element|Rest], UId, List) ->
    case Element of
        {PrepareTime, UId} ->
            {List ++ Rest, PrepareTime};
        _ ->
            delete_pending_entry(Rest, UId, List ++ [Element])
    end.

handle_pending_reads([], _CommitTime, _Key, _Clock) ->
    [];

handle_pending_reads([Element|Rest], CommitTime, Key, Clock) ->
    {Time, Client} = Element,
    case Time < CommitTime of
        true ->
            {reply, Reply, _State} = do_read(Key, Time, #state{clock=Clock}),
            riak_core_vnode:reply(Client, Reply),
            handle_pending_reads(Rest, CommitTime, Key, Clock);
        false ->
            [Element|Rest]
    end.

do_read(Key, Time, State=#state{clock=Clock}) -> 
    case eiger_materializer_vnode:read(Key, Time) of
        {ok, {EVT, Value}} ->
            case Time of
                latest -> 
                    Reply = {Key, Value, EVT, Clock};
                _ -> 
                    Reply = {Key, Value}
            end,
            {reply, Reply, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.
