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
-module(simple_eiger_materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


-define(MAX_VERSIONS_EIGER, 10).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_vnode/1,
         read/2,
         update/3]).

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

-record(state, {partition, snapshot_cache}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

read(Key, Time) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref,
                                        {read, Key, Time},
                                        eiger_materializer_vnode_master).

update(Key, Value, EVT) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, Value, EVT},
                                        eiger_materializer_vnode_master).

init([Partition]) ->
    {ok, #state{partition=Partition, snapshot_cache=dict:new()}}.

handle_command({read, Key, Time}, _Sender,
               State = #state{snapshot_cache=SnapshotCache}) ->
    case dict:find(Key, SnapshotCache) of
        {ok, ListValues} ->
            case Time of
                latest ->
                    {EVT, Value} = lists:last(ListValues);
                _Other ->
                    {EVT, Value} = get_version(lists:reverse(ListValues), Time)
            end;
        error ->
            {EVT, Value} = {0, null} 
    end,
    {reply, {ok, {EVT, Value}}, State};


handle_command({update, Key, Value, EVT}, _Sender,
               State = #state{snapshot_cache=SnapshotCache0})->
    
    case dict:find(Key, SnapshotCache0) of
        {ok, ListValues0} ->
            case ListValues0 of
                ?MAX_VERSIONS_EIGER ->
                    [_First|Rest] = ListValues0,
                    ListValues = orddict:store(EVT, Value, Rest);
                _ ->
                    ListValues = orddict:store(EVT, Value, ListValues0)
            end;
        error ->
            ListValues0 = orddict:new(),
            ListValues = orddict:store(EVT, Value, ListValues0)
    end,
    SnapshotCache = dict:store(Key, ListValues, SnapshotCache0),
    {reply, ok, State#state{snapshot_cache=SnapshotCache}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0} ,
                       _Sender,
                       State = #state{snapshot_cache = SnapshotCache}) ->
    F = fun({Key,Snapshots}, A) ->
                Fun(Key, Snapshots, A)
        end,
    Acc = dict:fold(F, Acc0, SnapshotCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State = #state{snapshot_cache = SnapshotCache0}) ->
    {Key, Snapshots} = binary_to_term(Data),
    SnapshotCache = dict:store(Key, Snapshots, SnapshotCache0),
    {reply, ok, State#state{snapshot_cache=SnapshotCache}}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{snapshot_cache = SnapshotCache}) ->
    case dict:fetch_keys(SnapshotCache) of
        [] ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{snapshot_cache=_SnapshotCache0}) ->
    SnapshotCache = dict:new(),
    {ok, State#state{snapshot_cache=SnapshotCache}}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

get_version([], _Time) ->
    {0, null};

get_version([{EVT, Value}|Rest], Time) ->
    case EVT =< Time of
        true ->
            {EVT, Value};
        false ->
            get_version(Rest, Time)
    end.
