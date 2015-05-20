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
-module(perform_read_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 read_data_item/5,
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


-record(state, {partition :: partition_id(),
                ops_cache :: ets:tid(),
                snapshot_cache :: ets:tid()
	       }).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, TxId, Key, Type, WriteSet) ->
    try
	riak_core_vnode_master:sync_command(Node,
				       {read,Node,TxId,Key,Type,WriteSet},
				       perform_read_vnode_master)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.




%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    OpsCache = materializer_vnode:get_cache_name(Partition,ops_cache),
    SnapshotCache = materializer_vnode:get_cache_name(Partition,snapshot_cache),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.


%% @doc Return active transactions in prepare state with their preparetime
handle_command({read,Node,TxId,Key,Type,WriteSet}, Sender,
               #state{ops_cache=OpsCache,snapshot_cache=SnapshotCache} = State) ->
    clocksi_readitem_fsm:start_link(Node, Sender, TxId,
				    Key, Type, WriteSet,{OpsCache,SnapshotCache}),
    {noreply,State};

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
