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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").


-define(SNAPSHOT_THRESHOLD, 10).
-define(SNAPSHOT_MIN, 2).
-define(OPS_THRESHOLD, 50).

%-ifdef(TEST).
%-include_lib("eunit/include/eunit.hrl").
%-endif.

-export([start_vnode/1,
         read/3,
         multi_read/3,
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

-record(state, {partition, ops_cache, snapshot_cache}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time
-spec read(key(), type(), tx_id()) -> {ok, term()} | {error, atom()}.
read(Key, Type, TxId) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref,
                                        {read, Key, Type, TxId},
                                        materializer_vnode_master).

%% @doc Reads multiple keys by calling multiple times the read function above.                                   
multi_read(Vnode, Reads, TxId) ->
	riak_core_vnode_master:sync_command(Vnode,
                                        {multi_read, Reads, TxId},
                                        materializer_vnode_master).





%%@doc write operation to cache for future read
-spec update(key(), #ec_payload{}) -> ok | {error, atom()}.
update(Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

handle_command({read, Key, Type, TxId}, _Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    Reply=internal_read(Key, Type, TxId, OpsCache, SnapshotCache),
    %riak_core_vnode:reply(Sender, Reply);
    {reply, Reply, State};
    
handle_command({multi_read, Reads, TxId}, _Sender,
               State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
    Reply= internal_multi_read(Reads, TxId, OpsCache, SnapshotCache),    
	%riak_core_vnode:reply(Sender, Reply);
	{reply, Reply, State};
	
handle_command({update, Key, DownstreamOp}, _Sender,
               State = #state{ops_cache = OpsCache})->
    true = op_insert(Key,DownstreamOp, OpsCache),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
                       _Sender,
                       State = #state{ops_cache = OpsCache}) ->
    F = fun({Key,Operation}, A) ->
                Fun(Key, Operation, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State = #state{ops_cache = OpsCache}) ->
    {Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, {Key, Operation}),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache = OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{ops_cache=OpsCache}) ->
    true = ets:delete(OpsCache),
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.



%%---------------- Internal Functions -------------------%%

internal_multi_read(Reads, TxId, OpsCache, SnapshotCache)->
	internal_multi_read([], Reads, TxId, OpsCache, SnapshotCache).

internal_multi_read(ReadResults, [], _TxId, _OpsCache, _SnapshotCache)->
	{ok, ReadResults};
internal_multi_read(ReadResults, [H|T], TxId, OpsCache, SnapshotCache)->
    case H of
        {Key, Type} ->
            case internal_read(Key, Type, TxId, OpsCache, SnapshotCache) of
            {ok, Value} ->
            	Value2=Type:value(Value), 
				internal_multi_read(lists:append(ReadResults, [Value2]), T, TxId, OpsCache, SnapshotCache);
			{error, Reason} ->
				{error, Reason}
			end;
        WrongFormat ->
            {error, {wrong_format, WrongFormat}}
    end.


%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
%% TODO: move this code to the materializer
-spec internal_read(term(), atom(),tx_id() | ignore, atom() ,atom() ) -> {ok, term()} | {error, no_snapshot}.
internal_read(Key, Type, TxId, OpsCache, SnapshotCache) ->
    case ets:lookup(SnapshotCache, Key) of
	[] ->
		Snapshot=ec_materializer:new(Type),
		ExistsSnapshot=false;
	[{_, {TxId, Snapshot}}] ->
		ExistsSnapshot=true
    end,
	case ets:lookup(OpsCache, Key) of
		[] ->
			case ExistsSnapshot of
			false ->  
				{ok, Snapshot};
			true ->
				{error, no_snapshot}
			end;
		[{_, OpsDict}] ->
			%{ok, Ops}= filter_ops(OpsDict),
			{ok, Ops}= dict:to_list(OpsDict),
			case Ops of
				[] ->
					{ok, Snapshot};
				[H|T] ->
					case ec_materializer:materialize_eager(Type, Snapshot, [H|T]) of
					{ok, Snapshot} ->
							snapshot_insert(Key, TxId, SnapshotCache),
							{ok, Snapshot};
					{error, Reason} ->
						{error, Reason}
					end
			end
	end.




%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
snapshot_insert(Key, {CommitTime,Snapshot}, SnapshotCache)->
            ets:insert(SnapshotCache, {Key, {CommitTime,Snapshot}}).


%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert(term(), ec_payload(),
                   atom() )-> true.
op_insert(Key,DownstreamOp, OpsCache)->
	ets:insert(OpsCache, {Key, DownstreamOp#ec_payload.tx_id, DownstreamOp}).