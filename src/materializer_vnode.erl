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

-record(state, {partition, ops_cache, snapshot_cache}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time
-spec read(key(), type(), vectorclock:vectorclock()) -> {ok, term()} | {error, atom()}.
read(Key, Type, SnapshotTime) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref,
                                        {read, Key, Type, SnapshotTime},
                                        materializer_vnode_master).

%%@doc write downstream operation to persistant log and ops_cache it for future read
-spec update(key(), #clocksi_payload{}) -> ok | {error, atom()}.
update(Key, DownstreamOp) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, materializer),
    [{NewPref,_}] = Preflist,
    riak_core_vnode_master:sync_command(NewPref, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

init([Partition]) ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    {ok, #state{partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

handle_command({read, Key, Type, SnapshotTime}, _Sender,
		State = #state{ops_cache=OpsCache, snapshot_cache=SnapshotCache}) ->
                          
    %lager:info("materialiser_vnode: operations: ~p", [Operations]),
    % get the latest snapshot for the key
    case ets:lookup(SnapshotCache, Key) of
    	[] ->
    		NewSnapshot=Type:new(),
            case ets:lookup(OpsCache, Key) of
            [] ->
            	Snapshot=NewSnapshot;
            [{_, OpsDict}] ->
            	lager:info("materialiser_vnode: about to filter OpsDict= ~p",[OpsDict]),
            	{ok, Ops}= filter_ops(OpsDict),
            	lager:info("materialiser_vnode: Ops to apply are: ~p",[Ops]),
            	LastOp=lists:last(Ops),
            	TxId = LastOp#clocksi_payload.txid,
            	{ok, Snapshot} = clocksi_materializer:update_snapshot(Type, NewSnapshot, SnapshotTime, Ops, TxId),
            	CommitTime = (lists:last(Ops))#clocksi_payload.commit_time,
            	lager:info("materialiser_vnode: caching new snapshot= ~p with Commit Time= ~p", [Snapshot, CommitTime]),
            	SnapshotDict=orddict:new(),
            	ets:insert(SnapshotCache, {Key, orddict:store(CommitTime,Snapshot, SnapshotDict)})
            end;
        [{_, SnapshotDict}] ->
			{ok, {SnapshotCommitTime, LatestSnapshot}} = get_latest_snapshot(SnapshotDict, SnapshotTime),
		    lager:info("materialiser_vnode: Latest snapshot for key ~p is ~p, with commit time ~p.",[Key, LatestSnapshot, SnapshotCommitTime]),
            case ets:lookup(OpsCache, Key) of
            [] ->
            	Snapshot=LatestSnapshot;
            [{_, OpsDict}] ->
            	lager:info("materialiser_vnode: about to filter OpsDict= ~p",[OpsDict]),
            	{ok, Ops}= filter_ops(OpsDict),
            	lager:info("materialiser_vnode: Ops to apply are: ~p",[Ops]),
            	case Ops of
            	[]->
            	    Snapshot=LatestSnapshot;
            	[H|T]->
            		LastOp=lists:last([H|T]),
            		TxId = LastOp#clocksi_payload.txid,
					{ok, Snapshot} = clocksi_materializer:update_snapshot(Type, LatestSnapshot, SnapshotTime, [H|T], TxId),
					CommitTime = LastOp#clocksi_payload.commit_time,
					lager:info("materialiser_vnode: caching new snapshot= ~p, snapshot committed with time:~p.", [Snapshot,CommitTime]),
					ets:insert(SnapshotCache, {Key, orddict:store(CommitTime,Snapshot, SnapshotDict)})
				end
            end
    end,
	lager:info("materialiser_vnode: snapshot: ~p", [Snapshot]),
	{reply, {ok, Snapshot}, State};

handle_command({update, Key, DownstreamOp}, _Sender,
               State = #state{ops_cache = OpsCache})->
    %% TODO: Remove unnecessary information from op_payload in log_Record
    LogRecord = #log_record{tx_id=DownstreamOp#clocksi_payload.txid,
                            op_type=downstreamop,
                            op_payload=DownstreamOp},
    LogId = log_utilities:get_logid_from_key(Key),
    [Node] = log_utilities:get_preflist_from_key(Key),
    case logging_vnode:append(Node,LogId,LogRecord) of
        {ok, _} ->
        	case ets:lookup(OpsCache, Key) of
        	[]->
        		lager:info("storing the first operation for the key"),
        		OpsDict=orddict:new();
        	[{_, OpsDict}]->
        		lager:info("materialiser_vnode: OpsDict= ~p",[OpsDict])
        	end,
        	lager:info("operation being stored: ~p", [DownstreamOp]),
        	OpsDict1=orddict:append(DownstreamOp#clocksi_payload.commit_time, DownstreamOp, OpsDict),
        	lager:info("materialiser_vnode: OpsDict= ~p",[OpsDict1]),
            true = ets:insert(OpsCache, {Key, OpsDict1}),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0} ,
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

-spec get_latest_snapshot(SnapshotDict::orddict:orddict(), SnapshotTime::vectorclock:vectorclock())
	 -> {ok, term()}.
get_latest_snapshot(SnapshotDict, SnapshotTime) ->
	case SnapshotDict of
	[]->
		{ok,[]};
	[H|T]->
		lager:info("materialiser_vnode: These are the stored Snapshots: ~p",[H|T]),
		case orddict:filter(fun(Key, _Value) -> 
				belongs_to_snapshot(Key, SnapshotTime) end, [H|T]) of 
			[]->
		        {ok,[]};
		    [H1|T1]->
				{CommitTime, Snapshot} = lists:last([H1|T1]),
				{ok, {CommitTime, Snapshot}}
        end
	end.

-spec filter_ops(orddict:orddict()) -> {ok, list()} | {error, wrong_format}.
filter_ops(Ops) ->
	filter_ops(Ops, []).
-spec filter_ops(orddict:orddict(), list()) -> {ok, list()} | {error, wrong_format}.
filter_ops([], Acc) ->
	{ok, Acc};
filter_ops([H|T], Acc) ->
	case H of 
	{_Key, Ops} ->
		filter_ops(T,lists:append(Ops, Acc));
	_ ->
		{error, wrong_format}
	end.
    
    
%% @doc Check whether a Key's Snapshot is included in a snapshot
%%      Input: Dc = Datacenter Id
%%             CommitTime = local commit time of this Snapshot at DC
%%             SnapshotTime = Orddict of [{Dc, Ts}]
%%      Outptut: true or false
-spec belongs_to_snapshot({Dc::term(),CommitTime::non_neg_integer()},
                        SnapshotTime::vectorclock:vectorclock()) -> boolean().
belongs_to_snapshot({Dc, CommitTime}, SnapshotTime) ->
    case vectorclock:get_clock_of_dc(Dc, SnapshotTime) of
        {ok, Ts} ->
            lager:info("materialiser_vnode: CommitTime: ~p SnapshotTime: ~p Result: ~p",
                       [CommitTime, Ts, CommitTime =< Ts]),
            CommitTime =< Ts;
        error  ->
            false
    end.
