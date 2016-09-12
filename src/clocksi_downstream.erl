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
-module(clocksi_downstream).

-include("antidote.hrl").

-export([generate_downstream_op/7]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(Transaction :: transaction(), Node :: term(), Key :: key(),
  Type :: type(), Update :: {op(), actor()}, list(), orddict()) ->
{error, _} | {ok, {merge, _} | {update, _}, _}.
generate_downstream_op(Transaction, Node, Key, Type, Update, WriteSet, InternalReadSet)->
	%%    {Op, Actor} = Update,
	Result=case orddict:find(Key, InternalReadSet) of
		{ok, {S, SCP}}->
			{S, SCP};
		error->
			case clocksi_vnode:read_data_item(Node, Transaction, Key, Type, WriteSet) of
				{ok, {S, SCP}}->
					{S, SCP};
				{error, _Reason}->
					{error, _Reason}
			end
	end,
	case Result of
		{Snapshot, SnapshotCommitParams}->
			%%            ALE PREV CODE
			%%            TypeString = lists:flatten(io_lib:format("~p", [Type])),
			%%            case string:str(TypeString, "riak_dt") of
			%%                0 -> %% dealing with an op_based crdt
			%%                    case Type:generate_downstream(Op, Actor, Snapshot) of
			%%                        {ok, OpParam} ->
			%%                            {ok, {update, OpParam}, SnapshotCommitParams};
			%%                        {error, Reason} ->
			%%                            {error, Reason}
			%%                    end;
			%%                1 -> %% dealing with a state_based crdt
			%%                    case Type:update(Op, Actor, Snapshot) of
			%%                        {ok, NewState} ->
			%%                            {ok, {merge, NewState}, SnapshotCommitParams};
			%%                        {error, Reason} ->
			%%                            {error, Reason}
			%%                    end
			%%            end;
			NewSnapshot=case Type of
				antidote_crdt_bcounter->
					%% bcounter data-type.
					bcounter_mgr:generate_downstream(Key, Update, Snapshot);
				_->
					Type:downstream(Update, Snapshot)
			end,
			{NewSnapshot, SnapshotCommitParams};
		{error, R}->
			{error, R} %% {error, Reason} is returned here.
	end.