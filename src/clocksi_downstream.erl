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

-include("floppy.hrl").

-export([generate_downstream_op/1]).

%% @doc Returns downstream operation for upstream operation
%%      input: Update - upstream operation
%%      output: Downstream operation or {error, Reason}
-spec generate_downstream_op(#clocksi_payload{}) ->
    {ok, #clocksi_payload{}} | {error, atom()}.
generate_downstream_op(Update) ->
    Key = Update#clocksi_payload.key,
    Type =  Update#clocksi_payload.type,
    {Op, Actor} =  Update#clocksi_payload.op_param,
    SnapshotTime = Update#clocksi_payload.snapshot_time,
    case materializer_vnode:read(Key, Type, SnapshotTime) of
        {ok, Snapshot} ->
            DownstreamOp = case Type of
                            crdt_orset ->
                                {ok, Op} = Type:update(Op, Actor, Snapshot),
                                Update#clocksi_payload{op_param=Op};
                            crdt_pncounter ->
                                {ok, Op} = Type:update(Op, Actor, Snapshot),
                                Update#clocksi_payload{op_param=Op};
                            _ ->
                                {ok, NewState} = Type:update(Op, Actor, Snapshot),
                                Update#clocksi_payload{op_param={merge, NewState}}
            end,
            {ok, DownstreamOp};
        {error, Reason} ->
            lager:info("Error: ~p", [Reason]),
            {error, Reason}
    end.
