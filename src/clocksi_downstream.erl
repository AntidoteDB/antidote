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
    {ok, #clocksi_payload{}} | {error, no_snapshot}.
generate_downstream_op(Update) ->
    Key = Update#clocksi_payload.key,
    Type =  Update#clocksi_payload.type,
    {Op, Actor} =  Update#clocksi_payload.op_param,
    TxId=Update#clocksi_payload.txid,
    SnapshotTime = Update#clocksi_payload.snapshot_time,
    case materializer_vnode:read(Key, Type, SnapshotTime, TxId) of
        {ok, Snapshot} ->
            {ok, NewState} = Type:update(Op, Actor, Snapshot),
            DownstreamOp = Update#clocksi_payload{op_param={merge, NewState}},
            {ok, DownstreamOp};
        {error, no_snapshot} ->
            {error, no_snapshot}
    end.
