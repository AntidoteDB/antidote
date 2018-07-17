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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc This module performs a read function over a CRDT snapshot,
%%%      given its key and type.
%%%      The function can be applied to a snapshot from the
%%%      transaction's read-set or from the materializer.
%%%      The read can be performed synchronously or asynchronously.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(clocksi_object_function).

%% API
-export([async_execute_object_function/8,
         async_execute_object_function/9,
         sync_execute_object_function/7]).

async_execute_object_function(Sender, Transaction, IndexNode, Key, Type, ReadFun, WriteSet, ReadSet) ->
    async_execute_object_function(Sender, Transaction, IndexNode, 0, Key, Type, ReadFun, WriteSet, ReadSet).

async_execute_object_function(Sender, Transaction, IndexNode, ReqNum, Key, Type, ReadFun, WriteSet, ReadSet) ->
    case orddict:find(Key, ReadSet) of
        {ok, Snapshot} ->
            {fsm, Sender0} = Sender,
            case Type:is_operation(ReadFun) of
                true ->
                    {Op, _Args} = ReadFun,
                    Updates2 = clocksi_vnode:reverse_and_filter_updates_per_key(WriteSet, Key),
                    Snapshot2 = clocksi_materializer:materialize_eager(Type, Snapshot, Updates2),
                    Value = Type:value(ReadFun, Snapshot2),
                    gen_statem:cast(Sender0, {ok, {ReqNum, Key, Type, Op, Snapshot2, Value}});
                false ->
                    gen_statem:cast(Sender0, {error, {function_not_supported, ReadFun}})
            end;
        error ->
            ok = clocksi_vnode:async_read_data_item(IndexNode, Transaction, ReqNum, Key, Type, ReadFun, WriteSet)
    end,
    ok.

sync_execute_object_function(Transaction, IndexNode, Key, Type, ReadFun, WriteSet, InternalReadSet) ->
    {Op, _Args} = ReadFun,
    case orddict:find(Key, InternalReadSet) of
        {ok, Snapshot} ->
            case Type:is_operation(ReadFun) of
                true ->
                    Updates2 = clocksi_vnode:reverse_and_filter_updates_per_key(WriteSet, Key),
                    Snapshot2 = clocksi_materializer:materialize_eager(Type, Snapshot, Updates2),
                    Value = Type:value(ReadFun, Snapshot2),
                    {ok, {Key, Type, Op, Snapshot2, Value}};
                false ->
                    {error, {function_not_supported, ReadFun}}
            end;
        error ->
            case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
                {ok, Snapshot, Value}->
                    {ok, {Key, Type, Op, Snapshot, Value}};
                {error, Reason}->
                    {error, {exec_object_function_failed, Reason}}
            end
    end.
