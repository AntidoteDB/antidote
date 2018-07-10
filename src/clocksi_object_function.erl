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
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(clocksi_object_function).

%% API
-export([async_execute_object_function/8, sync_execute_object_function/7]).

async_execute_object_function(Sender, Transaction, IndexNode, Key, Type, ReadFun, WriteSet, InternalReadSet) ->
%%    Snapshot = case orddict:find(Key, InternalReadSet) of
%%                   {ok, S} ->
%%                       S;
%%                   error ->
%%                       case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
%%                           {ok, S}->
%%                               S;
%%                           {error, Reason}->
%%                               {error, {exec_object_function_failed, Reason}}
%%                       end
%%               end,
%%    case Snapshot of
%%        {error, R} ->
%%            {error, R}; %% {error, Reason} is returned here.
%%        Snapshot ->
%%            case Type:is_operation(ReadFun) of
%%                true ->
%%                    {Snapshot, Type:value(ReadFun, Snapshot)};
%%                false ->
%%                    {error, {function_not_supported, ReadFun}}
%%            end
%%    end.
    case orddict:find(Key, InternalReadSet) of
        {ok, Snapshot} ->
            {fsm, Sender0} = Sender,
            case Type:is_operation(ReadFun) of
                true ->
                    {Op, _Args} = ReadFun,
                    Value = Type:value(ReadFun, Snapshot),
                    gen_statem:cast(Sender0, {ok, {Key, Type, Op, Snapshot, Value}});
                false ->
                    gen_statem:cast(Sender0, {error, {function_not_supported, ReadFun}})
            end;
        error ->
            %case clocksi_vnode:read_data_item(IndexNode, Transaction, Key, Type, WriteSet) of
            %    {ok, Snapshot, Value}->
            %        gen_statem:cast(IndexNode, {ok, {Key, Type, Snapshot, Value}});
            %        %{ok, Snapshot, Value};
            %    {error, Reason}->
            %        {error, {exec_object_function_failed, Reason}}
            %end
            ok = clocksi_vnode:async_read_data_item(IndexNode, Transaction, Key, Type, ReadFun, WriteSet)
    end,
    ok.

sync_execute_object_function(Transaction, IndexNode, Key, Type, ReadFun, WriteSet, InternalReadSet) ->
    {Op, _Args} = ReadFun,
    case orddict:find(Key, InternalReadSet) of
        {ok, Snapshot} ->
            case Type:is_operation(ReadFun) of
                true ->
                    Value = Type:value(ReadFun, Snapshot),
                    {ok, {Key, Type, Op, Snapshot, Value}};
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
