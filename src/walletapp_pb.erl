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
%% @todo Add transaction like operations - buy voucher and reduce
%%       balance

-module(walletapp_pb).

-export([credit/3,
         debit/3,
         getbalance/2,
         buyvoucher/3,
         usevoucher/3,
         readvouchers/2]).

-include("antidote.hrl").

-spec credit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
credit(Key, Amount, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok, Counter} ->
            CounterUpdt = antidotec_counter:increment(Amount, Counter),
            case antidotec_pb_socket:store_crdt(CounterUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
             {error, Reason}
    end.

-spec debit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
debit(Key, Amount, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok,Counter} ->
            CounterUpdt = antidotec_counter:decrement(Amount, Counter),
            case antidotec_pb_socket:store_crdt(CounterUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec getbalance(key(), pid()) -> {error, error_in_read} | {ok, integer()}.
getbalance(Key, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_pncounter, Pid) of
        {ok,Counter} ->
            {ok, antidotec_counter:value(Counter)};
        {error, _Reason} ->
            {error, error_in_read}
    end.

-spec buyvoucher(key(), term(), pid()) -> ok | {error, reason()}.
buyvoucher(Key, Voucher, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            SetUpdt = antidotec_set:add(Voucher,Set),
            case antidotec_pb_socket:store_crdt(SetUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                     {error, Reason}
           end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec usevoucher(key(), term(), pid()) -> ok | {error, reason()}.
usevoucher(Key, Voucher, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            SetUpdt = antidotec_set:remove(Voucher, Set),
            case antidotec_pb_socket:store_crdt(SetUpdt, Pid) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec readvouchers(key(), pid()) -> {ok, list()} | {error, reason()}.
readvouchers(Key, Pid) ->
    case antidotec_pb_socket:get_crdt(Key, riak_dt_orset, Pid) of
        {ok, Set} ->
            {ok, sets:to_list(antidotec_set:value(Set))};
        {error, Reason} ->
            {error, Reason}
    end.
