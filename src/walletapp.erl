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
%% @doc The walletapp is a test application for antidote.
%%      It makes use of create, read/get, and update operations on the
%%      antidote.
%%
%% @todo Add transaction like operations - buy voucher and reduce
%%       balance -> Will be done when merging with clock_SI branch
%%

-module(walletapp).

-export([credit/2,
         debit/2,
         init/2,
         getbalance/1,
         buyvoucher/2,
         usevoucher/2,
         readvouchers/1]).

-type reason() :: atom().

-define(SERVER, 'antidote@127.0.0.1').

%% @doc Initializes the application by setting a cookie.
init(Nodename, Cookie) ->
    case net_kernel:start([Nodename, longnames]) of
        {ok, _} ->
            erlang:set_cookie(node(), Cookie);
        {error, Reason} ->
           {error, Reason}
    end.

%% @doc Increases the available credit for a customer.
-spec credit(Key::term(), Amount::non_neg_integer()) -> ok | {error, reason()}.
credit(Key, Amount) ->
    case rpc:call(?SERVER, antidote, append, [Key, {{increment, Amount}, actor1}]) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Decreases the available credit for a customer.
-spec debit(Key::term(), Amount::non_neg_integer()) -> ok | {error, reason()}.
debit(Key, Amount) ->
    case rpc:call(?SERVER, antidote,  append, [Key,{{decrement,Amount}, actor1}]) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Returns the current balance for a customer.
-spec getbalance(Key::term()) -> {error, error_in_read} | {ok, number()}.
getbalance(Key) ->
    case rpc:call(?SERVER, antidote, read, [Key, riak_dt_pncounter]) of
        error ->
            {error, error_in_read};
        Val ->
            {ok, Val}
    end.

%% @doc Increases the number of available vouchers for a customer.
-spec buyvoucher(Key::term(),Voucher::term()) -> ok | {error, reason()}.
buyvoucher(Key, Voucher) ->
    rpc:call(?SERVER,antidote,  append, [Key, {{add, Voucher}, actor1}]).

%% @doc Decreases the number of available vouchers for a customer.
-spec usevoucher(Key::term(),Voucher::term()) -> ok | {error, reason()}.
usevoucher(Key, Voucher) ->
    rpc:call(?SERVER, antidote, append, [Key, {{remove, Voucher}, actor1}]).

%% @doc Returns the number of currently available vouchers for a customer.
-spec readvouchers(Key::term()) -> {ok, list()} | {error, reason()}.
readvouchers(Key) ->
    case rpc:call(?SERVER, antidote, read, [Key, riak_dt_orset]) of
        {error, Reason} ->
            {error, Reason};
        Val ->
            {ok, Val}
    end.
