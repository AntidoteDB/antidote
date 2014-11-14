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
-module(walletclient_pb).
-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([run/1, start/1]).

start(Args) ->
    [Key1, Key2] = Args,
    run([Key1,Key2]).

-spec run([key()]) -> ok | {error, reason()}.
run([Key_bal,Key_voucher])->
    io:format("Starting Wallet Client~n"),
    case antidotec_pb_socket:start_link("localhost",8087) of
        {ok, Pid} ->
            Result1 = testbalance(Key_bal, 10, [],Pid),
            io:format("~nTesting credit and debit operations: ~p ~n ", [Result1]),
            Result2 = testvoucher(Key_voucher,10,[],Pid),
            io:format("~nTesting voucher use and buy operations: ~p ~n", [Result2]),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec testbalance(key(), pos_integer(), [any()],pid()) -> [any()].
testbalance(_, 0, Result,_Pid) ->
    lists:reverse(Result);
testbalance(Key, N, Result,Pid) ->
    {A1, A2, A3} = now(),
    _ = random:seed(A1, A2, A3),
    Result1 = testcredit(Key, random:uniform(100),Pid),
    Result2 = testdebit(Key, random:uniform(100),Pid),
    timer:sleep(10),
    testbalance(Key, N-1, [Result2 | [Result1 | Result]],Pid).

-spec testcredit(key(), pos_integer(), pid()) -> ok | {error, reason()}.
testcredit(Key, Amount, Pid) ->
    {ok, Balbefore} = walletapp_pb:getbalance(Key, Pid),
    case walletapp_pb:credit(Key, Amount, Pid) of
        {error, Reason} ->
            {error, Reason};
        ok ->
            case walletapp_pb:getbalance(Key, Pid) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Val} ->
                    Val = Balbefore + Amount,
                    ok
            end
    end.

-spec testdebit(key(), pos_integer(), pid()) -> ok | {error, reason()}.
testdebit(Key, Amount, Pid) ->
    case walletapp_pb:getbalance(Key,Pid) of
        {ok, Balbefore} ->
            case walletapp_pb:debit(Key, Amount,Pid) of
                ok ->
                    case walletapp_pb:getbalance(Key,Pid) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, Val} ->
                            Val = Balbefore - Amount,
                            ok
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec testvoucher(key(), pos_integer(), [any()], pid()) -> [any()].
testvoucher(_,0,Result, _Pid) ->
    lists:reverse(Result);

testvoucher(Key, N, Result, Pid) ->
    {A1,A2,A3} = now(),
    _Seed = random:seed(A1, A2, A3),
    Result1 = testadd(Key, random:uniform(100), Pid),
    timer:sleep(10),
    testvoucher(Key, N-1,[Result1 | Result], Pid).

-spec testadd(key(), pos_integer(), pid()) -> ok | {error, reason()}.
testadd(Key, Voucher, Pid) ->
    case walletapp_pb:readvouchers(Key,Pid) of
        {error, Reason} ->
            {error, Reason};
        {ok, Init} ->
            case walletapp_pb:buyvoucher(Key,Voucher,Pid) of
                {error, Reason} ->
                    {error, Reason};
                ok ->
                    {ok, After} = walletapp_pb:readvouchers(Key,Pid),
                    UniqueSorted = lists:usort([Voucher|Init]),
                    Sorted = lists:sort(After),
                    UniqueSorted = Sorted,
                    ok
            end
    end.

