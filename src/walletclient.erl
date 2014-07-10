%% @doc This walletclient is a tiny test application for the wallet API.

%% @TODO Adapt to protocol buffer API once it is ready! 

-module(walletclient).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([run/1, start/1]).

-type key() :: term().
-type reason() :: atom().

start(Args) ->
    [Key1, Key2] = Args,
    run([Key1,Key2]).

-spec run([key()]) -> ok.
run([Key_bal,Key_voucher])->
    io:format("Starting Wallet Client~n"),
    case walletapp:init('wallet1@127.0.0.1', floppy) of
        true ->
            Result1 = testbalance(Key_bal, 10, []),
            io:format("~nTesting credit and debit operations: ~p ~n ", [Result1]),
            Result2 = testvoucher(Key_voucher,10,[]),
            io:format("~nTesting voucher use and buy operations: ~p ~n", [Result2]),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec testbalance(key(), pos_integer(), [any()]) -> [any()].
testbalance(_, 0, Result) ->
    lists:reverse(Result);
testbalance(Key, N, Result) ->
    {A1, A2, A3} = now(),
    _ = random:seed(A1, A2, A3),
    Result1 = testcredit(Key, random:uniform(100)),
    Result2 = testdebit(Key, random:uniform(100)),
    timer:sleep(10),
    testbalance(Key, N-1, [Result2 | [Result1 | Result]]).

-spec testcredit(key(), pos_integer()) -> ok | {error, reason()}.
testcredit(Key, Amount) ->
    {ok, Balbefore} = walletapp:getbalance(Key),
    case walletapp:credit(Key, Amount) of
        {error, Reason} ->
            {error, Reason};
        ok ->
            case walletapp:getbalance(Key) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Val} ->
                    Val = Balbefore + Amount,
                    ok
            end
    end.

-spec testdebit(key(), pos_integer()) -> ok | {error, reason()}.
testdebit(Key, Amount) ->
    case walletapp:getbalance(Key) of
        {ok, Balbefore} ->
            case walletapp:debit(Key, Amount) of
                ok ->
                    case walletapp:getbalance(Key) of
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

-spec testvoucher(key(), pos_integer(), [any()]) -> [any()].
testvoucher(_,0,Result) ->
    lists:reverse(Result);
testvoucher(Key, N, Result) ->
    {A1, A2, A3} = now(),
    _ = random:seed(A1, A2, A3),
    Result1 = testadd(Key, random:uniform(100)),
    timer:sleep(10),
    testvoucher(Key, N-1, [Result1 | Result]).

-spec testadd(key(), pos_integer()) -> ok | {error, reason()}.
testadd(Key, Voucher) ->
    case walletapp:readvouchers(Key) of
        {error, Reason} ->
            {error, Reason};
        {ok, Init} ->
            case walletapp:buyvoucher(Key,Voucher) of
                {error, Reason} ->
                    {error, Reason};
                ok ->
                    {ok, After} = walletapp:readvouchers(Key),
                    UniqueSorted = lists:usort([Voucher|Init]),
                    Sorted = lists:sort(After),
                    UniqueSorted = Sorted,
                    ok
            end
    end.
