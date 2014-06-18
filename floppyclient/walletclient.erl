-module(walletclient).
-include_lib("eunit/include/eunit.hrl").

-export([run/1, start/1]).

start(Args) ->
    [Key1, Key2] = Args,
    Result = run([Key1,Key2]),
    io:format("~p ~n",[Result]).

run([Key1,Key2])->
    walletapp:init('wallet1@127.0.0.1', floppy),
    Result1 = testbalance(Key1, 10, [bal]),
    Result2 = testvoucher(Key2,10,[voucher]),
    [Result1 | Result2].
       
testbalance(_, 0, Result) ->
    lists:reverse(Result);
testbalance(Key, N, Result) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Result1 = testcredit(Key, random:uniform(100)),
    Result2 = testdebit(Key, random:uniform(100)),    
    timer:sleep(10),
    testbalance(Key, N-1,[Result2 | [Result1 | Result]]).

testcredit(Key, Amount) ->
    Balbefore = walletapp:getbalance(Key),
    walletapp:credit(Key, Amount),
    case walletapp:getbalance(Key) of
	{error,Reason} ->
	    {error,Reason};
	 Val ->
	    ?assert(Val =:= Balbefore + Amount)
	    %{ok, credit, Balbefore, Amount, Val}
    end.

testdebit(Key, Amount) ->
    Balbefore = walletapp:getbalance(Key),
    walletapp:debit(Key, Amount),
    case walletapp:getbalance(Key) of
	{error,Reason} ->
	    {error,Reason};
	Val ->
	    ?assert(Val =:= Balbefore - Amount)	    
    end.
   
testvoucher(_,0,Result) ->
    lists:reverse(Result);
testvoucher(Key, N, Result) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Result1 = testadd(Key, random:uniform(100)),
    timer:sleep(10),
    testvoucher(Key, N-1,[Result1 | Result]).

testadd(Key, Voucher) ->
    Init = walletapp:readvouchers(Key),
    walletapp:buyvoucher(Key,Voucher),
    After = walletapp:readvouchers(Key),
    ?assert(lists:usort([Voucher| Init])=:= lists:sort(After)).
    %{ok, add, Init, Voucher, After}.
    
