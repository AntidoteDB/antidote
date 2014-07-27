-module(walletclient_pb).
-include_lib("eunit/include/eunit.hrl").

-export([run/2]).

run(Key1,Key2)->
    net_kernel:start(['walletclient@127.0.0.1', longnames]),
    erlang:set_cookie(node(),floppy),
    %Created = walletapp:createuser(Key1,Key2),
    {ok,Pid} = floppyc_pb_socket:start("localhost",8087),
    Result1 = testc(Key1, 10, [bal],Pid),
    Result2 = testvoucher(Key2,10,[voucher],Pid),
    [Result1 | Result2].
       
testc(_, 0, Result, _Pid) ->
    lists:reverse(Result);
testc(Key, N, Result, Pid) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Result1 = testcredit(Key, random:uniform(100), Pid),
    Result2 = testdebit(Key, random:uniform(100), Pid),
    %Result1 = testcredit(Key, 2),
    %Result2 = testdebit(Key,1),
    timer:sleep(10),
    testc(Key, N-1,[Result2 | [Result1 | Result]], Pid).

testcredit(Key, Amount, Pid) ->
    Balbefore = walletapp_pb:getbalance(Key, Pid),
    walletapp_pb:credit(Key, Amount, Pid),
    case walletapp_pb:getbalance(Key, Pid) of
        {error,Reason} ->
            {error,Reason};
        Val ->
            ?assert(Val =:= Balbefore + Amount)
            %{ok, credit, Balbefore, Amount, Val}
    end.

testdebit(Key, Amount, Pid) ->
    Balbefore = walletapp_pb:getbalance(Key, Pid),
    walletapp_pb:debit(Key, Amount, Pid),
    case walletapp_pb:getbalance(Key, Pid) of
        {error,Reason} ->
            {error,Reason};
        Val ->
            ?assert(Val =:= Balbefore - Amount)
            %{ok, debit, Balbefore, Amount, Val}
    end.

testvoucher(_,0,Result, _Pid) ->
    lists:reverse(Result);
testvoucher(Key, N, Result, Pid) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    Result1 = testadd(Key, random:uniform(100), Pid),
    timer:sleep(10),
    testvoucher(Key, N-1,[Result1 | Result], Pid).

testadd(Key, Voucher, Pid) ->
    Init = walletapp_pb:readvouchers(Key, Pid),
    walletapp_pb:buyvoucher(Key,Voucher, Pid),
    After = walletapp_pb:readvouchers(Key, Pid),
    ?assert(lists:usort([Voucher| Init])=:= lists:sort(After)).
%{ok, add, Init, Voucher, After}.

