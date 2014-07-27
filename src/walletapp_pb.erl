-module(walletapp_pb).

-export([credit/3, debit/3, init/3, getbalance/2, buyvoucher/3, usevoucher/3, readvouchers/2, createuser/3]).

%% walletapp uses floppystore apis - create, update, get

%specify separate key for bal and voucher, could be put under same object later
createuser(Keybal, Keyvoucher, Pid) ->
    Result1 = init(Keybal, riak_dt_pncounter, Pid),
    Result2 = init(Keyvoucher, riak_dt_orset, Pid), %which type to use for vouchers
    [Result1 | Result2].

init(Key, Type, Pid) ->
    floppyc_pb_socket:get_crdt(Key,Type,Pid).
    
credit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok,Counter} ->
            CounterUpdt = floppyc_counter:increment(Amount, Counter),
            floppyc_pb_socket:store_crdt(CounterUpdt,Pid);
        {error, Reason} -> 
             {error, Reason}
    end.

debit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok,Counter} ->
            CounterUpdt = floppyc_counter:decrement(Amount, Counter),
            floppyc_pb_socket:store_crdt(CounterUpdt,Pid);
        {error, Reason} -> 
            {error, Reason}
    end.

getbalance(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok,Counter} ->
            floppyc_counter:value(Counter);
        {error, Reason} -> 
            {error, Reason}
    end.

buyvoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            SetUpdt = floppyc_set:add(Voucher,Set),
            floppyc_pb_socket:store_crdt(SetUpdt,Pid);
        {error, Reason} -> 
            {error, Reason}
    end.

usevoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            SetUpdt = floppyc_set:remove(Voucher,Set),
            floppyc_pb_socket:store_crdt(SetUpdt,Pid);
        {error, Reason} -> 
            {error, Reason}
    end.

readvouchers(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            sets:to_list(floppyc_set:value(Set));
        {error, Reason} -> 
            {error, Reason}
    end. 

%% TODO - Add transaction like operations - buy voucher and reduce balance

