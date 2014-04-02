-module(walletapp).

-export([credit/2, debit/2, init/2, getbalance/1, buyvoucher/2, usevoucher/2, readvouchers/1, createuser/2]).

%% walletapp uses floppystore apis - create, update, get

%specify separate key for bal and voucher, could be put under same object later
createuser(Keybal, Keyvoucher) ->
    Result1 = init(Keybal, riak_dt_pncounter),
    Result2 = init(Keyvoucher, riak_dt_orset), %which type to use for vouchers
    [Result1 | Result2].

init(Key, Type) ->
    rpc:call('floppy1@127.0.0.1', floppy, create, [Key, Type]).
    
credit(Key, Amount) ->
    case rpc:call('floppy1@127.0.0.1', floppy, update, [Key,{{increment,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

debit(Key, Amount) ->
    case rpc:call('floppy1@127.0.0.1', floppy,  update, [Key,{{decrement,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

getbalance(Key) ->
    case rpc:call('floppy1@127.0.0.1', floppy, get, [Key]) of
	{ok, Val} ->
	    Val;
	{error, Reason} ->
	    {error, Reason}
    end. 
							      
buyvoucher(Key, Voucher) ->
    rpc:call('floppy1@127.0.0.1',floppy,  update, [Key, {{add, Voucher},actor1}]).

usevoucher(Key, Voucher) ->
    rpc:call('floppy1@127.0.0.1', floppy, update, [Key, {{remove, Voucher},actor1}]).

readvouchers(Key) ->
    case rpc:call('floppy1@127.0.0.1', floppy, get, [Key]) of
	{ok, Val} ->
	    Val;
	{error, Reason} ->
	    {error, Reason}
    end.
    

%% TODO - Add transaction like operations - buy voucher and reduce balance
    
