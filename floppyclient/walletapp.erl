-module(walletapp).

-export([credit/2, debit/2, init/2, getbalance/1, buyvoucher/2, usevoucher/2, readvouchers/1]).

-define(SERVER, 'floppy@127.0.0.1').
%% walletapp uses floppystore apis - create, update, get

init(Nodename, Cookie) ->
    net_kernel:start([Nodename, longnames]),
    erlang:set_cookie(node(),Cookie).

-spec credit(Key::term(), Amount::non_neg_integer()) -> term().  
credit(Key, Amount) ->
    case rpc:call(?SERVER, floppy, append, [Key,{{increment,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

debit(Key, Amount) ->
    case rpc:call(?SERVER, floppy,  append, [Key,{{decrement,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

getbalance(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_pncounter]) of	
	{error} ->
	    {error};
        Val ->
            Val
    end. 
							      
buyvoucher(Key, Voucher) ->
    rpc:call(?SERVER,floppy,  append, [Key, {{add, Voucher},actor1}]).

usevoucher(Key, Voucher) ->
    rpc:call(?SERVER, floppy, append, [Key, {{remove, Voucher},actor1}]).

readvouchers(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_orset]) of
	{error, Reason} ->
	    {error, Reason};
        Val ->
	    Val
    end.
    

%% TODO - Add transaction like operations - buy voucher and reduce balance
    
