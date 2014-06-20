%% @doc The walletapp is a test application for floppystore.
%%      It makes use of create, read/get, and update operations on the floppystore.

%% @TODO - Add transaction like operations - buy voucher and reduce balance

-module(walletapp).

-export([credit/2, debit/2, init/2, getbalance/1, buyvoucher/2, usevoucher/2, readvouchers/1]).

-define(SERVER, 'floppy@127.0.0.1').
%% walletapp uses floppystore apis - create, update, get

init(Nodename, Cookie) ->
    net_kernel:start([Nodename, longnames]),
    erlang:set_cookie(node(),Cookie).

-spec credit(Key::term(), Amount::non_neg_integer()) -> ok | {error, string()}.  
credit(Key, Amount) ->
    case rpc:call(?SERVER, floppy, append, [Key,{{increment,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec debit(Key::term(), Amount::non_neg_integer()) -> ok | {error, string()}.
debit(Key, Amount) ->
    case rpc:call(?SERVER, floppy,  append, [Key,{{decrement,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

-spec getbalance(Key::term()) -> error | number().				  
getbalance(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_pncounter]) of	
	{error} ->
	    {error};
        Val ->
            Val
    end. 

-spec buyvouchers(Key::term()) -> ok | {error, string()}.				  buyvoucher(Key, Voucher) ->
    rpc:call(?SERVER,floppy,  append, [Key, {{add, Voucher},actor1}]).

-spec usevouchers(Key::term()) -> ok | {error, string()}.
usevoucher(Key, Voucher) ->
    rpc:call(?SERVER, floppy, append, [Key, {{remove, Voucher},actor1}]).

-spec readvouchers(Key::term()) -> number() | {error, string()}.
readvouchers(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_orset]) of
	{error, Reason} ->
	    {error, Reason};
        Val ->
	    Val
    end.
    
    
