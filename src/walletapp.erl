%% @doc The walletapp is a test application for floppystore.
%%      It makes use of create, read/get, and update operations on the floppystore.

%% @TODO - Add transaction like operations - buy voucher and reduce balance -> Will be done when merging with clock_SI branch

-module(walletapp).

-export([credit/2, debit/2, init/2, getbalance/1, buyvoucher/2, usevoucher/2, readvouchers/1]).

-define(SERVER, 'floppy@127.0.0.1').
%% @doc Initializes the application by setting a cookie.
init(Nodename, Cookie) ->
    case net_kernel:start([Nodename, longnames]) of
	{ok, _ } -> 
	    erlang:set_cookie(node(),Cookie);
	 {error, reason} ->
	    {error, reason}
    end.

%% @doc Increases the available credit for a customer.
-spec credit(Key::term(), Amount::non_neg_integer()) -> ok | {error, string()}.  
credit(Key, Amount) ->
    case rpc:call(?SERVER, floppy, append, [Key,{{increment,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

%% @doc Decreases the available credit for a customer.
-spec debit(Key::term(), Amount::non_neg_integer()) -> ok | {error, string()}.
debit(Key, Amount) ->
    case rpc:call(?SERVER, floppy,  append, [Key,{{decrement,Amount}, actor1}]) of
	{ok, _} ->
	    ok;
	{error, Reason} ->
	    {error, Reason}
    end.

%% @doc Returns the current balance for a customer.
-spec getbalance(Key::term()) -> error | number().				  
getbalance(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_pncounter]) of	
	{error} ->
	    {error};
        Val ->
            Val
    end. 

%% @doc Increases the number of available vouchers for a customer.
-spec buyvoucher(Key::term(),Voucher::term()) -> ok | {error, string()}.		  
buyvoucher(Key, Voucher) ->
    rpc:call(?SERVER,floppy,  append, [Key, {{add, Voucher},actor1}]).

%% @doc Decreases the number of available vouchers for a customer.
-spec usevoucher(Key::term(),Voucher::term()) -> ok | {error, string()}.
usevoucher(Key, Voucher) ->
    rpc:call(?SERVER, floppy, append, [Key, {{remove, Voucher},actor1}]).

%% @doc Returns the number of currently available vouchers for a customer.
-spec readvouchers(Key::term()) -> number() | {error, string()}.
readvouchers(Key) ->
    case rpc:call(?SERVER, floppy, read, [Key, riak_dt_orset]) of
	{error, Reason} ->
	    {error, Reason};
        Val ->
	    Val
    end.
    
    
