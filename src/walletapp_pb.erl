-module(walletapp_pb).

-export([credit/3,
         debit/3,
         getbalance/2,
         buyvoucher/3,
         usevoucher/3,
         readvouchers/2]).

-type reason() :: atom().

%% walletapp uses floppystore apis - create, update, get

-spec credit(Key::term(), Amount::non_neg_integer(), Pid::pid()) -> ok | {error, reason()}.
credit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok, Counter} ->
            CounterUpdt = floppyc_counter:increment(Amount, Counter),
             case floppyc_pb_socket:store_crdt(CounterUpdt,Pid) of
                ok -> ok;
                {error,Reason} -> {error, Reason}
             end;
        {error, Reason} -> 
             {error, Reason}
    end.

-spec debit(Key::term(), Amount::non_neg_integer(), Pid::pid()) -> ok | {error, reason()}.
debit(Key, Amount, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok,Counter} ->
            CounterUpdt = floppyc_counter:decrement(Amount, Counter),
            case floppyc_pb_socket:store_crdt(CounterUpdt,Pid) of
                ok -> ok;
                {error,Reason} -> {error, Reason} 
            end;
        {error, Reason} -> 
            {error, Reason}
    end.

-spec getbalance(Key::term(), Pid::pid()) -> {error, error_in_read} | {ok, integer()}.
getbalance(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_pncounter,Pid) of
        {ok,Counter} ->
            {ok,floppyc_counter:value(Counter)};
        {error, _Reason} -> 
            {error, error_in_read}
    end.

-spec buyvoucher(Key::term(),Voucher::term(), Pid::pid()) -> ok | {error, reason()}.
buyvoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            SetUpdt = floppyc_set:add(Voucher,Set),
            case floppyc_pb_socket:store_crdt(SetUpdt,Pid) of
               ok -> ok;
               {error, Reason} -> {error, Reason}
           end;
        {error, Reason} -> 
            {error, Reason}
    end.
-spec usevoucher(Key::term(),Voucher::term(),Pid::pid()) -> ok | {error, reason()}.
usevoucher(Key, Voucher, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            SetUpdt = floppyc_set:remove(Voucher,Set),
           case floppyc_pb_socket:store_crdt(SetUpdt,Pid) of
               ok -> ok;
               {error, Reason} -> {error, Reason}
           end;
        {error, Reason} -> 
            {error, Reason}
    end.

-spec readvouchers(Key::term(), Pid::pid()) -> {ok, list()} | {error, reason()}.
readvouchers(Key, Pid) ->
    case floppyc_pb_socket:get_crdt(Key,riak_dt_orset,Pid) of
        {ok,Set} ->
            {ok,sets:to_list(floppyc_set:value(Set))};
        {error, Reason} -> 
            {error, Reason}
    end. 

%% TODO - Add transaction like operations - buy voucher and reduce balance

