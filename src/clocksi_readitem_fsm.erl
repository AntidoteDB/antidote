%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(clocksi_readitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2,
         waiting/2,
         return/2]).

%% Spawn

-record(state, {type,
                key,
                transaction,
                tx_coordinator,
                vnode,
                updates,
                pending_txs}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, Tx, Key, Type, Updates) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 Tx, Key, Type, Updates], []).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
    (MegaSecs*1000000 + Secs)*1000000 + MicroSecs.
%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, Transaction, Key, Type, Updates]) ->
    SD = #state{
            vnode=Vnode,
            type=Type,
            key=Key,
            tx_coordinator=Coordinator,
            transaction=Transaction,
            updates=Updates,
            pending_txs=[]},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%	if local clock is behinf, it sleeps the fms until the clock
%%	catches up. CLOCK-SI: clock scew.
check_clock(timeout, SD0=#state{transaction=Transaction}) ->
    TxId = Transaction#transaction.txn_id,
    T_TS = TxId#tx_id.snapshot_time,
    Time = now_milisec(erlang:now()),
    case (T_TS) > Time of true ->
            lager:info("ClockSI ReadItemFSM: waiting for clock to catchUp ~n"),
            timer:sleep(T_TS - Time),
            lager:info("ClockSI ReadItemFSM: done waiting... ~n"),
            {next_state, waiting, SD0, 0};

        false ->
            lager:info(
              "ClockSI ReadItemFSM: no need to wait for clock to catchUp ~n"),
            {next_state, waiting, SD0, 0}
    end.

waiting(timeout, SDO=#state{key = Key, transaction = Transaction}) ->
    {ok, LocalClock} = vectorclock:get_clock_by_key(Key),
    Snapshottime = Transaction#transaction.vec_snapshot_time,
    case vectorclock:is_greater_than(LocalClock, Snapshottime) of
        false ->
            _Result = clocksi_downstream_generator_vnode:trigger
                        (Key, {dummytx, [], vectorclock:from_list([]), 0}),
            %% TODO change this, add a heartbeat to increase vectorclock if
            %%     there are no pending txns in downstream generator
            {next_state, waiting, SDO, 1};
        true ->
            {next_state, return, SDO, 0}
    end.

%% @doc return:
%%	- Reads adn returns the log of specified Key using replication layer.
return(timeout, SD0=#state{key=Key,
                           tx_coordinator = Coordinator,
                           transaction = Transaction,type=Type,
                           updates=Updates}) ->
    lager:info(
      "ClockSI ReadItemFSM: reading key from the materialiser ~w", [Key]),
    Vec_snapshot_time = Transaction#transaction.vec_snapshot_time,
    case materializer_vnode:read(Key, Type, Vec_snapshot_time) of
        {ok, Snapshot} ->
            Updates2=filter_updates_per_key(Updates, Key),
            lager:info
              ("Filtered updates before completeing the read: ~w ~n" ,
               [Updates2]),
            Snapshot2=clocksi_materializer:update_snapshot_eager
                        (Type, Snapshot, Updates2),
            Reply=Type:value(Snapshot2);
        {error, Reason} ->
            lager:error
              ("ClockSI ReadItemFSM: reading from replog returned error: ~w",
               [Reason]),
            Reply={error, Reason}
    end,
    lager:info("ClockSI ReadItemFSM: replying to the tx coordinator ~w",
               [Coordinator]),
    riak_core_vnode:reply(Coordinator, Reply),
    lager:info("ClockSI ReadItemFSM: finished fsm for key ~w", [Key]),
    {stop, normal, SD0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%Internal functions

filter_updates_per_key(Updates, Key) ->
    int_filter_updates_key(Updates, Key, []).

int_filter_updates_key([], _Key, Updates2) ->
    Updates2;

int_filter_updates_key([Next|Rest], Key, Updates2) ->
    {_, {KeyPrime, Op}} = Next,
    lager:info("Comparing keys ~w==~w",[KeyPrime, Key]),
    case KeyPrime==Key of
        true ->
            int_filter_updates_key(Rest, Key, lists:append(Updates2, [Op]));
        false ->
            int_filter_updates_key(Rest, Key, Updates2)
    end.
