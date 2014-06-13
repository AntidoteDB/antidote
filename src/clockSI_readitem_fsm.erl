%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(clockSI_readitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2,
         get_txs_to_check/2,
         commit_notification/2, 
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
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator, Tx, Key, Type, Updates], []).

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
            {next_state, get_txs_to_check, SD0, 0};

        false ->
            lager:info("ClockSI ReadItemFSM: no need to wait for clock to catchUp ~n"),
            {next_state, get_txs_to_check, SD0, 0}
    end.		
%% @doc get_txs_to_check: 
%%	- Asks the Vnode for pending txs conflicting the cyrrent one
%%	- If none, goes to return state
%%	- Otherwise, goes to commit_notification
get_txs_to_check(timeout, SD0=#state{transaction= Transaction, vnode=Vnode, key=Key}) ->
    lager:info("ClockSI ReadItemFSM: Calling vnode ~w to get pending txs for Key ~w ~n", [Vnode, Key]),
    TxId = Transaction#transaction.txn_id,
    case clockSI_vnode:get_pending_txs(Vnode, {Key, TxId}) of
        {ok, empty} ->		
            lager:info("ClockSI ReadItemFSM: no txs to wait for ~w ~n", [Key]),
            {next_state, return, SD0, 0};
        {ok, Pending} ->
                                                %{Committing, Pendings} = pending_commits(T_snapshot_time, Txprimes),
            lager:info("ClockSI ReadItemFSM: txs to wait for ~w. Pendings ~w~n", [Key, Pending]),
            {next_state, commit_notification, SD0#state{pending_txs=Pending}};
        {error, _Reason} ->
            lager:error("ClockSI ReadItemFSM: error while retrieving pending txs for Key: ~w, Reason: ~w~n", [Key, _Reason]),
            {stop, normal, SD0}
    end.

%% @doc commit_notification:
%%	- The fsm stays in this state until the list of pending txs is empty.
%%	- The commit notifications are sent by the vnode.
commit_notification({committed, TxId}, SD0=#state{pending_txs=Left, key=Key}) ->
    Left2=lists:delete({Key, TxId}, Left),
    lager:info("ClockSI ReadItemFSM: tx ~w has committed, removed from the list of pending txs.", [TxId]),
    case Left2 of [] ->
            lager:info("ClockSI ReadItemFSM: no further txs to wait for"),
            {next_state, return, SD0#state{pending_txs=Left2}, 0};
        [H|T] ->
            lager:info("ClockSI ReadItemFSM: still waiting for txs ~w ~n", [Left2]),
            {next_state, commit_notification, SD0#state{pending_txs=[H|T]}};
        _-> 
            lager:info("ClockSI ReadItemFSM: tx ~w has committed ~n", [TxId])
    end.

%% @doc	return:
%%	- Reads adn retunrs the log of the specified Key using the replication layer.
return(timeout, SD0=#state{tx_coordinator=Coordinator, transaction=Transaction, key=Key, type=Type, updates=Updates}) ->
    lager:info("ClockSI ReadItemFSM: reading key ~w ~n", [Key]),
    case floppy_rep_vnode:read(Key, Type) of
        {ok, Ops} ->
            lager:info("ClockSI ReadItemFSM: got the operations for key ~w, calling the materializer... ~n", [Key]),	      
            ListofOps = [ Op || { _Key, Op } <- Ops ],  
            Snapshot=clockSI_materializer:get_snapshot(Type, Transaction#transaction.vec_snapshot_time, ListofOps),
            Updates2=filter_updates_per_key(Updates, Key),
            lager:info("Filtered updates before completeing the read: ~w ~n" , [Updates2]),
            Snapshot2=clockSI_materializer:update_snapshot_eager(Type, Snapshot, Updates2),
            Reply=Type:value(Snapshot2),
                                                %Reply = clockSI_materializer:materialize(Type, TxId#tx_id.snapshot_time, Ops),
            lager:info("ClockSI ReadItemFSM: finished materializing");
        _ ->
            lager:error("ClockSI ReadItemFSM: reading from the replication group has returned an unexpected response ~n"),
            Reply=error
    end,
    lager:info("ClockSI ReadItemFSM: replying to the tx coordinator ~w ~n", [Coordinator]),
    riak_core_vnode:reply(Coordinator, Reply),
    lager:info("ClockSI ReadItemFSM: finished fsm for key ~w ~n", [Key]),
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
    lager:info("Comparing keys ~w==~w~n",[KeyPrime, Key]),
    case KeyPrime==Key of
        true ->
            int_filter_updates_key(Rest, Key, lists:append(Updates2, [Op]));
        false -> 
            int_filter_updates_key(Rest, Key, Updates2)
    end.
