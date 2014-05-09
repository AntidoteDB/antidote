%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(clockSI_readitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/5]).

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
	 	tx,
		client,
		vnode,
		pending_txs}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Client, Tx, Key, Type) ->
    gen_fsm:start_link(?MODULE, [Vnode, Client, Tx, Key, Type], []).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs.
%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Client, Tx, Key, Type]) ->
    SD = #state{
		vnode=Vnode,
		type=Type,
		key=Key,
                client=Client, 
                tx=Tx,
		pending_txs=[]},
    {ok, check_clock, SD, 0}.

check_clock(timeout, SD0=#state{tx=Tx}) ->
    T_TS = Tx#tx.snapshot_time,
    Time = now_milisec(erlang:now()),
    if T_TS > Time ->
	timer:sleep(T_TS - Time)
    end,
    {next_state, get_txs_to_check, SD0, 0}.

get_txs_to_check(timeout, SD0=#state{tx=Tx, vnode=Vnode, key=Key}) ->
    T_snapshot_time = Tx#tx.snapshot_time,
    %Concurrent txs
    case riak_core_vnode_master:sync_command(Vnode, {get_pending_txs, T_snapshot_time, Key}, ?CLOCKSIMASTER) of
    {ok, empty} ->
	{next_state, return, SD0};
    {ok, Pending} ->
    %{Committing, Pendings} = pending_commits(T_snapshot_time, Txprimes),
        {next_state, commit_notification, SD0=#state{pending_txs=Pending}, 0};
    {error, _Reason} ->
	{stop, noraml, SD0}
    end.

commit_notification({committed, TxId}, SD0=#state{pending_txs=Left}) ->
    Left2=clean_left(TxId, Left),
    if Left2==[] ->
    	{next_state, return, SD0=#state{pending_txs=Left2}, 0};
    true->
    	{next_state, commit_notification, SD0=#state{pending_txs=Left2}}
    end.

return(timeout, SD0=#state{client=Client, tx= Tx, key=Key, type=Type}) ->
    case floppy_rep_vnode:read_clockSI(Key, Type) of
    {ok, Ops} ->
	Reply = clockSI_materializer:materialize(Type, Tx#tx.snapshot_time, Ops);
    Else ->
	Reply=Else
    end,
    gen_fsm:reply(Client, Reply),
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

%%%===================================================================
%%% Internal Functions
%%%===================================================================
clean_left(TxId, Left) ->
    internal_clean_left(TxId, Left, []).

internal_clean_left(_TxId, [], NewLeft) -> NewLeft;

internal_clean_left(TxId, [Next|Rest], NewLeft) ->
    if TxId==Next ->
	NewLeft;
    true ->
	internal_clean_left(TxId, Rest, lists:append(NewLeft, [Next]))
    end.
