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
	 	tx_id,
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

init([Vnode, Client, TxId, Key, Type]) ->
    SD = #state{
		vnode=Vnode,
		type=Type,
		key=Key,
                client=Client, 
                tx_id=TxId,
		pending_txs=[]},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%	if local clock is behinf, it sleeps the fms until the clock
%%	catches up. CLOCK-SI: clock scew.
check_clock(timeout, SD0=#state{tx_id=TxId}) ->
    T_TS = TxId#tx_id.snapshot_time,
    Time = now_milisec(erlang:now()),
    if T_TS > Time ->
	timer:sleep(T_TS - Time)
    end,
    {next_state, get_txs_to_check, SD0, 0}.
		
%% @doc get_txs_to_check: 
%%	- Asks the Vnode for pending txs conflicting the cyrrent one
%%	- If none, goes to return state
%%	- Otherwise, goes to commit_notification
get_txs_to_check(timeout, SD0=#state{tx_id=TxId, vnode=Vnode, key=Key}) ->
    case clockSI_vnode:get_pending_txs(Vnode, {Key, TxId}) of
    {ok, empty} ->
		{next_state, return, SD0};
    {ok, Pending} ->
    	%{Committing, Pendings} = pending_commits(T_snapshot_time, Txprimes),
        {next_state, commit_notification, SD0=#state{pending_txs=Pending}, 0};
    {error, _Reason} ->
		{stop, normal, SD0}
    end.

%% @doc commit_notification:
%%	- The fsm stays in this state until the list of pending txs is empty.
%%	- The commit notifications are sent by the vnode.
commit_notification({committed, TxId}, SD0=#state{pending_txs=Left}) ->
    Left2=lists:delete(TxId, Left),
    if Left2==[] ->
    	{next_state, return, SD0=#state{pending_txs=Left2}, 0};
    true->
    	{next_state, commit_notification, SD0=#state{pending_txs=Left2}}
    end.

%% @doc	return:
%%	- Reads adn retunrs the log of the specified Key using the replication layer.
return(timeout, SD0=#state{client=Client, tx_id= TxId, key=Key, type=Type}) ->
    case floppy_rep_vnode:read_clockSI(Key, Type) of
    {ok, Ops} ->
	Reply = clockSI_materializer:materialize(Type, TxId#tx_id.snapshot_time, Ops);
    _ ->
	Reply=error
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
