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
-export([check_clock/2, get_txs_to_check/2, check_committing/2, commit_notification/2, commit_ts_notification/2, check_prepared/2, commit_notification2/2, return/2]).

%% Spawn
-export([bcast_get_commit_ts/2]).

-record(state, {type,
		key,
	 	tx,
		vnode,
		client,
                committing_txs,
		left_committing,
		prepared_txs,
		left_prepared,
		left_acks}).

-record(tx, {snapshot_time, commit_time, prepare_time, state, write_set, origin}).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(Vnode, Client, Tx, Key, Type) ->
    gen_fsm:start_link(?MODULE, [Vnode, Client, Tx, Key, Type], []).

%start_link(Key, Op) ->
%    io:format('The worker is about to start~n'),
%    gen_fsm:start_link(?MODULE, [Key, , Op, ], []).

%receiveData(From, Key,Result) ->
%   io:format("Sending message to~w~n",[From]),
%   gen_fsm:send_event(From, {Key, Result}).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs.
%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Client, Tx, Key, Type]) ->
    SD = #state{
		type=Type,
		key=Key,
                vnode=Vnode,
                client=Client, 
                tx=Tx,
		committing_txs=[],
		left_committing=[],
		prepared_txs=[],
		left_prepared=[]},
    {ok, check_clock, SD, 0}.

check_clock(timeout, SD0=#state{tx=Tx}) ->
    T_TS = Tx#tx.snapshot_time,
    Time = now_milisec(erlang:now()),
    if T_TS > Time ->
	timer:sleep(T_TS - Time)
    end,
    {next_state, get_txs_to_wait, SD0, 0}.

get_txs_to_check(timeout, SD0=#state{tx=Tx}) ->
    T_snapshot_time = Tx#tx.snapshot_time,
    %Concurrent txs
    Txprimes=[Tx, Tx, Tx],
    {Committing, Pendings} = pending_commits(T_snapshot_time, Txprimes),
    {next_state, check_committing, SD0=#state{committing_txs=Committing, prepared_txs=Pendings}, 0}.

check_committing(timeout, SD0=#state{committing_txs=Committing, vnode=Vnode}) ->
    riak_core_vnode_master:sync_command(Vnode, {notify_commit, Committing}, ?CLOCKSIMASTER),
    {next_state, commit_notification, SD0=#state{left_committing=Committing}}.
    
commit_notification({committed, TxId}, SD0=#state{left_committing=Left}) ->
    Left2=clean_left(TxId, Left),
    if Left2==[] ->
    	{next_state, check_prepared, SD0=#state{left_committing=Left2}, 0};
    true->
    	{next_state, commit_notification, SD0=#state{left_committing=Left2}}
    end.

check_prepared(timeout, SD0=#state{tx=Tx, prepared_txs=Pendings}) ->
    spawn(clockSI_readitem_fsm, bcast_get_commit_ts, [Pendings, Tx#tx.snapshot_time]),
    {next_state, commit_ts_notification, SD0=#state{left_acks=Pendings}, 0}.

%Either already committed or t'_ts_commit > t_snapshot_time
commit_ts_notification({forget, TxId}, SD0=#state{left_acks=Left, vnode=Vnode, left_committing=Wait}) ->
    Left2=clean_left(TxId, Left),
    if Left2==[] ->
    	riak_core_vnode_master:sync_command(Vnode, {notify_commit, Wait}, ?CLOCKSIMASTER),
    	{next_state, commit_notification2, SD0=#state{left_committing=Wait, left_acks=Left2}};
    true->
    	{next_state, commit_ts_notification, SD0=#state{left_acks=Left2}}
    end;

%Tx not commited and t'_ts_commit < T_snapshot_time
commit_ts_notification({wait, TxId}, SD0=#state{left_acks=Left, vnode=Vnode, left_committing=Wait}) ->
    Left2=clean_left(TxId, Left),
    Wait2=lists:append(Wait, [TxId]),
    if Left2==[] ->
    	riak_core_vnode_master:sync_command(Vnode, {notify_commit, Wait2}, ?CLOCKSIMASTER),
    	{next_state, commit_notification2, SD0=#state{left_committing=Wait2, left_acks=Left2}};
    true->
    	{next_state, commit_ts_notification, SD0=#state{left_acks=Left2, left_committing=Wait2}}
    end.

commit_notification2({committed, TxId}, SD0=#state{left_committing=Left}) ->
    Left2=clean_left(TxId, Left),
    if Left2==[] ->
    	{next_state, return, SD0=#state{left_committing=Left2}, 0};
    true->
    	{next_state, commit_notification, SD0=#state{left_committing=Left2}}
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
bcast_get_commit_ts([], _Snapshot_time) ->
    ok;
bcast_get_commit_ts([Next, Rest], Snapshot_time) ->
    riak_core_vnode_master:sync_command(Next#tx.origin, {get_commit_ts, Snapshot_time}, ?CLOCKSIMASTER),
    bcast_get_commit_ts(Rest, Snapshot_time).

pending_commits(Snapshot_time, TXs)->
    internal_pending_commits(Snapshot_time, TXs, [], []).

internal_pending_commits(_Snapshot_time, [], Committing, Pendings)->
    {Committing, Pendings};

internal_pending_commits(Snapshot_time, [Next|Rest], Committing, Pendings)->
    case Rest#tx.state of
    commiting ->
	if Rest#tx.commit_time=<Snapshot_time ->
	    internal_pending_commits(Snapshot_time, Rest, lists:append(Committing,[Next]), Pendings);
	true ->
	    internal_pending_commits(Snapshot_time, Rest, Committing, Pendings)
	end;
    prepared ->
	internal_pending_commits(Snapshot_time, Rest, Committing, lists:append(Pendings,[Next]));
    else ->
	internal_pending_commits(Snapshot_time, Rest, Committing, Pendings)
    end.

clean_left(TxId, Left) ->
    internal_clean_left(TxId, Left, []).

internal_clean_left(_TxId, [], NewLeft) -> NewLeft;

internal_clean_left(TxId, [Next|Rest], NewLeft) ->
    if TxId==Next ->
	NewLeft;
    true ->
	internal_clean_left(TxId, Rest, lists:append(NewLeft, [Next]))
    end.
