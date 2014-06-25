-module(clockSI_updateitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2, update_item/2]).

-record(state, { 
	 	tx_id,
		coordinator}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Coordinator, Tx) ->
    gen_fsm:start_link(?MODULE, [Coordinator, Tx], []).

now_milisec({MegaSecs,Secs,MicroSecs}) ->
	(MegaSecs*1000000 + Secs)*1000000 + MicroSecs.
%%%===================================================================
%%% States
%%%===================================================================

init([Coordinator, TxId]) ->
    SD = #state{
                coordinator=Coordinator,
                tx_id=TxId},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behinf, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock scew.
check_clock(timeout, SD0=#state{tx_id=TxId}) ->
    T_TS = TxId#tx_id.snapshot_time,
    Time = now_milisec(erlang:now()),
    case T_TS > Time of
	true -> 
	    timer:sleep(T_TS - Time),
    	    {next_state, update_item, SD0, 0};
	false ->
    	    {next_state, update_item, SD0, 0}
    end.

%% @doc simply finishes the fsm.
update_item(timeout, SD0=#state{tx_id=_TxId, coordinator=Coordinator}) ->
    riak_core_vnode:reply(Coordinator, ok),
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
