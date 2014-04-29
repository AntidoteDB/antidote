-module(clockSI_updateitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2, update_item/2]).

-record(state, {
		key,
	 	tx,
		client}).

-record(tx, {snapshot_time, commit_time, prepare_time, state, write_set, origin}).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(Client, Tx, Key) ->
    gen_fsm:start_link(?MODULE, [Client, Tx, Key], []).

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

init([Client, Tx, Key]) ->
    SD = #state{
		key=Key,
                client=Client, 
                tx=Tx},
    {ok, check_clock, SD, 0}.

check_clock(timeout, SD0=#state{tx=Tx}) ->
    T_TS = Tx#tx.snapshot_time,
    Time = now_milisec(erlang:now()),
    if T_TS > Time ->
	timer:sleep(T_TS - Time)
    end,
    {next_state, update_item, SD0, 0}.

update_item(timeout, SD0=#state{tx=_Tx, key=_Key, client=_Client}) ->
    
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
