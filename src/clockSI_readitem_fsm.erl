%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(clockSI_readitem_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2]).% check_commiting/2, check_prepared/2, return/3]).

-record(state, {
                from :: pid(),
                op :: atom(),
	 	tx,
		vnode,
                key,
		client,
                param = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist2()}).

-record(tx, {ts, cts}).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(Vnode, Client, Tx) ->
    gen_fsm:start_link(?MODULE, [Vnode, Client, Tx], []).

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

init([Vnode, Client,  Tx]) ->
    SD = #state{
                vnode=Vnode,
                client=Client, 
                tx=Tx},
    {ok, check_clock, SD, 0}.

check_clock(timeout, SD0=#state{tx=Tx}) ->
    T_TS = Tx#tx.ts,
    Time = now_milisec(erlang:now()),
    if T_TS > Time ->
	timer:sleep(T_TS - Time)
    end,
    {next_state, check_commiting, SD0, 0}.

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


