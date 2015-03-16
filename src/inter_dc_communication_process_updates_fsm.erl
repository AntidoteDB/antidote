%%% Handles socket connections, and bridges a remote server
%%% With a progressquest game.
-module(inter_dc_communication_process_updates_fsm).
-behaviour(gen_fsm).

-record(state, {transactions,parent_pid,prev_child_pid}). % the current socket

-export([start_link/3]).
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).
-export([receive_message/2]).

-define(TIMEOUT,10000).

start_link(Transactions,ParentPid,PrevChildPid) ->
    gen_fsm:start_link(?MODULE, [Transactions,ParentPid,PrevChildPid], []).

init([Transactions,ParentPid,PrevChildPid]) ->
    {ok, receive_message, #state{transactions=Transactions,parent_pid=ParentPid,
				prev_child_pid=PrevChildPid},0}.

receive_message(timeout, State=#state{transactions=Transactions,parent_pid=ParentPid}) ->
    inter_dc_recvr_vnode:store_updates(Transactions),
    ParentPid ! {self(), put_in_queue},
    {stop, normal, State}.

handle_info(Message, _StateName, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
