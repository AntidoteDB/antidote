-module(inter_dc_repl).
-behaviour(gen_fsm).

%% FSM to send a set of updates to another DC

-include("floppy.hrl").
-include("inter_dc_repl.hrl").

%%gen_fsm call backs
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([ready/2, await_ack/2]).

%%public api
-export([propagate/1, propagate_sync/1, propagate_sync/2, acknowledge/2]).

-record(state,
        { otherdc :: node(),
          otherpid :: pid(),
          payload :: payload(),
          retries :: integer(),
          from :: {non_neg_integer(),pid()} | ignore,
          ackrecvd
        }).

-type state() :: #state{}.
-define(TIMEOUT,1000).

%%public api
propagate_sync(Payload) ->
    Me = self(),
    ReqId = 1, %Generate reqID here -> adapt then type in the state record!
    _ = gen_fsm:start_link(?MODULE, {?OTHER_DC, inter_dc_recvr, Payload, {ReqId, Me}}, []),
    receive
        {ReqId, normal} -> done;
        {ReqId, Reason} -> Reason
    end.

propagate(Payload) ->
    gen_fsm:start_link(?MODULE,
                       {?OTHER_DC, inter_dc_recvr, Payload, ignore},
                       []).

propagate_sync(Payload, OtherDc) ->
    Me = self(),
    ReqId = 1, %Generate reqID here -> adapt then type in the state record!
    _ = gen_fsm:start_link(?MODULE, {OtherDc, inter_dc_recvr, Payload, {ReqId, Me}}, []),
    receive
        {ReqId, normal} -> done;
        {ReqId, Reason} -> Reason
    end.

-spec acknowledge(pid(),payload()) -> ok.
acknowledge(Pid,DcId) ->
    gen_fsm:send_event(Pid, {ok,DcId}),
    ok.

%% gen_fsm callbacks
-spec init({node(),pid(),payload(),{non_neg_integer(),pid()} | ignore}) ->
                  {ok,ready,state(),0}.
init({OtherDC, OtherPid, Payload, From}) ->
    StateData = #state{otherdc = OtherDC,
                       otherpid = OtherPid,
                       payload = Payload,
                       retries = 5, from = From,
                       ackrecvd= 0},
    lager:info("Replicting to DCs ~p",[OtherDC]),
    {ok, ready, StateData, 0}.

-spec ready(timeout,state()) -> {next_state, await_ack,state(),?TIMEOUT}.
ready(timeout, StateData = #state{otherdc = OtherDc,
                                  otherpid = PID, payload = Payload}) ->
    lager:info("Start replicating"),
    inter_dc_recvr:replicate(OtherDc, PID, Payload, {self(),node()}),
    {next_state, await_ack,
     StateData#state{ackrecvd = 0},
     ?TIMEOUT}.

await_ack({ok,_DcId}, StateData) ->    
    lager:info("Ack rceived"),    
    {stop, normal, StateData};       

await_ack(timeout,StateData=#state{retries = RetriesLeft}) ->
    case RetriesLeft of
        0 -> {stop, request_time_out, StateData};
        _ -> {next_state, ready, StateData#state{retries = RetriesLeft -1},0}
    end.

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
terminate(Reason, _StateName, _State=#state{from = From}) ->
    case From of
        {ReqId, Pid} ->
            Pid ! {ReqId, Reason},
            Reason;
        ignore ->
            Reason
    end.

%% @private
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
