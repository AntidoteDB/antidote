-module(inter_dc_repl).
-behaviour(gen_fsm).
-include("floppy.hrl").
-include("inter_dc_repl.hrl").

%%gen_fsm call backs
-export([init/1, handle_event/3, handle_sync_event/4, 
    handle_info/3, terminate/3, code_change/4]).

-export([ready/2, await_ack/2]).

%%public api
-export([propogate/1, propogate_sync/1, acknowledge/2]).

-record(state, 
        { otherdcs :: node(),
          otherpid :: pid(),
          payload :: payload(),
          retries :: integer(),
          from :: {non_neg_integer(),pid()} | ignore,
      ackrecvd
      }).

-type state() :: #state{}.
-define(TIMEOUT,1000).
 

%public api
propogate_sync(Payload) ->
    Me = self(),
    ReqId = 1, %Generate reqID here -> adapt then type in the state record!
    _ = gen_fsm:start_link(?MODULE, {?OTHER_DC, inter_dc_recvr, Payload, {ReqId, Me}}, []),
    receive
        {ReqId, normal} -> done;
    {ReqId, Reason} -> Reason
    end.

propogate(Payload) ->
    gen_fsm:start_link(?MODULE, {?OTHER_DC, inter_dc_recvr, Payload, ignore}, []).
   
-spec acknowledge(pid(),payload()) -> ok.
acknowledge(Pid, Payload) ->
    gen_fsm:send_event(Pid, {ok, Payload}),   
    ok.

%% gen_fsm callbacks
-spec init({node(),pid(),payload(),{non_neg_integer(),pid()} | ignore}) -> {ok,ready,state(),0}.
init({OtherDC, OtherPid, Payload, From}) ->
    StateData = #state{otherdcs = OtherDC, otherpid = OtherPid, payload = Payload, retries = 5, from = From, ackrecvd= 0},
    {ok, ready, StateData, 0}.

-spec ready(timeout,state()) -> {next_state, await_ack,state(),?TIMEOUT}.
ready(timeout, StateData = #state{otherdcs = OtherDcs, otherpid = PID, payload = Payload}) ->
    inter_dc_recvr:replicate(OtherDcs, PID, Payload, {self(),node()}),
    {next_state, await_ack, 
         StateData#state{ackrecvd = 0},
         ?TIMEOUT}.

await_ack({ok,Payload}, StateData=#state{ackrecvd = AckRecvd , otherdcs = OtherDCs}) ->
    io:format("Replicated ~p ~n",[Payload]),
    TotalAck = AckRecvd+1,
    NoDcs = length(OtherDCs),
    case TotalAck of
        NoDcs ->
            {stop, normal, StateData#state{ackrecvd = TotalAck}};
        _ -> 
            {next_state, await_ack, StateData#state{ackrecvd = TotalAck}, ?TIMEOUT}
    end;

await_ack(timeout,StateData=#state{ackrecvd = AckRecvd}) ->
    case AckRecvd of
        0 ->
            {stop, request_timeout, StateData};
        _ ->
            {stop, normal, StateData}  %TODO: Handle the case when not all DCs recieve updates
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
