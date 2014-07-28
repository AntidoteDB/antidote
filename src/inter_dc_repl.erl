-module(inter_dc_repl).
-behaviour(gen_fsm).
-include("floppy.hrl").

%%gen_fsm call backs
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

-export([ready/2, await_ack/2]).

%%public api
-export([propogate/1, propogate_sync/1, acknowledge/2]).

-record(state,
        { otherdc,
          otherpid,
          payload,
          retries,
          from
        }).

-define(TIMEOUT,60000).

%%public api
propogate_sync(Payload) ->
    Me = self(),
    ReqId = 1, %Generate reqID here
    _ = gen_fsm:start_link(?MODULE, [?OTHER_DC, inter_dc_recvr, Payload, {ReqId, Me}], []),
    receive
        {ReqId, normal} -> done;
        {ReqId, Reason} -> Reason
    end.

propogate(Payload) ->
    gen_fsm:start_link(?MODULE, [?OTHER_DC, inter_dc_recvr, Payload, ignore], []).

acknowledge(Pid, Payload) ->
    gen_fsm:send_event(Pid, {ok, Payload}),
    ok.

%% gen_fsm callbacks

init([OtherDC, OtherPid, Payload, From]) ->
    StateData = #state{otherdc = OtherDC, otherpid = OtherPid, payload = Payload, retries = 5, from = From},
    {ok, ready, StateData, 0}.

ready(timeout, StateData = #state{otherdc = Other, otherpid = PID, retries = RetryLeft, payload = Payload}) ->
    inter_dc_recvr:replicate({PID,Other}, Payload, {self(),node()}),
    {next_state, await_ack,
     StateData#state{retries = RetryLeft -1},
     ?TIMEOUT}.

await_ack({ok,Payload}, StateData) ->
    lager:info("Replicated ~p ~n",[Payload]),
    {stop, normal, StateData};
await_ack(timeout,StateData = #state{retries = RetryLeft}) ->
    case RetryLeft of
        0 -> {stop, request_timeout, StateData};
        _ -> {next_state, ready, StateData, 0}
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
