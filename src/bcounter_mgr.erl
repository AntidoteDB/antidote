-module(bcounter_mgr).
-behaviour(gen_server).

-export([start_link/0,
         process_op/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {req_queue, timeout, transfer_timer}).
-define(DEFAULT_RESERVATION_TIMEOUT, 500000). %Microsecons
-define(TRANSFER_PERIOD, 5000). %Milliseconds

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:info("Started Bounded counter manager at node ~p", [node()]),
    Timer=erlang:send_after(?TRANSFER_PERIOD, self(), transfer_periodic),
    {ok, #state{req_queue=orddict:new(), timeout=?DEFAULT_RESERVATION_TIMEOUT, transfer_timer=Timer}}.

process_op(Key, {{decrement, _}=Operation, _Actor}, BCounter) ->
   gen_server:call(?MODULE, {consume, Key, Operation, BCounter});

process_op(Key, {{increment, _}=Operation, _Actor}, BCounter) ->
   gen_server:call(?MODULE, {generate, Key, Operation, BCounter});

process_op(Key, {{transfer, _Amount}=Operation, _Actor}, BCounter) ->
   gen_server:call(?MODULE, {transfer, Key, Operation, BCounter}).


%% ===================================================================
%% Callbacks
%% ===================================================================

handle_call({_IncOrDec, Key, {_,Amount}=Operation, BCounter}, _From, #state{req_queue=RequestsQueue}=State) ->
    lager:info("Checking Available Permissions for ~p", [dc_meta_data_utilities:get_my_dc_id()]),
    MyId = dc_meta_data_utilities:get_my_dc_id(),
    case crdt_bcounter:generate_downstream(Operation, MyId, BCounter) of
        {error, no_permissions} = FailedResult ->
            Available = crdt_bcounter:local_permissions(MyId, BCounter),
            UpdtQueue=queue_request(Key, Amount - Available, RequestsQueue),
            lager:info("Updated Requests Queue ~p.", [UpdtQueue]),
            {reply, FailedResult, State#state{req_queue=UpdtQueue}};
        Result -> {reply, Result, State}
    end.

handle_cast(_Info, State) ->
    {stop,badmsg,State}.

handle_info(transfer_periodic, #state{req_queue=RequestsQueue,transfer_timer=OldTimer}=State) ->
    erlang:cancel_timer(OldTimer),
    lager:info("Periodic Resources Transfer ~p.",[RequestsQueue]),
    orddict:fold(
      fun(Key, Queue, Accum) ->
        case Queue of
            [] -> Accum;
            Queue ->
                Required = lists:foldl(fun({Amount,_Timeout}, Acc) -> Acc + Amount end, 0, Queue),
                {ok,Obj} = antidote:read(Key, crdt_bcounter),
                Available = crdt_bcounter:permissions(Obj),
                lager:info("Going to Request ~p for key ~p.", [Required - Available, Key])
        end
      end, orddict:new(), RequestsQueue),
    NewTimer=erlang:send_after(?TRANSFER_PERIOD, self(), transfer_periodic),
    {noreply,State#state{transfer_timer=NewTimer}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

queue_request(Key, Amount, RequestsQueue) ->
    lager:info("Current requests Queue ~p", [RequestsQueue]),
    QueueForKey = case orddict:find(Key, RequestsQueue) of
                      {ok, Value} -> Value;
                      error -> []
                  end,
    CurrTime = erlang:now(),
    orddict:store(Key, [{Amount, CurrTime} | QueueForKey], RequestsQueue).



