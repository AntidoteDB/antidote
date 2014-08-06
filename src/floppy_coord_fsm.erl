%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(floppy_coord_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").


%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, finish_op/3]).


-record(state, {
          from :: pid(),
          type :: atom(),
          log_id,
          error :: [term()],
          payload = undefined :: term() | undefined,
          preflist :: preflist()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Type, LogId, Payload) ->
    gen_fsm:start_link(?MODULE, {From, Type, LogId, Payload}, []).

%%start_link(Key, Op) ->
%%    lager:info('The worker is about to start~n'),
%%    gen_fsm:start_link(?MODULE, [Key, , Op, ], []).

finish_op(From, Result, Message) ->
    gen_fsm:send_event(From, {Result, Message}).
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init({From, Type, LogId, Payload}) ->
    SD = #state{
            from=From,
            type=Type, 
            log_id=LogId,
            error=[],
            payload=Payload
		    },
    {ok, prepare, SD, 0}.


%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{log_id=LogId}) ->
    Preflist = log_utilities:get_apl_from_logid(LogId, replication),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{type=Type,
                            log_id=LogId,
                            payload=Payload,
                            from=From,
                            preflist=Preflist}) ->
    lager:info("Coord: Execute operation ~w ~w ~w Preflist:~w ~n",
               [Type, LogId, Payload, Preflist]),
    case Preflist of
        [] ->
            lager:info("Coord: Nothing in pref list~n"),
            From ! {error, no_alive_vnode},
            {stop, normal, SD0};
        [H|T] ->
            %% Send the operation to the first vnode in the preflist;
            %% Will timeout if the vnode does not respond within INDC_TIMEOUT
            lager:info("Coord: Forward to node~w~n",[H]),
            floppy_rep_vnode:operate(H, self(), Type, LogId, Payload),
            SD1 = SD0#state{preflist=T},
            {next_state, waiting, SD1, ?COORD_TIMEOUT}
    end.

%% @doc The contacted vnode failed to respond within timeout. So contact
%% the next one in the preflist
waiting(timeout, SD0=#state{type=Type,
                            log_id=LogId,
                            payload=Payload,
                            from=From,
                            error=Error,
                            preflist=Preflist}) ->
    lager:info("Coord: COORD_TIMEOUT, retry...~n"),
    case Preflist of
        [] ->
            lager:info("Coord: Nothing in pref list~n"),
            case Error of
                [] ->
                    From ! {error, can_not_reach_vnode};
                _ ->
                    From ! {error, Error}
            end,
            {stop, normal, SD0};
      [H|T] ->
            lager:info("Coord: Forward to node:~w~n",[H]),
            floppy_rep_vnode:operate(H, self(), Type, LogId, Payload),
            SD1 = SD0#state{preflist=T},
            {next_state, waiting, SD1, ?COORD_TIMEOUT}
    end;

%% @doc Receive result and reply the result to the process that started the fsm (From).
waiting({error, Message}, SD=#state{error=Error0}) ->
    lager:info("Coord: Got error message ~w ~n",[Message]),
    Error1 = Error0++[Message],
    {next_state, waiting, SD#state{error=Error1}, ?COORD_TIMEOUT};
waiting({Result, Message}, SD=#state{from=From}) ->
    lager:info("Coord: Finish operation ~w ~w ~n",[Result, Message]),
    From! {Result, Message},   
    {stop, normal, SD}.

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
