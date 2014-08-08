%% @doc The coordinator for stat write opeartions.  This example will
%%      show how to properly replicate your data in Riak Core by making 
%%      use of the _preflist_.
%%
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

-record(state, {from :: pid(),
                type :: atom(),
                log_id,
                key,
                error :: [term()],
                payload = undefined :: term() | undefined,
                preflist :: preflist()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Type, Key, Payload) ->
    gen_fsm:start_link(?MODULE, {From, Type, Key, Payload}, []).

finish_op(From, Result, Message) ->
    gen_fsm:send_event(From, {Result, Message}).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init({From, Type, Key, Payload}) ->
    SD = #state{from=From,
                type=Type,
                key=Key,
                error=[],
                payload=Payload},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    {next_state, execute, SD0#state{preflist=Preflist}, 0}.

%% @doc Execute the write request and then go into waiting state to
%%      verify it has meets consistency requirements.
execute(timeout, SD0=#state{type=Type,
                            key=Key,
                            payload=Payload,
                            from=From,
                            preflist=Preflist}) ->
    lager:info("Execute operation ~p ~p Preflist: ~p",
               [Type, Payload, Preflist]),
    case Preflist of
        [] ->
            lager:info("No nodes online; primary preference list empty."),
            From ! {error, no_alive_vnode},
            {stop, normal, SD0};
        [H|T] ->
            lager:info("Forward to first node in preflist: ~p",
                       [H]),
            floppy_rep_vnode:operate(H, self(), Type, Key, Payload),
            {next_state, waiting, SD0#state{preflist=T}, ?COORD_TIMEOUT}
    end.

%% @doc The contacted vnode failed to respond within timeout. So contact
%% the next one in the preflist
waiting(timeout, SD0=#state{type=Type,
                            key=Key,
                            payload=Payload,
                            from=From,
                            preflist=Preflist}) ->
    lager:info("Coord: COORD_TIMEOUT, retry..."),
    case Preflist of
        [] ->
            lager:info("Coord: Nothing in pref list"),
            From ! {error, no_alive_vnode},
            {stop, normal, SD0};
      [H|T] ->
            lager:info("Coord: Forward to node:~w",[H]),
            floppy_rep_vnode:operate(H, self(), Type, Key, Payload),
            SD1 = SD0#state{preflist=T},
            {next_state, waiting, SD1, ?COORD_TIMEOUT}
    end;

%% @doc Receive result and reply the result to the process that started the fsm (From).
waiting({error, Message}, SD=#state{error=Error0}) ->
    lager:info("Error received: ~p", [Message]),
    Error1 = Error0 ++ [Message],
    {next_state, waiting, SD#state{error=Error1}, ?COORD_TIMEOUT};
waiting({Result, Message}, SD=#state{from=From}) ->
    From ! {Result, Message},
    {stop, normal, SD}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
