%% @doc The floppystore coordinator FSM is responsible for selecting
%%      coordinator node out of the preference list for a given key.
%%      This coordinator node will be forwarded the request, which will
%%      perform the local write, and then the replicated write.
%%

-module(floppy_coord_fsm).

-behavior(gen_fsm).

-include("floppy.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([prepare/2,
         execute/2,
         waiting/2,
         finish_op/3]).

-record(state, {from :: pid(),
                operation :: atom(),
                key :: key(),
                type :: type(),
                errors :: [term()],
                payload = undefined :: term() | undefined,
                preflist :: preflist()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Operation, Key, Type, Payload) ->
    gen_fsm:start_link(?MODULE, {From, Operation, Key, Type, Payload}, []).

finish_op(From, Result, Message) ->
    gen_fsm:send_event(From, {Result, Message}).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init({From, Operation, Key, Type, Payload}) ->
    SD = #state{from=From,
                operation=Operation,
                key=Key,
                type=Type,
                errors=[],
                payload=Payload},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    {next_state, execute, SD0#state{preflist=Preflist}, 0}.

%% @doc Execute the write request and then go into waiting state to
%%      verify it has meets consistency requirements.
execute(timeout, SD0=#state{operation=Operation,
                            key=Key,
                            type=Type,
                            payload=Payload,
                            from=From,
                            preflist=Preflist}) ->
    lager:info("Execute operation ~p ~p ~p ~p Preflist: ~p",
               [Key, Type, Operation, Payload, Preflist]),
    case Preflist of
        [] ->
            lager:info("No nodes online; primary preference list empty."),
            From ! {error, no_alive_vnode},
            {stop, normal, SD0};
        [H|T] ->
            lager:info("Forward to first node in preflist: ~p", [H]),
            floppy_rep_vnode:operate(H, self(), Operation, Key, Type, Payload),
            {next_state, waiting, SD0#state{preflist=T}, ?COORD_TIMEOUT}
    end.

%% @doc The contacted vnode failed to respond within timeout. So contact
%% the next one in the preflist
waiting(timeout, SD0=#state{operation=Operation,
                            key=Key,
                            type=Type,
                            payload=Payload,
                            from=From,
                            preflist=Preflist}) ->
    case Preflist of
        [] ->
            lager:info("Empty preference list; sending message to: ~p!",
                       [From]),
            From ! {error, quorum_unreachable},
            {stop, normal, SD0};
        [H|T] ->
            lager:info("Forwarding to node: ~p", [H]),
            floppy_rep_vnode:operate(H, self(), Operation, Key, Type, Payload),
            {next_state, waiting, SD0#state{preflist=T}, ?COORD_TIMEOUT}
    end;

%% @doc Receive result and reply the result to the process that started the fsm (From).
waiting({error, Message}, SD=#state{errors=Errors0}) ->
    lager:info("Error received: ~p", [Message]),
    Errors = Errors0 ++ [Message],
    {next_state, waiting, SD#state{errors=Errors}, ?COORD_TIMEOUT};
waiting({Result, Message}, SD=#state{from=From}) ->
    lager:info("Result received: ~p", [{Result, Message}]),
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
