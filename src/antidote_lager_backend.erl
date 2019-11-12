-module(antidote_lager_backend).

%% This module can be removed once lager is removed from all dependencies of antidote

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
        code_change/3]).

%% @private
init([Level]) when is_atom(Level) -> {ok, no_state};
init([_, true]) -> {ok, no_state};
init([_, false]) -> {ok, no_state};
init([_, {Formatter, _}]) when is_atom(Formatter) -> {ok, no_state};
init(_) -> {ok, no_state}.

%% @private
handle_call(get_loglevel, State) -> {ok, {mask, 255}, State};
handle_call({set_loglevel, _}, State) -> {ok, ok, State};
handle_call(_Request, State) -> {ok, ok, State}.

%% @private
handle_event({log, Message}, State) ->
    {lager_msg, _, _, Level, _, _, InternalMessage} = Message,
    ToLog = binary_to_list(iolist_to_binary(InternalMessage)),
    case Level of
        emergency -> logger:emergency("~p", [ToLog]);
        alert -> logger:alert("~p", [ToLog]);
        critical -> logger:critical("~p", [ToLog]);
        error -> logger:error("~p", [ToLog]);
        warning -> logger:warning("~p", [ToLog]);
        notice -> logger:notice("~p", [ToLog]);
        info -> logger:info("~p", [ToLog]);
        debug -> logger:debug("~p", [ToLog])
    end,
    {ok, State};

handle_event(_Event, State) -> {ok, State}.
handle_info(_Info, State) -> {ok, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

