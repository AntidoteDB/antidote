-module(proxy).
 
-behaviour(gen_server).
 
-export([start_link/0]).
 
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
	 update/2,
	 read/1,
	 returnResult/3]).
 
-record(state, {}).

update(Key, Op)->
    gen_server:call(?MODULE, {update, Key, Op}).

read(Key)->
    gen_server:call(?MODULE, {read, Key}).

returnResult(Key, Result, Client) ->
    gen_server:call(?MODULE, {returnResult, Key, Result, Client}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
 
init([]) ->
    {ok, #state{}}.

handle_call({update, Key, Op}, From, State ) ->
    io:format("Proxy: Update from client ~w~n", [From]),
    floppy_coord_sup:start_fsm([self(), update, Key, Op, From]),
    {noreply, State};

handle_call({read, Key}, From, State) ->
    io:format("Proxy: Read from client ~w~n", [From]),
    floppy_coord_sup:start_fsm([self(), read, Key, noop, From]),
    {noreply, State};

handle_call({returnResult, Key, Result, Client}, _From, State) ->
    io:format("Proxy: Returning result~n"),
    gen_server:reply(Client, {Key, Result}),
    {reply, {ok}, State};
 
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.
 
handle_cast(_Msg, State) ->
    {noreply, State}.
 
handle_info(_Info, State) ->
    {noreply, State}.
 
terminate(_Reason, _State) ->
    ok.
 
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
