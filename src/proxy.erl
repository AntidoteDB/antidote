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
	 update/3,
	 read/2,
	 returnResult/3]).
 
-record(state, {}).

update(Key, Op, Client)->
    gen_server:call(?MODULE, {update, Key, Op, Client}).

read(Key, Client)->
    gen_server:call(?MODULE, {read, Key, Client}).

returnResult(Key, Result, Client) ->
    gen_server:call(?MODULE, {returnResult, Key, Result, Client}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
 
init([]) ->
    {ok, #state{}}.

handle_call({update, Key, Op, Client}, From, State ) ->
    io:format("Client: ~w From: ~w ~n", [Client, From]),
    floppy_coord_sup:start_fsm([self(), update, Key, Op, From]),
    {noreply, State};

handle_call({read, Key, Client}, From, State) ->
    io:format("Client: ~w From: ~w ~n", [Client, From]),
    floppy_coord_sup:start_fsm([self(), read, Key, noop, From]),
    {noreply, State};

handle_call({returnResult, Key, Result, Client}, _From, State) ->
    io:format("Proxy returning result~n"),
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
