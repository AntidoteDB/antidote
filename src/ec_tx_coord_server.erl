-module(ec_tx_coord_server).
-behaviour(gen_server).
-export([start/2, start_link/2, run/1, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%% The friendly supervisor is started dynamically!
-record(state, {index=1,
                limit=0,
                tx_sups=[]
                }).

start(Limit, Sup) when is_integer(Limit) ->
    gen_server:start({local, ?MODULE}, ?MODULE, {Limit, Sup}, []).

start_link(Limit, Sup) when is_integer(Limit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Limit, Sup}, []).

run(Args) ->
    gen_server:cast(?MODULE, {async, Args}).

stop() ->
    gen_server:call(?MODULE, stop).

%% Gen server
init({Limit, Sup}) ->
    self() ! {start_sup, Sup},
    {ok, #state{limit=Limit}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({async, Args}, S=#state{index=Index, tx_sups=TxSups, limit=Limit}) ->
    CurrentSup = lists:nth(Index, TxSups),
    spawn(supervisor,start_child, [CurrentSup, Args]),
    NewIndex= (Index rem Limit) + 1,
    {noreply, S#state{index= NewIndex}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({start_sup, Sup}, S= #state{limit=Limit}) ->
    SupId = wait_until_registered(Sup),
    T = lists:seq(1, Limit),
    TxSups=lists:foldl(fun(_, Acc) -> {ok,PId}=supervisor:start_child(SupId, []), [PId]++Acc end, [], T),
    {noreply, S#state{limit=Limit, tx_sups=TxSups}};

handle_info(_Info, State) ->
    {ok, State}.

wait_until_registered(Sup) ->
    case whereis(Sup) of 
        undefined ->
            lager:info("Not registered ~w",[Sup]),
            timer:sleep(100),
            wait_until_registered(Sup);
        PId ->
            lager:info("Registered!"),
            PId
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

