-module(inter_dc_manager).
-behaviour(gen_server).

-export([start_link/0,
         get_my_dc/0,
         start_receiver/1,
         get_dcs/0,
         add_dc/1,
         add_list_dcs/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {
        dcs,
        port
    }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_my_dc() ->
    gen_server:call(?MODULE, get_my_dc, infinity).

start_receiver(Port) ->
    gen_server:call(?MODULE, {start_receiver, Port}, infinity).

get_dcs() ->
    gen_server:call(?MODULE, get_dcs, infinity).

add_dc(NewDC) ->
    gen_server:call(?MODULE, {add_dc, NewDC}, infinity).

add_list_dcs(DCs) ->
    gen_server:call(?MODULE, {add_list_dcs, DCs}, infinity).
   

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #state{dcs=[]}}.

handle_call(get_my_dc, _From, #state{port=Port} = State) ->
    {reply, {ok, {my_ip(),Port}}, State};

handle_call({start_receiver, Port}, _From, State) ->
    {ok, _} = antidote_sup:start_rep(Port),
    {reply, {ok, {my_ip(),Port}}, State#state{port=Port}};

handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
    {reply, {ok, DCs}, State};

handle_call({add_dc, NewDC}, _From, #state{dcs=DCs0} = State) ->
    DCs = DCs0 ++ [NewDC],
    {reply, ok, State#state{dcs=DCs}};

handle_call({add_list_dcs, DCs}, _From, #state{dcs=DCs0} = State) ->
    DCs1 = DCs0 ++ DCs,
    {reply, ok, State#state{dcs=DCs1}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

my_ip() ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    inet_parse:ntoa(Ip).
