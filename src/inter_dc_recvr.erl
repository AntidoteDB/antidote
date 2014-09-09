-module(inter_dc_recvr).

-behaviour(gen_server).

%%public API
-export([start_link/0, replicate/4, stop/1]).

%%gen_server call backs
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%%API

start_link() ->
    {ok, PID} = gen_server:start_link(?MODULE, [], []),
    register(inter_dc_recvr, PID),
    {ok, PID}.

replicate(Nodes, Name, Payload, Origin) ->
    lager:info("Sending update ~p to ~p ~n",[Payload, Nodes]),
    gen_server:abcast(Nodes, Name, {replicate, Payload, from, Origin}).

stop(Pid)->
    gen_server:call(Pid, terminate).

%%Server functions
init([]) ->
    {ok, []}.

handle_cast({replicate, Payload, from, {PID,Node}}, _StateData) ->
    apply(Payload),
    DcId = dc_utilities:get_my_dc_id(),
    gen_server:cast({inter_dc_recvr,Node},{acknowledge,Payload,PID,DcId}),
    {noreply,_StateData};
handle_cast({acknowledge, _Payload, PID, DcId}, _StateData) ->
    inter_dc_repl:acknowledge(PID,DcId),
    {noreply, _StateData};
handle_cast({get_update, Partition, OtherDc, FromOp}, _StateData) ->
    inter_dc_repl_vnode:get_update(Partition, OtherDc, FromOp).

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State}.

handle_info(Msg, State) ->
    lager:info("Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    lager:info("Inter_dc_repl_recvr stopping"),
    ok;
terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}.

%%private

apply(Payload) ->
    io:format("Recieved update ~p ~n",[Payload]), 
    inter_dc_recvr_vnode:store_updates(Payload),    
    ok.
