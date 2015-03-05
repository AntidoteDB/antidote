-module(inter_dc_manager).
%%-behaviour(gen_server).

-export([%%start_link/0
	 get_my_dc/0,
         start_receiver/1,
         get_dcs/0,
         add_dc/1,
         add_list_dcs/1,
	 get_my_read_dc/0,
         start_read_receiver/1,
         get_read_dcs/0,
         add_read_dc/1,
         add_list_read_dcs/1,
	 %% get_my_safe_send_dc/0,
         %% start_safe_send_receiver/1,
         %% get_safe_send_dcs/0,
         %% add_safe_send_dc/1,
         %% add_list_safe_send_dcs/1,
	 set_replication_keys/1
	]).

%%-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
%%        terminate/2, code_change/3]).


-define(META_PREFIX_DC, {dcid,port}).
-define(META_PREFIX_MY_DC, {mydcid,port}).
%% -define(META_PREFIX_SAFE_DC, {dcidsafe,port}).
-define(META_PREFIX_READ_DC, {dcidread,port}).
-define(META_PREFIX_MY_READ_DC, {mydcidread,port}).

%% -record(state, {
%% 	  dcs,
%% 	  dcs_read,
%% 	  dcs_safe_send,
%% 	  port,
%% 	  port_read,
%% 	  port_safe_send
%%     }).


%% ===================================================================
%% Public API
%% ===================================================================

%% start_link() ->
%%     gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_my_dc() ->
    %% gen_server:call(?MODULE, get_my_dc, infinity).
    riak_core_metadata:get(?META_PREFIX_MY_DC,mydc).

start_receiver({DcIp, Port}) ->
    %% gen_server:call(?MODULE, {start_receiver, Port}, infinity).
    riak_core_metadata:put(?META_PREFIX_MY_DC,mydc,{DcIp,Port}),
    %%riak_core_metadata:put(?META_PREFIX_DC,{DcIp,Port},0),
    ok.

get_dcs() ->
    %%gen_server:call(?MODULE, get_dcs, infinity).
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({DC,[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

add_dc(NewDC) ->
    %% gen_server:call(?MODULE, {add_dc, NewDC}, infinity).
    riak_core_metadata:put(?META_PREFIX_DC,NewDC,0),
    ok.

add_list_dcs(DCs) ->
    %% gen_server:call(?MODULE, {add_list_dcs, DCs}, infinity).
    lists:foldl(fun(DC,_Acc) ->
			riak_core_metadata:put(?META_PREFIX_DC,DC,0)
		end, 0, DCs),
    ok.


get_my_read_dc() ->
    %% gen_server:call(?MODULE, get_my_read_dc, infinity).
    riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc).

start_read_receiver({DcIp,Port}) ->
    %%gen_server:call(?MODULE, {start_read_receiver, Port}, infinity).
    riak_core_metadata:put(?META_PREFIX_MY_READ_DC,mydc,{DcIp,Port}),
    %%riak_core_metadata:put(?META_PREFIX_READ_DC,{DcIp,Port},0),
    ok.

get_read_dcs() ->
    %% gen_server:call(?MODULE, get_read_dcs, infinity).
    DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
    lists:foldl(fun({DC,[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

add_read_dc(NewDC) ->
    %% gen_server:call(?MODULE, {add_read_dc, NewDC}, infinity).
    riak_core_metadata:put(?META_PREFIX_READ_DC,NewDC,0),
    ok.

add_list_read_dcs(DCs) ->
    %% gen_server:call(?MODULE, {add_list_read_dcs, DCs}, infinity).
    lists:foldl(fun(DC,_Acc) ->
			riak_core_metadata:put(?META_PREFIX_READ_DC,DC,0)
		end, 0, DCs),
    ok.



%% get_my_safe_send_dc() ->
%%     %%gen_server:call(?MODULE, get_my_safe_send_dc, infinity).
%%     {ok, riak_core_metadata:get(?META_PREFIX_SAFE_DC,mydc)}.

%% start_safe_send_receiver({DcIp,Port}) ->
%%     %% gen_server:call(?MODULE, {start_safe_send_receiver, Port}, infinity).
%%     riak_core_metadata:put(?META_PREFIX_SAFE_DC,mydc,{DcIp,Port}),
%%     riak_core_metadata:put(?META_PREFIX_SAFE_DC,{DcIp,Port},0),
%%     ok.

%% get_safe_send_dcs() ->
%%     %% gen_server:call(?MODULE, get_safe_send_dcs, infinity).
%%     DcList = riak_core_metadata:to_list(?META_PREFIX_SAFE_DC),
%%     NewList = lists:foldl(fun({DC,[0|_T]},NewAcc) ->
%% 			lists:append([DC],NewAcc) end,
%% 		[], DcList),
%%     {ok,NewList}.
	
%% add_safe_send_dc(NewDC) ->
%%     %% gen_server:call(?MODULE, {add_safe_send_dc, NewDC}, infinity).
%%     riak_core_metadata:put(?META_PREFIX_SAFE_DC,NewDC,0),
%%     ok.

%% add_list_safe_send_dcs(DCs) ->
%%     %% gen_server:call(?MODULE, {add_list_safe_send_dcs, DCs}, infinity).
%%     lists:foldl(fun(DC,_Acc) ->
%% 			riak_core_metadata:put(?META_PREFIX_SAFE_DC,DC,0)
%% 		end, 0, DCs),
%%     ok.




set_replication_keys(KeyDescription) ->
    %%gen_server:call(?MODULE, {set_replication_keys, KeyDescription}, infinity).
    replication_check:set_replication(KeyDescription).


%% %% ===================================================================
%% %% gen_server callbacks
%% %% ===================================================================

%% init([]) ->
%%     {ok, #state{dcs=[]}}.

%% handle_call(get_my_dc, _From, #state{port=Port} = State) ->
%%     {reply, {ok, {my_ip(),Port}}, State};

%% handle_call({start_receiver, Port}, _From, State) ->
%%     {ok, _} = antidote_sup:start_rep(Port),
%%     {reply, {ok, {my_ip(),Port}}, State#state{port=Port}};

%% handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
%%     {reply, {ok, DCs}, State};

%% handle_call({add_dc, NewDC}, _From, #state{dcs=DCs0} = State) ->
%%     DCs = DCs0 ++ [NewDC],
%%     {reply, ok, State#state{dcs=DCs}};

%% handle_call({add_list_dcs, DCs}, _From, #state{dcs=DCs0} = State) ->
%%     DCs1 = DCs0 ++ DCs,
%%     {reply, ok, State#state{dcs=DCs1}};



%% handle_call(get_my_read_dc, _From, #state{port_read=Port} = State) ->
%%     {reply, {ok, {my_ip(),Port}}, State};

%% handle_call({start_read_reciever, Port}, _Form, State) ->
%%     {ok, _} = antidote_sup:start_cross_dc_read_communication_recvr(Port),
%%     {reply, {ok, {my_ip(),Port}}, State#state{port_read=Port}};

%% handle_call(get_read_dcs, _From, #state{dcs_read=DCs} = State) ->
%%     {reply, {ok, DCs}, State};

%% handle_call({add_read_dc, NewDC}, _From, #state{dcs_read=DCs0} = State) ->
%%     DCs = DCs0 ++ [NewDC],
%%     {reply, ok, State#state{dcs_read=DCs}};

%% handle_call({add_list_read_dcs, DCs}, _From, #state{dcs_read=DCs0} = State) ->
%%     DCs1 = DCs0 ++ DCs,
%%     {reply, ok, State#state{dcs_read=DCs1}};




%% handle_call({start_collect_sent}, _From, State) ->
%%     {ok, _} = antidote_sup:start_collect_sent(),
%%     {reply, ok, State};

%% handle_call({start_safe_time_sender}, _From, State) ->
%%     {ok, _} = antidote_sup:start_safe_time_sender(),
%%     {reply, ok, State};



%% handle_call({start_senders}, _From, State) ->
%%     {ok, _} = antidote_sup:start_senders(),
%%     {reply, ok, State};

%% handle_call({start_recvrs, ComPort, ReadPort}, _From, State) ->
%%     {ok, _} = antidote_sup:start_recvrs(ComPort,ReadPort),
%%     {reply, {ok, {my_ip(),ComPort}}, State#state{port=ComPort,port_read=ReadPort}};



%% %% handle_call(get_my_safe_send_dc, _From, #state{port_safe_send=Port} = State) ->
%% %%     {reply, {ok, {my_ip(),Port}}, State};

%% %% handle_call({start_safe_send_reciever, Port}, _Form, State) ->
%% %%     %% ToDo: fix this
%% %%     {ok, _} = antidote_sup:state_read_rep(Port),
%% %%     {reply, {ok, {my_ip(),Port}}, State#state{port_safe_send=Port}};

%% %% handle_call(get_safe_send_dcs, _From, #state{dcs_safe_send=DCs} = State) ->
%% %%     {reply, {ok, DCs}, State};

%% %% handle_call({add_safe_send_dc, NewDC}, _From, #state{dcs_safe_send=DCs0} = State) ->
%% %%     DCs = DCs0 ++ [NewDC],
%% %%     {reply, ok, State#state{dcs_safe_send=DCs}};

%% %% handle_call({add_list_safe_send_dcs, DCs}, _From, #state{dcs_safe_send=DCs0} = State) ->
%% %%     DCs1 = DCs0 ++ DCs,
%% %%     {reply, ok, State#state{dcs_safe_send=DCs1}};


%% handle_call({set_replication_keys, ReplicationKeys}, _From, State) ->
%%     {reply, replication_check:set_replication(ReplicationKeys), State}.


%% handle_cast(_Info, State) ->
%%     {noreply, State}.

%% handle_info(_Info, State) ->
%%     {noreply, State}.

%% %% @private
%% terminate(_Reason, _State) ->
%%     ok.

%% %% @private
%% code_change(_OldVsn, State, _Extra) ->
%%     {ok, State}.

%% my_ip() ->
%%     {ok, List} = inet:getif(),
%%     {Ip, _, _} = hd(List),
%%     inet_parse:ntoa(Ip).
