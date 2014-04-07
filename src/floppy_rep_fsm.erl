%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(floppy_rep_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

-record(state, {
                from :: pid(),
                op :: atom(),
                key,
                param = undefined :: term() | undefined,
		readresult,
                preflist :: riak_core_apl:preflist2(),
		num_to_ack = 0 :: non_neg_integer(),
		opid}).
-define(BUCKET, <<"floppy">>).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(From, Op,  Key, Param, OpId) ->
    io:format("rep:The worker is about to start~w~n",[From]),
    gen_fsm:start_link(?MODULE, [From, Op, Key, Param, OpId], []).

%start_link(Key, Op) ->
%    io:format('The worker is about to start~n'),
%    gen_fsm:start_link(?MODULE, [Key, , Op, ], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([From, Op,  Key, Param, OpId]) ->
    SD = #state{from=From,
                op=Op,
                key=Key,
                param=Param,
		opid= OpId,
		readresult=[]},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, ?N, logging),
    NewPref = [Node|| {Node,_} <- Preflist ],
    SD = SD0#state{preflist=NewPref},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{op=Op,
                            key=Key,
                            param=Param,
                            preflist=Preflist,
			    opid=OpId}) ->
    case Op of 
	update ->
	    io:format("replication propagating updates!~n"),
	    logging_vnode:dupdate(Preflist, Key, Param, OpId),
	    SD1 = SD0#state{num_to_ack=?NUM_W},
	    {next_state, waiting, SD1};
%	create ->
%	    io:format("replication propagating create!~w~n",[Preflist]),
%	    logging_vnode:dcreate(Preflist, Key, Param, OpClock),
%	    Num_w = SD0#state.num_w,
%	    SD1 = SD0#state{num_to_ack=Num_w},
%	    {next_state, waiting, SD1};
	read ->
	    io:format("replication propagating reads!~n"),
	    logging_vnode:dread(Preflist, Key),
	    SD1 = SD0#state{num_to_ack=?NUM_R},
	    {next_state, waiting, SD1};
	_ ->
	    io:format("wrong commands~n"),
	   %%reply message to user
	    {stop, normal, SD0}
    end.

%% @doc Waits for 1 write reqs to respond.
waiting({ok, Result}, SD=#state{op=Op, from= From, key=Key, readresult= ReadResult , num_to_ack= NumToAck}) ->
    io:format("rep_fsm:got reply~n"),
    if Op == read ->
	Result1 = union_ops(ReadResult, Result);
    true ->
	Result1 = Result
    end,
    io:format("Result1:~w~n",[Result1]),
    if NumToAck == 1 -> 
    	io:format("Finish collecting replies for ~w ~w ~n", [Op, Result1]),
	floppy_coord_fsm:receiveData(From,  Key, Result1),	
	{stop, normal, SD};
	true ->
         io:format("Keep collecting keys!"),
	{next_state, waiting, SD#state{num_to_ack= NumToAck-1}}
    end;

waiting({error, no_key}, SD) ->
    {stop, normal, SD}.

union_ops(Set1,[]) ->
    Set1;
union_ops(Set1, [Op|T]) ->
    io:format("Printing op ~w ~n", [Op]),
    {_, #operation{opNumber= OpId}} = Op,
    Set3 = union_list(Set1, OpId,[]),
    Set4 = lists:append(Set3, [Op]),
    union_ops(Set4, T).


union_list([], _OpId, Set2) ->
    Set2;
union_list([H|T], OpId, Set2) ->
    {_, #operation{opNumber= OpNum}} = H,
    if OpNum /= OpId ->
	Set3 = lists:append(Set2, [H])
    end,
    union_list(T, OpId, Set3).
    

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


