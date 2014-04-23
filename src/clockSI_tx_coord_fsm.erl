%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(clockSI_tx_coord_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, finishOp/3]).


%-record(operationCSI, {opType, key, params}).


-record(state, {
                from :: pid(),
                operations :: list,
                snapshotTime = undefined :: term() | undefined,
                txType = undefined :: term() | undefined,
                currentOp = undefined :: term() | undefined,
                currentOpLeader :: riak_core_apl:preflist2()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, SnapshotTime, Operations) ->
    gen_fsm:start_link(?MODULE, [From, SnapshotTime, Operations], []).

finishOp(From, Key,Result) ->
   gen_fsm:send_event(From, {Key, Result}).
   
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the s,,tate data.
init([From, SnapshotTime, Operations]) ->

%				if
%                	lists:keymember(update, 1, Operations)  ->
%                		TxType = update;
%                	true ->
                		TxType = read,
 %               end,
                
    SD = #state{
                from=From,
                operations = Operations, 
                snapshotTime=SnapshotTime,
                txType=TxType 	
		},
    {ok, prepare, SD, 0}.


%% @doc Prepare the execution of the operation by obtaining an operation from the list,
%%		and getting the leader of the operation.
prepare(timeout, SD0=#state{operations=Operations}) ->
	case Operations of 
	[] ->
	    io:format("Executed all operations.~n"),
	    {stop, normal, SD0};
	[Op|TailOps] ->
		[Op|TailOps] = Operations,
		{_, Key,_} = Op, 
		DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    	[Leader] = riak_core_apl:get_primary_apl(DocIdx, 1, replication),	
        SD1 = SD0#state{operations=TailOps, currentOp=Op, currentOpLeader=Leader},
        {next_state, execute, SD1, 0}
    end.


%% @doc Contact the leader computed in the prepare state for it to execute the operation
execute(timeout, SD0=#state{
                            currentOp=CurrentOp,
               				snapshotTime=SnapshotTime,
                            currentOpLeader=CurrentOpLeader}) ->
    [OpType, Key, Param]=CurrentOp,                        
    io:format("Coord: Execute operation ~w ~w ~w~n",[OpType, Key, Param]),
	io:format("Coord: Forward to node~w~n",[CurrentOpLeader]),
	{IndexNode, _} = CurrentOpLeader,
	clockSI_vnode:handleOp(IndexNode, self(), OpType, Key, Param, SnapshotTime), 
    SD1 = SD0#state{},
    {next_state, prepare, SD1, ?INDC_TIMEOUT}.


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


