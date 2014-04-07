%% @doc The coordinator for stat write opeartions.  This example will
%% show how to properly replicate your data in Riak Core by making use
%% of the _preflist_.
-module(floppy_coord_fsm).
-behavior(gen_fsm).
-include("floppy.hrl").

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, receiveData/3]).


-define(BUCKET, <<"floppy">>).
-record(state, {
                from :: pid(),
                op :: atom(),
                key,
		client,
                param = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist2()}).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(From, Op, Key, Param, Client) ->
    io:format('Coord:The worker is about to start~n'),
    gen_fsm:start_link(?MODULE, [From, Op, Key, Param, Client], []).

%start_link(Key, Op) ->
%    io:format('The worker is about to start~n'),
%    gen_fsm:start_link(?MODULE, [Key, , Op, ], []).

receiveData(From, Key,Result) ->
   io:format("Sending message to~w~n",[From]),
   gen_fsm:send_event(From, {Key, Result}).
%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the s,,tate data.
init([From, Op,  Key, Param, Client]) ->
    SD = #state{
                from=From,
                op=Op, 
                key=Key,
                param=Param,
		client=Client},
		%num_w=1},
    io:format("Coord:init~n"),
    {ok, prepare, SD, 0}.


%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, ?N, replication),
    io:format("Coord:prepare~n"),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{
                            op=Op,
                            key=Key,
                            param=Param,
                            preflist=Preflist}) ->
    io:format("coord:execute~w~n",[self()]),
    case Preflist of 
	[] ->
	    io:format("no pref list~n"),
	    {stop, normal, SD0};
	[H|T] ->
	    io:format("First node:~w~n",[H]),
	    {IndexNode, _} = H,
	    floppy_rep_vnode:handle(IndexNode, self(), Op, Key, Param), 
            SD1 = SD0#state{preflist=T},
	    io:format("Coord fsm:Going to wait~n"),
            {next_state, waiting, SD1, 1000}
    end.

%% @doc Waits for 1 write reqs to respond.
waiting(timeout, SD0=#state{op=Op,
			   key=Key,
			   param=Param,
			   preflist=Preflist}) ->
    io:format("TIMEOUT: No acknowledge, retrying...~n"),
    case Preflist of 
	[] ->
	    io:format("no pref list~n"),
	    {stop, normal, SD0};
	[H|T] ->
	    io:format("First node:~w~n",[H]),
	    {IndexNode, _} = H,
	    floppy_rep_vnode:handle(IndexNode, self(), Op, Key, Param), 
            SD1 = SD0#state{preflist=T},
	    io:format("Coord fsm:Going to wait~n"),
            {next_state, waiting, SD1, 1000}
    end;

waiting({Key, Val}, SD=#state{from=_From,client=Client}) ->
    io:format("Received Message~n"),
    io:format("Floppy coord fsm got message!  ~w ~n", [Key]),
    proxy:returnResult(Key, Val, Client),    
    {stop, normal, SD};


waiting({error, no_key}, SD) ->
    {stop, normal, SD}.

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


