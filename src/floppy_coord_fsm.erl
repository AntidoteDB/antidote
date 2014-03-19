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
-export([prepare/2, execute/2, waiting/2]).


-define(BUCKET, <<"floppy">>).
-record(state, {req_id :: pos_integer(),
                from :: pid(),
                op :: atom(),
                key,
                val = undefined :: term() | undefined,
                preflist :: riak_core_apl:preflist2()}).
                %num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

%start_link(Key, Op) ->
%    start_link(Key, Op).

start_link(ReqID, From, Op, Key, Val) ->
    io:format('Coord:The worker is about to start~n'),
    gen_fsm:start_link(?MODULE, [ReqID, From, Op, Key, Val], []).

%start_link(Key, Op) ->
%    io:format('The worker is about to start~n'),
%    gen_fsm:start_link(?MODULE, [Key, , Op, ], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Op,  Key, Val]) ->
    SD = #state{req_id=ReqID,
                from=From,
                op=Op, 
                key=Key,
                val=Val},
		%num_w=1},
    io:format("Coord:init~n"),
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key}) ->
    DocIdx = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = riak_core_apl:get_apl(DocIdx, ?N, replication),
    io:format("Coord:prepare~n"),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID,
                            op=Op,
                            key=Key,
                            val=Val,
                            preflist=Preflist}) ->
			%    num_w=W}) ->
    io:format("coord:execute"),
    case Preflist of 
	[] ->
	    io:format("no pref list~n"),
	    {stop, normal, SD0};
	[H|T] ->
	    io:format("something in list~w~n",[H]),
	    floppy_rep_vnode:handle(H, Op, ReqID, Key, Val), 
            SD1 = SD0#state{preflist=[T]},
            {next_state, waiting, SD1}
    end.

%% @doc Waits for 1 write reqs to respond.
waiting({ReqID,Key, Val}, SD0=#state{from=From}) ->
    SD = SD0#state{val=Val},
    io:format("Floppy coord fsm got message! ~w ~w ~w ~w ~n", [ReqID, From, Key,Val]),
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


