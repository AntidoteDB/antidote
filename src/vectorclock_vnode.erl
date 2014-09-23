-module(vectorclock_vnode).

-behaviour(riak_core_vnode).

-include("floppy.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1]).

-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-record(currentclock,{last_received_clock, partition_vectorclock}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Initialize the clock
init([_Partition]) ->
    {ok, #currentclock{last_received_clock=dict:new(),
                      partition_vectorclock=dict:new()}}.

%% @doc
handle_command(get_clock, _Sender, #currentclock{partition_vectorclock=Clock} = State) ->
    {reply, {ok, Clock}, State};

%% @doc
%% update_vectorclock(partition, dc, time)
%% if last_received_vectorclock[partition][dc] < time    
%%    partition_vectorclock[partition][dc] = last_received_vectorclock[partition][dc]
%% last_received_vectorclock[partition][dc] = time
handle_command({update_clock, DcId, Timestamp}, _Sender, 
               #currentclock{last_received_clock=LastClock,
                             partition_vectorclock=VClock
                            } = State) ->
    case dict:find(DcId, LastClock) of
        {ok, LClock} ->
            case LClock < Timestamp of
                true ->
                    NewLClock = dict:store(DcId, Timestamp, LastClock),
                    NewPClock = dict:store(DcId, LClock, VClock),
                    {reply, {ok, NewLClock}, State#currentclock{last_received_clock=NewLClock,
                                                              partition_vectorclock=NewPClock}
                    };
                false ->                    
                    {reply, {ok, VClock}, State}
            end;
        error ->
             NewLClock = dict:store(DcId, Timestamp, LastClock),
            {reply, {ok, NewLClock}, State#currentclock{last_received_clock=NewLClock}}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command( _Message , _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
