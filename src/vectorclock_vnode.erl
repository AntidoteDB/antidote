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

-record(currentclock,{clock}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Initialize the clock
init([_Partition]) ->
     {ok, #currentclock{clock = orddict:new()}}.

%% @doc 
handle_command({get_clock}, _Sender, #currentclock{clock = Clock} = State) ->
    {reply, {ok, Clock}, State};

%% @doc 
handle_command({update_clock, Dc_id, Timestamp}, _Sender, #currentclock{clock = Clock} = State) ->
    New_clock = orddict:update(Dc_id, 
                               fun(Value) -> 
                                       case Timestamp > Value of 
                                           true -> Timestamp;
                                           false -> Value 
                                           end
                               end,
                               Timestamp,
                               Clock),
    {reply, {ok, New_clock}, State#currentclock{clock = New_clock}};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
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
    {noreply, State}.

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
