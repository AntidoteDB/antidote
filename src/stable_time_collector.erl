%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% InterDC subscriber - connects to remote PUB sockets and listens to a defined subset of messages.
%% The messages are filter based on a binary prefix.

-module(stable_time_collector).
-behaviour(gen_server).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
	 update_partition_count/4,
	 get_ping/1,
	 get_registered_name/0,
	 start_timer/0
	]).

%% Server methods
-export([
	 init/1,
	 start_link/0,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3
	]).

%% State
-record(state, {
	  ready :: boolean(),
	  partition :: partition_id(),
	  partition_op_count :: cache_id(),
	  partition_time :: cache_id(),
	  timer :: any()
	 }).

%%%% API --------------------------------------------------------------------+


-spec update_partition_count(atom(), partition_id(),[{dcid(),#op_number{}}],clock_time()) -> ok.
update_partition_count(ServerName, Partition, DCIDOpList, Time) ->
    gen_server:cast({global, ServerName}, {update_partition_count, Partition, DCIDOpList, Time}).

-spec get_ping(atom()) -> #partial_ping{}.
get_ping(ServerName) ->
    gen_server:call({global, ServerName}, get_ping).

%% Start the heartbeat timer
-spec start_timer() -> ok.
start_timer() -> 
    gen_server:call({global, get_registered_name()}, start_timer).

-spec get_registered_name() -> atom().
get_registered_name() ->
    DCID = dc_meta_data_utilities:get_my_dc_id(),
    DCIDatom = dc_utilities:dc_id_to_atom(DCID),
    list_to_atom(atom_to_list(stc) ++ atom_to_list(DCIDatom)).

%%%% Server methods ---------------------------------------------------------+

start_link() ->
    gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

init([]) ->
    [Partition|_Rest] = dc_utilities:get_my_partitions(),
    _ = global:register_name(get_registered_name(),self()),
    {ok, #state{timer = none,
		ready = false,
		partition = Partition,
		partition_op_count = ets:new(partition_op_count,[set]),
		partition_time = ets:new(partition_time, [set])}}.

handle_cast({update_partition_count, Partition, DCIDOpList, Time}, State = #state{ready=false}) ->
    handle_cast({update_partition_count, Partition, DCIDOpList, Time}, State#state{ready=true});
handle_cast({update_partition_count, Partition, DCIDOpList, Time}, State) ->
    update_partition_count_internal(Partition, DCIDOpList, Time, State),
    {noreply, State}.

update_partition_count_internal(Partition, DCIDOpList, Time,
				#state{partition_op_count = OpTable, partition_time = TimeTable}) ->
    true = ets:insert(OpTable, {Partition, DCIDOpList}),
    true = ets:insert(TimeTable, {Partition, Time}).

handle_info(ping, State = #state{partition = Partition}) ->
    Ping = create_ping(State),
    Txn = inter_dc_txn:partial_ping(Partition, Ping),
    inter_dc_pub:broadcast(Txn),
    {noreply, set_timer(State)};
handle_info(_Info, State) ->
    {noreply, State}.

-spec create_ping(#state{}) -> #partial_ping{}.
create_ping(#state{ready=false}) ->
    #partial_ping{partition_dcid_op_list = [], time = 0};
create_ping(#state{partition_op_count = OpTable, partition_time = TimeTable}) ->
    PartitionDCIDOpList = ets:tab2list(OpTable),
    [{_Partition,FirstTime}|Rest] = ets:tab2list(TimeTable),
    MinTime = 
	lists:foldl(fun({_P,NextTime},Acc) ->
			    case NextTime < Acc of
				true -> NextTime;
				false -> Acc
			    end
		    end, FirstTime, Rest),
    #partial_ping{partition_dcid_op_list = PartitionDCIDOpList, time = MinTime}.
    
%% Start the timer
handle_call(start_timer, _Sender, State) ->
    {reply, ok, set_timer(State)};
				
handle_call(get_ping, _From, State) ->
    {reply, create_ping(State), State};

handle_call(_Request, _From, State) -> {reply, {error, unknown_call}, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, State) ->
    _ = del_timer(State),
    ok.

%% Cancels the ping timer, if one is set.
-spec del_timer(#state{}) -> #state{}.
del_timer(State = #state{timer = none}) -> State;
del_timer(State = #state{timer = Timer}) ->
  _ = erlang:cancel_timer(Timer),
  State#state{timer = none}.

%% Cancels the previous ping timer and sets a new one.
-spec set_timer(#state{}) -> #state{}.
set_timer(State) ->
    State1 = del_timer(State),
    State1#state{timer = erlang:send_after(?HEARTBEAT_PERIOD, self(), ping)}.
