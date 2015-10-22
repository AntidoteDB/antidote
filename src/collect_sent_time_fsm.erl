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

%% @doc : There are number of DCs-1 instances of this server running in each DC
%% Each of them keep track of the time of updates sent from each partition
%% to an external DC

-module(collect_sent_time_fsm).
-behaviour(gen_server).
-include("antidote.hrl").


-export([start_link/2]).
-export([init/1,
	 get_max_sent_time/2,
	 update_sent_time/3,
	 handle_cast/2,
	 handle_call/3,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).


-record(state, {sent_times,
		num_partitions,
		count,
                dcid}).

-define(REGISTER, global).
-define(REGNAME(MYDC,DC), {global,get_atom(MYDC,DC)}).


%% LastSentTs is the last sent safe_time that was sent
%% to the exernal DC, this value is included so that
%% incase the server was restarted, this reinits the state
get_max_sent_time(DcId, _LastSentTs) ->
    try
	gen_server:call(?REGNAME(inter_dc_manager:get_my_dc(),DcId),
			{get_max_sent_time})
    catch
	_:R ->
	    lager:error("Exception caught getting max sent time, DcId ~p, error: ~p", [DcId,R]),
	    0
    end.


update_sent_time(DcId, Partition, Timestamp) ->
    try
	gen_server:cast(?REGNAME(inter_dc_manager:get_my_dc(),DcId),
			{update_sent_time, Partition, Timestamp})
    catch
	_:R ->
	    lager:error("Exception caught updating sent time, DcId ~p, error: ~p", [DcId,R]),
	    0
    end.


start_link(DcId, StartTimestamp) ->
    lager:info("Calling start at me ~w, for ~w", [inter_dc_manager:get_my_dc(),DcId]),
    gen_server:start_link({?REGISTER, get_atom(inter_dc_manager:get_my_dc(),DcId)},
			  ?MODULE, [DcId, StartTimestamp], []).


init([DcId, _StartTimestamp]) ->
    lager:info("Starting safe time sender for ~w", [DcId]),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NumPartitions = riak_core_ring:num_partitions(Ring),
    SentTimes = dict:new(),
    {ok, #state{sent_times=SentTimes,
		count=0,
		num_partitions=NumPartitions,
		dcid=DcId}}.

%% Updates the sent time of a given partition
handle_cast({update_sent_time, Partition, Timestamp}, State=#state{sent_times=LastSent}) ->
    NewSent = dict:store(Partition, Timestamp, LastSent),
    {noreply, State#state{sent_times=NewSent}}.


handle_call({get_max_sent_time}, _From, State=#state{sent_times=LastSent,
						     count=Count,
						     num_partitions=NumPartitions}) ->
    %% Maybe should use a more efficient data-structure so you don't
    %% have to iterate over an entire list each time
    %% assume all partitions participate
    case dict:size(LastSent) of
	NumPartitions ->
	    %%NumPartitions ->
	    [{_FirstPartition, FirstTimestamp}|_Rest] = dict:to_list(LastSent),
	    Time = dict:fold(fun(_Partition, Timestamp, MinTimestamp) ->
				     case Timestamp > MinTimestamp of
					 true ->
					     MinTimestamp;
					 _ -> 
					     Timestamp
				     end
			     end,
			     FirstTimestamp, LastSent),
	    {reply, Time, State#state{count=0}};
	Asize ->
	    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
	    NumPartitions1 = riak_core_ring:num_partitions(Ring),
	    case Count > 100 of
		true ->
		    lager:error("not all nodes participated in safe time calc: ~w, ~w", [Asize, dict:to_list(LastSent)]),
		    NewSent = dict:new(),
		    {reply, 0, State#state{sent_times=NewSent, count=0,num_partitions=NumPartitions1}};
		false ->
		    {reply, 0, State#state{count=Count+1,num_partitions=NumPartitions1}}
	    end
    end.


handle_info(Message, StateData) ->
    lager:error("Recevied info:  ~p",[Message]),
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State) -> {ok, StateName, State}.

terminate(_Reason, _SN) ->
    ok.

%% Helper function
get_atom({MyDcAddr, MyPort}, {DcAddr, Port}) ->
    list_to_atom(atom_to_list(?MODULE) ++ atom_to_list(MyDcAddr) ++
		     integer_to_list(MyPort) ++ atom_to_list(DcAddr) ++ integer_to_list(Port)).
