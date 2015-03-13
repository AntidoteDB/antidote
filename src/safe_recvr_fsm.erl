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

-module(safe_recvr_fsm).
-behaviour(gen_server).
-include("antidote.hrl").


-export([start_link/1]).
-export([init/1,
	 update_recvrd_time/3,
	 handle_cast/2,
	 handle_call/3,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).


-record(state, {recvrd_times,
		num_partitions,
		last_max,
                dcid}).

-define(REGISTER, global).
-define(REGNAME(MYDC,DC), {global,get_atom(MYDC,DC)}).


update_recvrd_time(DcId, Partition, Timestamp) ->
    try
	gen_server:cast(?REGNAME(inter_dc_manager:get_my_dc(),DcId),
			{update_recvrd_time, Partition, Timestamp})
    catch
	_:R ->
	    lager:error("Exception caught updating recvrd time, DcId ~p, error: ~p", [DcId,R]),
	    0
    end.


start_link(DcId) ->
    gen_server:start_link({?REGISTER, get_atom(inter_dc_manager:get_my_dc(),DcId)},
			  ?MODULE, [DcId], []).


init([DcId]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NumPartitions = riak_core_ring:num_partitions(Ring),
    SentTimes = dict:new(),
    {ok, #state{recvrd_times=SentTimes,
		num_partitions=NumPartitions,
		last_max=0,
		dcid=DcId}}.

%% Updates the sent time of a given partition
handle_cast({update_recvrd_time, Partition, UpTime}, State=#state{recvrd_times=LastRecvrd,
								     num_partitions=NumPartitions,
								     dcid=DcId,last_max=LastMax}) ->
    NewRecvrd = dict:store(Partition, UpTime, LastRecvrd),
    case dict:size(LastRecvrd) of
	NumPartitions ->
	    [{_FirstPartition, FirstTimestamp}|_Rest] = dict:to_list(LastRecvrd),
	    Time = dict:fold(fun(_Partition, Timestamp, MinTimestamp) ->
				     case Timestamp > MinTimestamp of
					 true ->
					     MinTimestamp;
					 _ -> 
					     Timestamp
				     end
			     end,
			     FirstTimestamp, LastRecvrd),
	    case Time > LastMax of
		true ->
		    case vectorclock:update_safe_clock_local(DcId,Time) of
			{ok,_} ->
			    NewTime = Time;
			{error,_} ->
			    NewTime = LastMax
		    end;
		false ->
		    NewTime = LastMax
	    end;
	_ ->
	    NewTime = LastMax
    end,
    {noreply, State#state{recvrd_times=NewRecvrd,last_max=NewTime}}.

handle_call(Message,_From,StateData) ->
    lager:error("Recevied bad msg in safe_recvr_fsm:  ~p",[Message]),
    {stop,badmsg,StateData}.

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
