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


-export([start_link/1]).
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
                dcid}).


%% LastSentTs is the last sent safe_time that was sent
%% to the exernal DC, this value is included so that
%% incase the server was restarted, this reinits the state
get_max_sent_time(DcId, LastSentTs) ->
    Pid = global:whereis_name(get_atom(DcId)),
    %% Start this service if it is down
    %% Should add a try_catch incase concurrent server creations/failures
    case Pid of
	undefined ->
	    start_link([DcId, LastSentTs]);
	_ -> Pid
    end,
    gen_server:call({global, get_atom(DcId)}, {get_max_sent_time}).


update_sent_time(DcId, Partition, Timestamp) ->
    Pid = global:whereis_name(get_atom(DcId)),
    %% Start this service if it is down
    case Pid of
	undefined ->
	    start_link([DcId, 0]);
	_ -> Pid
    end,
    gen_server:cast({global, get_atom(DcId)},
		    {update_sent_time, Partition, Timestamp}).


start_link([DcId, StartTimestamp]) ->
    gen_server:start_link({global, get_atom(DcId)}, ?MODULE, [DcId, StartTimestamp], []).


%% TODO, Fix, Are partitions based on numbers?? Or do they have an ID??
init([DcId, _StartTimestamp]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    NumPartitions = riak_core_ring:num_partitions(Ring),
    %%SentTimes = min_dc_sent_dict(NumPartitions, StartTimestamp, dict:new()),
    SentTimes = dict:new(),
    {ok, #state{sent_times=SentTimes,
		num_partitions=NumPartitions,
		dcid=DcId}}.

%% Updates the sent time of a given partition
handle_cast({update_sent_time, Partition, Timestamp}, State=#state{sent_times=LastSent}) ->
    NewSent = dict:store(Partition, Timestamp, LastSent),
    {noreply, State#state{sent_times=NewSent}}.


handle_call({get_max_sent_time}, _From, State=#state{sent_times=LastSent,
						    num_partitions=NumPartitions}) ->
    %% Maybe should use a more efficient data-structure so you don't
    %% have to iterate over an entire list each time
    case dict:size(LastSent) of
	NumPartitions ->
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
	    {reply, Time, State};
	_ ->
	    {reply, 0, State}
    end.


%% Is this the right way to get the partition ids?
%% This is a helper function
%%min_dc_sent_dict(-1,_StartTimestamp,DcSent) ->
%%    DcSent;
%%min_dc_sent_dict(N,StartTimestamp,DcSent) ->
%%    min_dc_sent_dict(N-1, StartTimestamp, dict:store(N, StartTimestamp, DcSent)).


%% FIX ToDo: should I remove these, or add others?
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
-spec get_atom(DcId :: term())
	      -> atom().
get_atom({DcAddr, Port}) ->
    list_to_atom(atom_to_list(?MODULE) ++ atom_to_list(DcAddr) ++ integer_to_list(Port)).
