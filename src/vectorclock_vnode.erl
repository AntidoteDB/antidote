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
-module(vectorclock_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
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

-define(META_PREFIX, {partition,vectorclock}).

-define(META_PREFIX_DC, {dcid,partition,vectorclock}).

-define(META_PREFIX_SAFE, {dcid,vectorclock}).

-record(currentclock,{last_received_clock :: vectorclock:vectorclock(),
                      partition_vectorclock :: vectorclock:vectorclock(),
                      stable_snapshot :: vectorclock:vectorclock(),
		      safe_clock :: vectorclock:vectorclock(),
		      sent_clock :: dict(),
		      safe_to_ack :: vectorclock:vectorclock(),
                      partition,
                      num_p}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Initialize the clock
init([Partition]) ->
    NewPClock = dict:new(),
    riak_core_metadata:put(?META_PREFIX, Partition, NewPClock),
    {ok, #currentclock{last_received_clock=dict:new(),
                       partition_vectorclock=NewPClock,
                       stable_snapshot = dict:new(),
		       safe_clock = dict:new(),
		       sent_clock = dict:new(),
		       safe_to_ack = dict:new(),
                       partition = Partition,
                       num_p=0}}.

%% @doc returns physical clock?
handle_command(get_clock, _Sender,
               #currentclock{partition_vectorclock=Clock} = State) ->
    {reply, {ok, Clock}, State};

% old remove
handle_command(get_stable_snapshot, _Sender,
               State=#currentclock{stable_snapshot=Clock}) ->
    {reply, {ok, Clock}, State};

% returns safel time
%% This is only called during the external read,
%% should maybe advance the safe_clock by here
%% unless meta-data is used
handle_command(get_safe_time, _Sender,
	       State=#currentclock{safe_clock=Clock}) ->
    {reply, {ok, Clock}, State};



%% @doc : calculate stable snapshot from min of vectorclock (each entry)
%% from all partitions
handle_command(calculate_stable_snapshot, _Sender,
               State=#currentclock{partition_vectorclock = Clock,
                                   num_p=NumP}) ->
    %% Calculate stable_snapshot from minimum of vectorclock of all partitions
    NumPartitions = case NumP of
                        0 ->
                            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                            riak_core_ring:num_partitions(Ring);
			_ -> NumP
		    end,
    NumMetadata = length(riak_core_metadata:to_list(?META_PREFIX)),
    Stable_snapshot =
        %% If metadata doesnot contain clock of all partitions
        %% donot calculate stable snapshot
        case NumPartitions == NumMetadata of
            true ->
                riak_core_metadata:fold(
                  fun({_Key, V}, A) ->
                          find_min(V,{Clock,A})
                  end,
                  Clock, ?META_PREFIX);
            false ->
                dict:new()
        end,
    {reply, {ok, Stable_snapshot},
     State#currentclock{stable_snapshot=Stable_snapshot,
                        num_p = NumPartitions}};


%% This update happens when a external DC sends an time saying
%% all updates up to this time have been sent to this DC
handle_command({update_safe_clock, IsLocal, DcId, Timestamp}, _Sender, 
	       #currentclock{safe_clock=SafeClock
			     } = State) ->
    case dict:find(DcId,SafeClock) of
	{ok,DcSafeClock} ->
	    case DcSafeClock < Timestamp of
		true ->
		    NewSafeClock = dict:store(DcId,Timestamp,SafeClock),
		    case IsLocal of
			false ->
			    try
				riak_core_metadata:put(?META_PREFIX_SAFE,DcId,Timestamp),
				{reply, {ok, NewSafeClock},State#currentclock{safe_clock=NewSafeClock}}
			    catch
				_:Reason ->
				    lager:error("Exception caught ~p", [Reason]),
				    {reply,{ok,NewSafeClock},State#currentclock{safe_clock=NewSafeClock}}
			    end;
			true ->
			    {reply, {ok, NewSafeClock}, State#currentclock{safe_clock=NewSafeClock}}
		    end;
		false ->
		    {reply, {ok, SafeClock}, State}
	    end;
	error ->
	    NewSafeClock = dict:store(DcId,Timestamp,SafeClock),
	    try
		riak_core_metadata:put(?META_PREFIX_SAFE,DcId,Timestamp),
		{reply,{ok,NewSafeClock},State#currentclock{safe_clock=NewSafeClock}}
	    catch
		_:Reason ->
		    lager:error("Exception caught ~p", [Reason]),
		    {reply,{ok,NewSafeClock},State#currentclock{safe_clock=NewSafeClock}}
	    end
    end;




handle_command({update_safe_vector_local, Vector}, _Sender,
	       #currentclock{safe_clock=SafeClock
			     } = State) ->
    NewSafeClock = dict:fold(fun(DcId,Timestamp,NewDict) ->
				     case dict:find(DcId, NewDict) of
					 true ->
					     dict:update(DcId, fun(OldTimestamp) ->
								       case OldTimestamp < Timestamp of
									   true ->
									       Timestamp;
									   _ -> 
									       OldTimestamp
								       end
							       end, NewDict);
					 false -> 
					     dict:store(DcId, Timestamp, NewDict)
				     end
			     end, SafeClock, Vector),
    {reply,{ok,NewSafeClock},State#currentclock{safe_clock=NewSafeClock}};

		    
%% Don't do this anymore, is expensive and unecessary,
%% instead a single node is used to keep track of this
%% The sent clock is a value circulated around the local DC
%% Keeps track of the number of updates a local partition has sent
%% to each remote DC
handle_command({update_sent_clock, DcId, SendPartition, Timestamp}, _Sender,
	       #currentclock{sent_clock=SentClock
			    } = State) ->
    
    case dict:find({DcId,SendPartition},SentClock) of
	{ok,PartitionSentClock} ->
	    case PartitionSentClock < Timestamp of
		true ->
		    NewSentClock = dict:store({DcId,SendPartition},Timestamp,SentClock),
		    try
			riak_core_metadata:put(?META_PREFIX_DC,{SendPartition,DcId},Timestamp),
			{reply, {ok, NewSentClock},State#currentclock{sent_clock=NewSentClock}}
		    catch
			_:Reason ->
			    lager:error("Exception caught ~p",[Reason]),
			    {reply,{ok,NewSentClock},State}
		    end;
		false ->
		    {reply, {ok, SentClock}, State}
	    end;
	error ->
	    NewSentClock = dict:store({DcId,SendPartition},Timestamp,SentClock),
	    try
		riak_core_metadata:put(?META_PREFIX_DC, {SendPartition,DcId},Timestamp),
		{reply,{ok,NewSentClock},State#currentclock{sent_clock=NewSentClock}}
	    catch
		_:Reason ->
		    lager:error("Exception caught ~p!", [Reason]), 
		    {reply,{ok,NewSentClock},State}
	    end
    end;



%% This is an old way of updating sent clock, keeping for now for reference
%% in case want to change back
handle_command({update_sent_clock2, DcId, SendPartition, Timestamp}, _Sender,
	       #currentclock{sent_clock=SentClock,
			     partition=Partition
			    } = State) ->

    case dict:find(SendPartition, SentClock) of
	{ok, PartitionSentClock} ->
	    case dict:find(DcId, PartitionSentClock) < Timestamp of
		true -> 
		    NewPartitionSentClock = dict:store(DcId, Timestamp, vectorclock:vectorclock()),
		    NewSentClock = dict:store(SendPartition, NewPartitionSentClock, SentClock),
		    try
			riak_core_metadata:put(?META_PREFIX_DC, {Partition,DcId}, NewPartitionSentClock),
			{reply, {ok, NewPartitionSentClock},
			 State#currentclock{sent_clock=NewSentClock}
			}
		    catch
			_:Reason ->
			    lager:error("Exception caught ~p!",[Reason]),
			    {reply, {ok, SentClock},State}
		    end;
		false ->
		    {reply, {ok, SentClock}, State}
	    end;
	error ->
	    NewPartitionSentClock = dict:store(DcId, Timestamp, vectorclock:vectorclock()),
	    NewSentClock = dict:store(SendPartition, NewPartitionSentClock, SentClock),
	    try
		riak_core_metadata:put(?META_PREFIX, Partition, NewSentClock),
		{reply, {ok, NewSentClock},
		 State#currentclock{sent_clock=NewSentClock}
		}
	    catch
		_:Reason ->
		    lager:error("Exception caught ~p!", [Reason]),
		    {reply, {ok, State#currentclock{sent_clock=NewSentClock}}, State}
	    end
    end;






%% @doc This function implements following code
%% if last_received_vectorclock[partition][dc] < time
%%   vectorclock[partition][dc] = last_received_vectorclock[partition][dc]
%% last_received_vectorclock[partition][dc] = time
handle_command({update_clock, DcId, Timestamp}, _Sender,
               #currentclock{last_received_clock=LastClock,
                             safe_clock=VClock,
                             partition=Partition
                            } = State) ->
    case dict:find(DcId, LastClock) of
        {ok, LClock} ->
            case LClock < Timestamp of
                true ->
                    NewLClock = dict:store(DcId, Timestamp, LastClock),
                    NewPClock = dict:store(DcId, Timestamp-1, VClock),
                    %% Broadcast new pvv to other partition
                    try
                        riak_core_metadata:put(?META_PREFIX, Partition, NewPClock),
                        {reply, {ok, NewPClock},
                         State#currentclock{last_received_clock=NewLClock,
                                        safe_clock=NewPClock}
                        }
                    catch
                        _:Reason ->
                            lager:error("Exception caught ~p! ",[Reason]),
                            {reply, {ok, VClock},State}
                    end;
                false ->
                    {reply, {ok, VClock}, State}
            end;
        error ->
            NewLClock = dict:store(DcId, Timestamp, LastClock),
            NewPClock = dict:store(DcId, Timestamp - 1, VClock),
            try
                riak_core_metadata:put(?META_PREFIX, Partition, NewPClock),
                {reply, {ok, NewPClock},
                 State#currentclock{last_received_clock=NewLClock,
                                    safe_clock=NewPClock}
                }
            catch
                _:Reason ->
                    lager:error("Exception caught ~p! ",[Reason]),
                    {reply, {ok, VClock},State}
            end
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

find_min([VClock], {PVV, StableClock}) ->
    dict:fold(fun(Dc, _, Snapshot) ->
                      {ok, Clock1} = vectorclock:get_clock_of_dc(Dc, VClock),
                      {ok, Clock2} = vectorclock:get_clock_of_dc(Dc, Snapshot),
                      dict:store(Dc, min(Clock1, Clock2), Snapshot)
               end,
               StableClock,
               PVV).
