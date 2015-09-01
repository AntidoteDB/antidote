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
	 get_stable_snapshot/0,
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
-define(META_PREFIX_SS, {partition_ss,vectorclock}).

-record(currentclock,{last_received_clock :: vectorclock:vectorclock(),
                      partition_vectorclock :: vectorclock:vectorclock(),
                      stable_snapshot :: vectorclock:vectorclock(),
                      partition :: partition_id(),
                      num_p :: non_neg_integer()}).

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
                       partition = Partition,
                       num_p=0}}.

%% @doc get the stable time directly from the locally stored meta-data
%%      instead of sending a reqest to a possibly external serialized server
get_stable_snapshot() ->
    riak_core_metadata:get(?META_PREFIX_SS,1,[{default,dict:new()}]).

get_clock_of_dc(VClock, Dc) ->
  {ok, Clock} = vectorclock:get_clock_of_dc(Dc, VClock),
  Clock.

min_dc_entry(VClocks, Dc) -> min_dc_entry(VClocks, Dc, get_clock_of_dc(hd(VClocks), Dc)).
min_dc_entry([], _, Min) -> Min;
min_dc_entry([VClock|Tail], Dc, Min) -> min_dc_entry(Tail, Dc, min(Min, get_clock_of_dc(VClock, Dc))).

find_min_of(VClocks, DcKeys) ->
  F = fun(Dc) -> {Dc, min_dc_entry(VClocks, Dc)} end,
  vectorclock:from_list(lists:map(F, DcKeys)).

%% @doc
handle_command(get_clock, _Sender,
               #currentclock{partition_vectorclock=Clock} = State) ->
    {reply, {ok, Clock}, State};

handle_command(get_stable_snapshot, _Sender,
               State=#currentclock{stable_snapshot=Clock}) ->
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
                %% Some metadata values can be lists of size > 1, so we flatten the list now
                AllVClocks = riak_core_metadata:fold(fun({_Key, V}, A) -> V ++ A end, [Clock], ?META_PREFIX),
                %% Then we calculate the min vclock for specified DCID keys
                find_min_of(AllVClocks, vectorclock:dcids(Clock));
            false ->
                dict:new()
        end,
    try
	%% Instead of just storing the time in the state data, also store it in riak core
	%% meta-data.  The reason for this is so that when starting a new transaction, the
	%% corrdinator can read direclty from the nodes locally stored meta-data, instead of sending
	%% a request to a serialized vnode.
	%%
	%% Not sure this is the best way to store data in riak core meta_data because everyone will be putting to the same key
	%% which might cause conlifcts, but they are resolved anyway on read.
	riak_core_metadata:put(?META_PREFIX_SS, 1, Stable_snapshot)
    catch
	_:Reason ->
	    lager:error("Exception caught ~p! ",[Reason])
    end,
    {noreply, State#currentclock{
      stable_snapshot=Stable_snapshot, num_p = NumPartitions}};

%% @doc This function implements following code
%% if last_received_vectorclock[partition][dc] < time
%%   vectorclock[partition][dc] = last_received_vectorclock[partition][dc]
%% last_received_vectorclock[partition][dc] = time
handle_command({update_clock, DcId, Timestamp}, _Sender,
               #currentclock{last_received_clock=LastClock,
                             partition_vectorclock=VClock,
                             partition=Partition
                            } = State) ->
    case dict:find(DcId, LastClock) of
        {ok, LClock} ->
            case LClock < Timestamp of
                true ->
                    NewLClock = dict:store(DcId, Timestamp, LastClock),
                    NewPClock = dict:store(DcId, Timestamp - 1, VClock),
                    %% Broadcast new pvv to other partition
                    try
                        riak_core_metadata:put(?META_PREFIX, Partition, NewPClock),
                        {reply, ok,
                         State#currentclock{last_received_clock=NewLClock,
                                        partition_vectorclock=NewPClock}
                        }
                    catch
                        _:Reason ->
                            lager:error("Exception caught ~p! ",[Reason]),
                            {reply, ok, State}
                    end;
                false ->
                    {reply, ok, State}
            end;
        error ->
            NewLClock = dict:store(DcId, Timestamp, LastClock),
            NewPClock = dict:store(DcId, Timestamp - 1, VClock),
            try
                riak_core_metadata:put(?META_PREFIX, Partition, NewPClock),
                {reply, ok,
                 State#currentclock{last_received_clock=NewLClock,
                                    partition_vectorclock=NewPClock}
                }
            catch
                _:Reason ->
                    lager:error("Exception caught ~p! ",[Reason]),
                    {reply, ok, State}
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
