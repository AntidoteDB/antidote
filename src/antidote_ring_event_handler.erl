%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(antidote_ring_event_handler).
-behaviour(gen_event).

-include("antidote.hrl").
-include_lib("kernel/include/logger.hrl").

-export([update_status/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).
-record(state, {}).

init([]) ->
    update_status(),
    {ok, #state{}}.

handle_event({ring_update, _}, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Members = riak_core_ring:all_members(Ring),
    {Claimant, RingReady, Down, MarkedDown, Changes} = riak_core_status:ring_status(),
    ?LOG_NOTICE("Ring is changing!\nClaimant: ~p\nReady: ~p\nNodes down: ~p\nMarked down: ~p\nChanges: ~p\nClaimed: ~p\nPending: ~p", [
        Claimant, RingReady, Down, MarkedDown, length(Changes),
        [{Node, claim_percent(Ring, Node)} || Node <- Members],
        [{Node, future_claim_percentage(Changes, Ring, Node)} || Node <- Members]
    ]),

%% ring status
    update_status(),
    {ok, State}.

handle_call(_Event, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


update_status() ->
    %% ring status
    {_Claimant, RingReady, Down, MarkedDown, Changes} = riak_core_status:ring_status(),
    ?STATS({ring_ready, RingReady}),


    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Members = riak_core_ring:all_members(Ring),

    %% node availability and ring state for every member as seen by this node
    lists:foreach(fun(Node) ->
        %% member status
        NodeState = riak_core_ring:member_status(Ring, Node),
        ?STATS({node_state, Node, NodeState}),

        %% ring claimed
        RingClaimed = claim_percent(Ring, Node),
        ?STATS({ring_claimed, Node, RingClaimed}),

        %% ring pending
        RingPending = future_claim_percentage(Changes, Ring, Node),
        ?STATS({ring_pending, Node, RingPending}),

        ?STATS({ring_member_availability, Node, node_availability(Node, Down, MarkedDown)})
                  end, Members),

    ok.


claim_percent(Ring, Node) ->
    RingSize = riak_core_ring:num_partitions(Ring),
    Indices = riak_core_ring:indices(Ring, Node),
    length(Indices) * 100 / RingSize.

future_claim_percentage([], _Ring, _Node) ->
    0.0;
future_claim_percentage(_Changes, Ring, Node) ->
    FutureRingSize = riak_core_ring:future_num_partitions(Ring),
    NextIndices = riak_core_ring:future_indices(Ring, Node),
    length(NextIndices) * 100 / FutureRingSize.


node_availability(Node, Down, MarkedDown) ->
    case {lists:member(Node, Down), lists:member(Node, MarkedDown)} of
        {false, false} -> 1;
        {true,  true } -> -1;
        {true,  false} -> -2;
        {false, true } -> 2
    end.
