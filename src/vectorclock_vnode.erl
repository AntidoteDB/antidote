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

-module(vectorclock_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
  get_stable_snapshot/0,
  recalculate_stable_snapshot/1,
  update_partition_clock/2]).

%% Vnode methods
-export([
  init/1,
  start_vnode/1,
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
  handle_exit/3,
  handle_overload_command/3,
  handle_overload_info/2
]).

-ignore_xref([start_vnode/1]).

-define(META_PREFIX, {partition, vectorclock}).
-define(META_PREFIX_SS, {partition_ss, vectorclock}).

%% Vnode state
-record(state, {
  vectorclock :: vectorclock:vectorclock(),
  partition :: partition_id()
}).

%%%% API --------------------------------------------------------------------+

get_stable_snapshot() -> riak_core_metadata:get(?META_PREFIX_SS, 1, [{default, dict:new()}]).

recalculate_stable_snapshot(Partition) ->
  dc_utilities:call_vnode(Partition, vectorclock_vnode_master, calculate_stable_snapshot).

update_partition_clock(Partition, NewClock) ->
  dc_utilities:call_vnode(Partition, vectorclock_vnode_master, {update_clock, NewClock}).


%%%% VNode methods ----------------------------------------------------------+

start_vnode(I) ->
  riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Initialize the clock
init([Partition]) ->
  NewPClock = dict:new(),
  metadata_maybe_put(?META_PREFIX, Partition, NewPClock),
  {ok, #state{
    vectorclock = NewPClock,
    partition = Partition
  }}.

%% @doc : calculate stable snapshot as the minimum of all partition vectorclocks
handle_command(calculate_stable_snapshot, _Sender, State) ->
  Metadata = metadata_maybe_list(?META_PREFIX),
  %% If metadata does not contain clock of all partitions, do not calculate the stable snapshot
  case dc_utilities:get_partitions_num() == length(Metadata) of
    false -> logger:warning("Metadata misses entries for some partitions, skipping the calculate_stable_snapshot.");
    true ->
      VClocks = lists:foldl(fun({_Key, Value}, AccList) ->
        case is_list(Value) of
          true -> Value ++ AccList;
          false -> [Value] ++ AccList
        end
      end, [], Metadata),
      %% Calculate stable_snapshot from minimum of vectorclock of all partitions
      StableSnapshot = vectorclock:min(VClocks),
      metadata_maybe_put(?META_PREFIX_SS, 1, StableSnapshot)
  end,
  {noreply, State};

handle_command({update_clock, NewClock}, _Sender, State = #state{vectorclock = Current, partition = Partition}) ->
  Max = vectorclock:max([Current, NewClock]),
  metadata_maybe_put(?META_PREFIX, Partition, Max),
  {noreply, State#state{vectorclock = Max}}.

metadata_maybe_put(Prefix, Key, Value) ->
  case catch riak_core_metadata:put(Prefix, Key, Value) of
    {'EXIT', {shutdown, _}} -> logger:warning("Failed to update partition clock: shutting down.");
    Normal -> Normal
  end.

metadata_maybe_list(Prefix) ->
  case catch riak_core_metadata:to_list(Prefix) of
    {'EXIT', Reason} -> logger:warning("Failed to fetch metadata (reason: ~p)", [Reason]), [];
    Normal -> Normal
  end.

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
handle_overload_command(_, _, _) ->
    ok.
handle_overload_info(_, _) ->
    ok.
