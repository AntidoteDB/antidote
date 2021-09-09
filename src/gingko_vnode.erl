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

-module(gingko_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").

-export([start_vnode/1,
  init/1,
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
  handle_overload_info/2]).

-export([ping/0]).

-ignore_xref([start_vnode/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_tx: the prepared txn for each key. Note that for
%%              each key, there can be at most one prepared txn in any
%%              time.
%%          committed_tx: the transaction id of the last committed
%%              transaction for each key.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: partition_id(), gingko::pid()}).

%%%===================================================================
%%% External API
%%%===================================================================
ping() ->
    send_to_one(testkey, {hello}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
  riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%%
%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
   Gingko = gingko:start(Partition),
    logger:notice("Gingko Started at:: ~p",[Gingko]),
  {ok, #state{partition = Partition, gingko = Gingko}}.

handle_command({hello}, _Sender, State = #state{partition = Partition}) ->
  {reply, {hello, Partition}, State};

handle_command(_Message, _Sender, State) ->
  {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
  {noreply, State}.

handoff_starting(_TargetNode, State) ->
  {true, State}.

handoff_cancelled(State) ->
  {ok, State}.

handoff_finished(_TargetNode, State) ->
  {ok, State}.

handle_handoff_data(_Data, State) ->
  {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
  term_to_binary({StatName, Val}).

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



send_to_one(Key, Cmd) ->
    DocIdx = riak_core_util:chash_key({default_bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, gingko),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode,
        Cmd,
        gingko_vnode_master).
