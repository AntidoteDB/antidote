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

%% This vnode receives all transactions happening at remote DCs,
%% in commit order for each DC and with no missing operations
%% (ensured by interDC). The goal of this module is to ensure
%% that transactions are committed when their causal dependencies
%% are satisfied.

-module(inter_dc_dep_vnode).
-behaviour(riak_core_vnode).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([
    handle_transaction/1]).

%% VNode methods
-export([
    init/1,
    start_vnode/1,
    handle_command/3,
    handle_coverage/4,
    handle_exit/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1]).

%% VNode state
-record(state, {
    partition :: partition_id(),
    last_updated :: non_neg_integer()
}).

%%%% API --------------------------------------------------------------------+

%% Passes the received transaction to the dependency buffer.
%% At this point no message can be lost (the transport layer must ensure all transactions are delivered reliably).
-spec handle_transaction(#interdc_txn{}) -> ok.
handle_transaction(Txn = #interdc_txn{partition = P}) ->
    dc_utilities:call_vnode_sync(P, inter_dc_dep_vnode_master, {txn, Txn}).

%%%% VNode methods ----------------------------------------------------------+

-spec init([partition_id()]) -> {ok, #state{}}.
init([Partition]) ->
    {ok, #state{partition = Partition, last_updated = 0}}.

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% Store the normal transaction
try_store(#interdc_txn{partition = Partition, operations = Updates, timestamp = TxCommitTime}) ->
    %% The transactions are delivered reliably and in order, so the entry for originating DC is irrelevant.
    %% Therefore, we remove it prior to further checks.
    %% Put the operations in the log
%%    Payloads = [Op#operation.payload || Op <- Ops],
%%    {ok, _} = logging_vnode:append_group(dc_utilities:partition_to_indexnode(Partition), [Partition], Payloads, false),
    LogRecord = #log_record{op_type = commit,
        op_payload = {TxCommitTime, Updates}},
    {ok, _} = logging_vnode:append_remote(dc_utilities:partition_to_indexnode(Partition), [Partition], LogRecord, false),
    ok = lists:foreach(fun(Op) ->
        %lager:info("updates are ~p", [Updates]),
        {Key, Type, Params} = Op,
        DownstreamOp = #ec_payload{key = Key, type = Type, op_param = Params, commit_time = TxCommitTime},
        ec_materializer_vnode:update(Key, DownstreamOp) end, Updates).

handle_command({txn, Txn}, _Sender, State) ->
%% Put the operations in the log
    {reply, try_store(Txn), State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
handoff_starting(_TargetNode, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_TargetNode, State) -> {ok, State}.
handle_handoff_command(_Message, _Sender, State) -> {noreply, State}.
handle_handoff_data(_Data, State) -> {reply, ok, State}.
encode_handoff_item(_ObjectName, _ObjectValue) -> <<>>.
is_empty(State) -> {true, State}.
terminate(_Reason, _ModState) -> ok.
delete(State) -> {ok, State}.
