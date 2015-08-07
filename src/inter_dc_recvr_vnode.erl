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

%% @doc This vnode is responsible for receiving updates from remote DCs and 
%% applying to local partition in causal order

-module(inter_dc_recvr_vnode).
-behaviour(riak_core_vnode).
-include("inter_dc_repl.hrl").
-include("antidote.hrl").

-export([start_vnode/1,
         %%API begin
         store_updates/1,
         %%API end
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
         handle_exit/3]).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% public API

%% @doc store_updates: sends the updates from remote DC to corresponding
%%  partition's vnode. Input is a list of transactions from remote DC.
-spec store_updates(Transactions::[clocksi_transaction_reader:transaction()])
                   -> ok.
store_updates(Transactions) ->
    Transaction = hd(Transactions),
    {_Txid,_Commitime,_ST,Ops} = Transaction,
    Operation = hd(Ops),
    Logrecord = Operation#operation.payload,
    Payload = Logrecord#log_record.op_payload,
    Op_type = Logrecord#log_record.op_type,
    case Op_type of
        noop ->
            Key = Payload,
            Node = log_utilities:get_my_node(Key),
            Preflist = [{Key, Node}];
        _ ->
            {Key,_Type,_Op} = Payload,
            Preflist = log_utilities:get_preflist_from_key(Key)
    end,
    Indexnode = hd(Preflist),
    lists:foreach(fun(Txn) ->
                          store_update(Indexnode, Txn)
                  end, Transactions),
    riak_core_vnode_master:command(Indexnode, {process_queue},
                                   inter_dc_recvr_vnode_master),
    ok.

store_update(Node, Transaction) ->
    riak_core_vnode_master:sync_command(Node,
                                        {store_update, Transaction},
                                        inter_dc_recvr_vnode_master).

%% riak_core_vnode call backs
init([Partition]) ->
    StateFile = string:concat(integer_to_list(Partition), "replstate"),
    Path = filename:join(
             app_helper:get_env(riak_core, platform_data_dir), StateFile),
    case dets:open_file(StateFile, [{file, Path}, {type, set}]) of
        {ok, StateStore} ->
            case dets:lookup(StateStore, recvr_state) of
                %%If file already exists read previous state from it.
                [{recvr_state, State}] ->
                    {ok, State};
                [] ->
                    {ok, State } = inter_dc_repl_update:init_state(Partition),
                    {ok, State#recvr_state{statestore = StateStore}};
                Error -> Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% process one replication request from other Dc. Update is put in a queue for each DC.
%% Updates are expected to recieve in causal order.
handle_command({store_update, Transaction}, _Sender, State) ->
    {ok, NewState} = inter_dc_repl_update:enqueue_update(
                       Transaction, State),
    %% Dets shouldnt be used
    %% ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {reply, ok, NewState};

handle_command({process_queue}, _Sender, State) ->
    {ok, NewState} = inter_dc_repl_update:process_queue(State),
    %% Dets shouldn't be used
    %% ok = dets:insert(State#recvr_state.statestore, {recvr_state, NewState}),
    {noreply, NewState}.

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

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State = #recvr_state{partition=Partition}) ->
    meta_data_sender:remove_partition(Partition),
    ok.
