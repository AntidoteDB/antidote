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
-module(ec_readitem_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([return/2]).

%% Spawn

-record(state, {type,
                key,
                tx_id,
                tx_coordinator,
                vnode,
                updates}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Vnode, Coordinator, TxId, Key, Type, Updates) ->
    gen_fsm:start_link(?MODULE, [Vnode, Coordinator,
                                 TxId, Key, Type, Updates], []).


%%%===================================================================
%%% States
%%%===================================================================

init([Vnode, Coordinator, TxId, Key, Type, Updates]) ->
    SD = #state{vnode=Vnode,
                type=Type,
                key=Key,
                tx_coordinator=Coordinator,
                tx_id=TxId,
                updates=Updates},
    {ok, return, SD, 0}.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(timeout, SD0=#state{key=Key,
                           tx_coordinator=Coordinator,
                           tx_id=TxId,
                           type=Type,
                           vnode=IndexNode,
                           updates=Updates}) ->
    case materializer_vnode:read(Key, Type, TxId) of
        {ok, Snapshot} ->
            case generate_downstream_operations(Updates, TxId, IndexNode, Key, []) of
                {ok, Updates2} ->
                    Snapshot2=ec_materializer:materialize_eager(Type, Snapshot, Updates2),
                    Reply = {ok, Snapshot2};
                {error, Reason} ->
                    Reply={error, Reason}
            end;
        {error, Reason} ->
            Reply={error, Reason}
    end,
    riak_core_vnode:reply(Coordinator, Reply),
    {stop, normal, SD0};
return(_SomeMessage, SDO) ->
    {next_state, return, SDO,0}.

handle_info(_Info, StateName, StateData) ->
    {next_state,StateName,StateData,1}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Internal functions

generate_downstream_operations([], _Txn, _IndexNode, _Key, DownOps) ->
    {ok, DownOps};

generate_downstream_operations([{Type, Param}|Rest], Txn, IndexNode, Key, DownOps0) ->
    case ec_downstream:generate_downstream_op(Txn, IndexNode, Key, Type, Param, DownOps0) of
        {ok, DownstreamRecord} ->
            generate_downstream_operations(Rest, Txn, IndexNode, Key, DownOps0 ++ [DownstreamRecord]);
        {error, Reason} ->
            {error, Reason}
    end.
