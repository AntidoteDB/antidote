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

-module(eiger_checkdeps_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([gather/2,
         reply/2]).

-record(state, {
          deps_ack,
          from,
          total_deps
          }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Deps) ->
    gen_fsm:start_link(?MODULE, [From, Deps], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ListDeps]) ->
    DepsPartition = lists:foldl(fun(Dependency, Dict)->
                                    {Key, _TimeStamp} = Dependency,
                                    Preflist = log_utilities:get_preflist_from_key(Key),
                                    IndexNode = hd(Preflist),
                                    dict:append(IndexNode, Dependency, Dict)
                                end, dict:new(), ListDeps),
    lists:foreach(fun({Partition, Slice}) ->
                    eiger_vnode:check_deps(Partition, Slice)
                  end, dict:to_list(DepsPartition)),
                                    
    SD = #state{    
            from=From,
            deps_ack=0,
            total_deps=length(dict:fetch_keys(DepsPartition))
           },
    {ok, gather, SD}.

gather(timeout, SD0) ->
    {next_state, gather, SD0};

gather(deps_checked, SD0=#state{deps_ack=DepsAck0, total_deps=NPDeps}) ->
    DepsAck1 = DepsAck0 + 1,
        case DepsAck1 of
            NPDeps ->
                {next_state, reply, SD0#state{deps_ack=DepsAck1}, 0};
        _ ->
                {next_state, gather, SD0#state{deps_ack=DepsAck1}, 0}
    end.

reply(timeout, S0=#state{from=From}) ->
    From ! ok,
    {stop, normal, S0}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
