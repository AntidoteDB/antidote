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
%% @doc Supervise the fsm.
-module(meta_data_sender_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/1]).
-export([init/1]).


start_link(FunsAndInitStateList) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, FunsAndInitStateList).


start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).


init(FunsAndInitStateList) ->
    Workers = lists:map(fun(FunsAndInitState) ->
				[Name | _] = FunsAndInitState,
				{Name,
				 {meta_data_sender, start_link, FunsAndInitState},
				 transient, 5000, worker, [meta_data_sender]}
			end, FunsAndInitStateList),
    {ok, {{one_for_one, 5, 10}, Workers}}.
