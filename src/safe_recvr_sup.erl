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
-module(safe_recvr_sup).
-behavior(supervisor).

-export([start_link/2,start_fsm/1]).
-export([init/1]).


start_link(DcId, StartTimestamp) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [DcId, StartTimestamp]).


start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

init([DcList]) ->
     Workers = lists:foldl(fun(DcId, Acc) ->
				  Acc ++ [{{safe_recvr_fsm,DcId},
					   {safe_recvr_fsm, start_link, [DcId]},
					   permanent, 1000, worker, [safe_recvr_fsm]}]
			  end, [], DcList),
    {ok, {{one_for_one, 5, 10}, Workers}}.


