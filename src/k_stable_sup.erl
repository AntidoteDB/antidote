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
%%
%% Supervisor for k-stable functions
%% Call path:
%% antidote_sup.erl starts this supervisor
%% this supervisor starts the gen_server
%%
%% -------------------------------------------------------------------
-module(k_stable_sup).

-behavior(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Init) ->
    lager:info("start_link"),
    supervisor:start_link({local, ?MODULE}, ?MODULE, Init).

%% TODO: Something is wrong here
%% {"Kernel pid terminated",application_controller,"{application_start_failure,antidote,{{shutdown,{failed_to_start_child,k_stable_sup,{function_clause,[{supervisor,check_startspec,[{k_stable,{k_stable,start_link,[]},permanent,5000,worker,[k_stable]},[]],[{file,\"supervisor.erl\"},{line,1295}]},{supervisor,init_children,2,[{file,\"supervisor.erl\"},{line,312}]},{gen_server,init_it,6,[{file,\"gen_server.erl\"},{line,328}]},{proc_lib,init_p_do_apply,3,[{file,\"proc_lib.erl\"},{line,247}]}]}}},{antidote_app,start,[normal,[]]}}}"}


init(_Args) ->
    lager:info("init"),
    Worker = {k_stable,
        {k_stable, start_link, []},
        permanent, 5000, worker, [k_stable]},
    {ok, {{one_for_one, 5, 10}, Worker}}.
