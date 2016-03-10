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

-module(safe_time_functions).

-include("antidote.hrl").

-export([update_func/2,
	 merge_entries/1,
	 export_funcs_and_vals/1]).

%% These functions are input to create a meta_data_sender
%% The functions merge by taking the minimum of all entries per node per DC
export_funcs_and_vals(Name) ->
    [Name, fun update_func/2, fun merge_entries/1, dict:new(), dict:new()].

%% Update fun is called on each entry of the dict after merging
%% Here just always take the new dict
update_func(Last,Time) ->
    case Last of
	undefined ->
	    true;
	_ ->
	    true
    end.

%% This assumes the dicts being sent have all DCs
merge_entries(Dict) ->
    %% This has an entry per partition, just keep everything
    Dict.

