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

-export([update_func_min/2,
	 get_max_time/1,
	 export_funcs_and_vals/0]).

%% These functions are input to create a meta_data_sender
%% The functions merge by taking the minimum of all entries per node per DC
export_funcs_and_vals() ->
    [safe, fun update_func_min/2, fun get_min_time/1, dict:new(), dict:new()].

update_func_min(Last,Time) ->
    case Last of
	undefined ->
	    true;
	_ ->
	    Time >= Last
    end.

%% This assumes the dicts being sent have all DCs
get_max_time(Dict) ->
    dict:fold(fun(_NodeId, NodeDict, Acc1) ->
		      case NodeDict of
			  undefined ->
			      Acc1;
			  _ ->
			      dict:fold(fun(DcId, Time, Acc2) ->
						PrevTime = case dict:find(DcId, Acc2)
							       {ok, Val} ->
								   Val;
							       error ->
								   Time
							   end,
						case Time >= PrevTime of
						    true ->
							dict:store(DcId, Time, Acc2);
						    false ->
							Acc2
						end
					end, Acc1, NodeDict)
		      end
	      end, dict:new(), Dict).
