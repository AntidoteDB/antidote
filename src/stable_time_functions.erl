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

-module(stable_time_functions).

-include("antidote.hrl").

-export([update_func_min/2,
         get_min_time/1,
         export_funcs_and_vals/0]).

%% These functions are input to create a meta_data_sender
%% The functions merge by taking the minimum of all entries per node per DC
export_funcs_and_vals() ->
    [stable, fun update_func_min/2, fun get_min_time/1, dict:new(), dict:new()].

update_func_min(Last, Time) ->
    case Last of
        undefined ->
            true;
        _ ->
            Time >= Last
    end.

%% This assumes the dicts being sent have all DCs
get_min_time(Dict) ->
    {MinDict, FoundUndefined} =
        dict:fold(fun(NodeId, NodeDict, {Acc1, Undefined}) ->
                      case NodeDict of
                          undefined ->
                              logger:debug("missing a time for node ~p", [NodeId]),
                              {Acc1, true};
                          _ ->
                          RetDict =
                              dict:fold(fun(DcId, Time, Acc2) ->
                                            PrevTime = case dict:find(DcId, Acc2) of
                                                           {ok, Val} ->
                                                               Val;
                                                           error ->
                                                               Time
                                                       end,
                                            case PrevTime >= Time of
                                                true ->
                                                    dict:store(DcId, Time, Acc2);
                                                false ->
                                                    dict:store(DcId, PrevTime, Acc2)
                                            end
                                        end, Acc1, NodeDict),
                          {RetDict, Undefined}
                      end
                  end, {dict:new(), false}, Dict),
    %% This means we didn't get updated from all nodes/paritions so 0 is the stable time
    case FoundUndefined of
        true ->
            dict:fold(fun(NodeId, _Val, Acc) ->
                          dict:store(NodeId, 0, Acc)
                      end, dict:new(), MinDict);
        false ->
            MinDict
    end.
