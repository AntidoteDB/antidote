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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module with functions concerning Conflict
%%%      Resolution Policies (CRPs) on tables.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(table_crps).

-define(CRP(TableLevel, DepLevel, PDepLevel), {TableLevel, DepLevel, PDepLevel}).
-define(ADD_WINS, add).
-define(REMOVE_WINS, remove).
-define(NO_CONCURRENCY, noconcurrency).

%% API
-export([get_rule/1, table_level/1, dep_level/1, p_dep_level/1]).

get_rule(?CRP(TableLevel, DepLevel, PDepLevel)) ->
    Rule1 = rule_table_level(TableLevel),
    Rule2 = rule_dep_level(DepLevel, Rule1),
    rule_p_dep_level(PDepLevel, Rule2);
get_rule(Table) ->
    get_rule(table_utils:policy(Table)).

table_level(?CRP(TableLevel, _, _)) -> TableLevel.

dep_level(?CRP(_, DepLevel, _)) -> DepLevel.

p_dep_level(?CRP(_, _, PDepLevel)) -> PDepLevel.

%% ===================================================================
%% Internal functions
%% ===================================================================

rule_table_level(?ADD_WINS) -> [d, i];
rule_table_level(?REMOVE_WINS) -> [i, d];
rule_table_level(Crp) -> rule_table_level(table_level(Crp)).

rule_dep_level(undefined, Rule) -> Rule;
rule_dep_level(?ADD_WINS, Rule) -> Rule;
rule_dep_level(?REMOVE_WINS, Rule) -> Rule;
rule_dep_level(?NO_CONCURRENCY, Rule) -> Rule;
rule_dep_level(Crp, Rule) -> rule_dep_level(dep_level(Crp), Rule).

rule_p_dep_level(undefined, Rule) -> Rule;
rule_p_dep_level(?ADD_WINS, Rule) -> lists:append(Rule, [t]);
rule_p_dep_level(?REMOVE_WINS, Rule) -> Rule;
rule_p_dep_level(?NO_CONCURRENCY, Rule) -> Rule;
rule_p_dep_level(Crp, Rule) -> rule_p_dep_level(p_dep_level(Crp), Rule).
