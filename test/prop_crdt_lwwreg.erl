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

-module(prop_crdt_lwwreg).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").

%% API
-export([prop_counter_spec/0]).


prop_counter_spec() ->
 crdt_properties:crdt_satisfies_spec(antidote_crdt_lwwreg, fun op/0, fun spec/1).


spec(Operations) ->
  case Operations of
    [] ->
      % initial value is empty binary
      <<>>;
    _ ->
      % times are corrected, so that old values are always overridden
      Operations2 = [correctTime(Operations, Op) || Op <- Operations],
      % select the value based on the assign with maximum timestamp (or max. value if timestamps are equal)
       {_MaxTime, MaxVal} = lists:max([{Time, Val} || {_, {assign, Val, Time}} <- Operations2]),
       MaxVal
  end.

correctTime(Operations, {OperationVc, {assign, Value, Timestamp}}) ->
  Before = [{Vc,Op} || {Vc,Op} <- Operations, Vc =/= OperationVc, crdt_properties:clock_le(Vc, OperationVc)],
  Before2 = [correctTime(Before, Op) || Op <- Before],
  BeforeTimestampMax = lists:max([0] ++ [T || {_, {assign, _, T}} <- Before2]),
  {OperationVc, {assign, Value, max(Timestamp, BeforeTimestampMax+1) }}.


% generates a random counter operation
op() ->
  {assign, oneof([a,b,c,d,e,f,g,h,i]), non_neg_integer()}.

