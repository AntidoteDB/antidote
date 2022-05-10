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

-module(crdt_properties).

-define(PROPER_NO_TRANS, true).
-include_lib("proper/include/proper.hrl").



-export([
  crdt_satisfies_spec/3,
  crdt_satisfies_partial_spec/3,
  spec_to_partial/1,
  clock_le/2,
  subcontext/2,
  filter_resets/1,
  latest_operations/1
]).

-export_type([clocked_operation/0]).



-type clock() :: #{replica() => non_neg_integer()}.
-type clocked_operation() :: {Clock :: clock(), Operation :: any()}.
-type clocked_effect() :: {Clock :: clock(), Effect :: any()}.
-type replica() :: dc1 | dc2 | dc3.

-record(test_replica_state, {
  state :: any(),
  clock = #{} :: clock(),
  operations = [] :: [clocked_operation()],
  downstreamOps = [] :: [clocked_effect()]
}).

-type test_replica_state() :: #test_replica_state{}.
-type test_state() :: #{replica() => test_replica_state()}.
-type test_operation() :: {pull, replica(), replica()} | {exec, replica(), any()}.


%% this checks whether the implementation satisfies a given CRDT specification
%% Crdt: module name of the CRDT to test
%% OperationGen: proper generator for generating a single random CRDT operation
%% Spec: A function which takes a list of {Clock,Operation} pairs and returns the expected value of the CRDT.
%% The clock can be used to determine the happens-before relation between operations
-spec crdt_satisfies_spec(atom(), fun(() -> proper_types:raw_type()), fun(([clocked_operation()]) -> term())) -> proper:forall_clause().
crdt_satisfies_spec(Crdt, OperationGen, Spec) ->
  ?FORALL(Ops, generateOps(OperationGen),
      checkSpec(Crdt, Ops, Spec)
    ).

%% this checks whether the implementation satisfies a given partial CRDT specification
%% Crdt: module name of the CRDT to test
%% OperationGen: proper generator for generating a single random CRDT operation
%% Spec: A function which takes a list of {Clock,Operation} pairs and the value of the CRDT and checks some relation between them.
-spec crdt_satisfies_partial_spec(atom(), fun(() -> proper_types:raw_type()), fun(([clocked_operation()], term()) -> proper:test())) -> proper:forall_clause().
crdt_satisfies_partial_spec(Crdt, OperationGen, Spec) ->
  ?FORALL(Ops, generateOps(OperationGen),
      checkPartialSpec(Crdt, Ops, Spec)
    ).



% generates a list of operations
generateOps(OpGen) ->
  list(oneof([
    % pulls one operation from the first replica to the second
    {pull, replica(), replica()},
    % execute operation on a given replica
    {exec, replica(), OpGen()}
  ])).

replica() -> oneof([dc1, dc2, dc3]).

clock_le(A, B) ->
  lists:all(fun(R) -> maps:get(R, A) =< maps:get(R, B, 0) end, maps:keys(A)).


subcontext(Clock, Operations) ->
  [{OpClock, Op} || {OpClock, Op} <- Operations, clock_le(OpClock, Clock), not clock_le(Clock, OpClock)].

%% removes all operations which are followed by a reset-operation (including the resets)
filter_resets(Operations) ->
  ResetClocks = [Clock || {Clock, {reset, {}}} <- Operations],
  % consider only operations, that are not invalidated by a reset:
  [{Clock, Op} ||
    % all operations ...
    {Clock, Op} <- Operations,
    % such that no reset comes after the operation
    [] == [ResetClock || ResetClock <- ResetClocks, clock_le(Clock, ResetClock)]].

%% returns only those operations, which are not followed by another operation
%% also removes all reset operations
latest_operations(Operations) ->
  [Op ||
    {Clock, Op} <- Operations,
    Op =/= {reset, {}},
    [] == [C || {C, _} <- Operations, C =/= Clock, clock_le(Clock, C)]].


% executes/checks the specification
checkSpec(Crdt, Ops, Spec) ->
  checkPartialSpec(Crdt, Ops, spec_to_partial(Spec)).

% converts a full specification (returning a CRDT value) to a partial specification
-spec spec_to_partial(fun(([clocked_operation()]) -> term())) -> fun(([clocked_operation()], term()) -> proper:test()).
spec_to_partial(Spec) ->
  fun(Operations, RValue) ->
    SpecValue = Spec(Operations),
    ?WHENFAIL(
      begin
        io:format("Expected value: ~p~n", [SpecValue]),
        io:format("Actual value  : ~p~n", [RValue])
      end,
      SpecValue == RValue
    )
  end.

% executes/checks the specification
checkPartialSpec(Crdt, Ops, Spec) ->
  % check that the CRDT is registered:
  true = antidote_crdt:is_type(Crdt),
  % check that all generated operatiosn are valid:
  _ = [case Crdt:is_operation(Op) of
            true -> true;
            false -> throw({invalid_operation, Op})
          end || {exec, _, Op} <- Ops],

  InitialState = maps:from_list(
    [{Dc, #test_replica_state{state = Crdt:new()}} || Dc <- [dc1, dc2, dc3]]),
  EndState = execSystem(Crdt, Ops, InitialState),
  conjunction(
    [{binary_encoding, checkBinaryEncoding(Crdt, EndState)}] ++
    [{R,
      checkSpecEnd(Crdt, Spec, EndState, R)}
    || R <- maps:keys(EndState)]).

checkSpecEnd(Crdt, Spec, EndState, R) ->
  RState = maps:get(R, EndState),
  RClock = RState#test_replica_state.clock,
  RValue = Crdt:value(RState#test_replica_state.state),

  % get the visible operations:
  VisibleOperations = [{Clock, Op} ||
    Replica <- maps:keys(EndState),
    {Clock, Op} <- (maps:get(Replica, EndState))#test_replica_state.operations,
    clock_le(Clock, RClock)],

  ?WHENFAIL(
    begin
      io:format("Checking value on ~p~n", [R])
    end,
    Spec(VisibleOperations, RValue)
  ).


-spec execSystem(atom(), [test_operation()], test_state()) -> test_state().
execSystem(_Crdt, [], State) ->
  State;
execSystem(Crdt, [{pull, Source, Target}|RemainingOps], State) ->
  TargetState = maps:get(Target, State),
  TargetClock = TargetState#test_replica_state.clock,
  SourceState = maps:get(Source, State),
  % get all downstream operations at the source, which are not yet delivered to the target,
  % and which have all dependencies already delivered at the target
  Effects = [{Clock, Effect} ||
    {Clock, Effect} <- SourceState#test_replica_state.downstreamOps,
    not clock_le(Clock, TargetClock),
    clock_le(Clock#{Source => 0}, TargetClock)
  ],
  NewState =
    case Effects of
      [] -> State;
      [{Clock, Op}|_] ->
        {ok, NewCrdtState} = Crdt:update(Op, TargetState#test_replica_state.state),
        NewTargetState = TargetState#test_replica_state{
          state = NewCrdtState,
          clock = TargetClock#{Source => maps:get(Source, Clock)}
        },
        State#{Target => NewTargetState}
    end,


  execSystem(Crdt, RemainingOps, NewState);
execSystem(Crdt, [{exec, Replica, Op}|RemainingOps], State) ->
  ReplicaState = maps:get(Replica, State),
  CrdtState = ReplicaState#test_replica_state.state,
  CrdtStateForDownstream =
    case Crdt:require_state_downstream(Op) of
      true -> CrdtState;
      false -> no_state
    end,
  {ok, Effect} = Crdt:downstream(Op, CrdtStateForDownstream),
  {ok, NewCrdtState} = Crdt:update(Effect, CrdtState),

  ReplicaClock = ReplicaState#test_replica_state.clock,
  NewReplicaClock = ReplicaClock#{Replica => maps:get(Replica, ReplicaClock, 0) + 1},

  NewReplicaState = ReplicaState#test_replica_state{
    state = NewCrdtState,
    clock = NewReplicaClock,
    operations = ReplicaState#test_replica_state.operations ++ [{NewReplicaClock, Op}],
    downstreamOps = ReplicaState#test_replica_state.downstreamOps ++ [{NewReplicaClock, Effect}]
  },
  NewState = State#{Replica => NewReplicaState},
  execSystem(Crdt, RemainingOps, NewState).



checkBinaryEncoding(Crdt, EndState) ->
  conjunction([{R, checkBinaryEncoding(Crdt, EndState, R)} || R <- maps:keys(EndState)]).

checkBinaryEncoding(Crdt, EndState, R) ->
  RState = maps:get(R, EndState),
  CrdtState = RState#test_replica_state.state,
  BinState = Crdt:to_binary(CrdtState),
  true = is_binary(BinState),
  {ok, CrdtState2} = Crdt:from_binary(BinState),

  conjunction([
    {equal_state, ?WHENFAIL(
      begin
        io:format("CRDT state before: ~p~n", [CrdtState]),
        io:format("CRDT state after: ~p~n", [CrdtState2])
      end,
      Crdt:equal(CrdtState, CrdtState2)
    )},
    {equal_value, ?WHENFAIL(
      begin
        io:format("CRDT value before: ~p~n", [Crdt:value(CrdtState)]),
        io:format("CRDT value after: ~p~n", [rdt:value(CrdtState2)])
      end,
      Crdt:value(CrdtState) == Crdt:value(CrdtState2)
    )}
  ]).


%%printState(State) ->
%%  [printReplicaState(R, ReplicaState) || {R, ReplicaState} <- maps:to_list(State)].
%%
%%printReplicaState(R, S) ->
%%  io:format("Replica ~p : ~n", [R]),
%%  io:format("   State ~p : ~n", [S#test_replica_state.state]),
%%  io:format("   operations ~p : ~n", [S#test_replica_state.operations]),
%%  io:format("   downstreamOps ~p : ~n", [S#test_replica_state.downstreamOps]),
%%  ok.
