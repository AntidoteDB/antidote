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
-module(ec_materializer).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([new/1,
    create_snapshot/1,
    materialize_eager/3,
    check_operations/1,
    check_operation/1,
    is_crdt/1]).

%% @doc Creates an empty CRDT for a given type.
-spec new(type()) -> snapshot().
new(Type) ->
    create_snapshot(Type).

%% @doc Creates an empty CRDT
-spec create_snapshot(type()) -> snapshot().
create_snapshot(Type) ->
    Type:new().

%% @doc Applies an operation to a snapshot of a crdt.
%%      This function yields an error if the crdt does not have a corresponding update operation.
-spec update_snapshot(type(), snapshot(), op()) -> {ok, snapshot()} | {error, reason()}.
update_snapshot(Type, Snapshot, Op) ->
    case Op of
        {merge, State} ->
            {ok, Type:merge(Snapshot, State)};
        {update, DownstreamOp} ->
            Type:update(DownstreamOp, Snapshot);
        _ ->
            %lager:info("Unexpected log record: ~p for snapshot: ~p", [Op, Snapshot]),
            {error, unexpected_format, Op}
    end.

%% @doc Apply updates in given order without any checks.
%%    Careful: In contrast to materialize/6, it takes just operations, not ec_payloads!
-spec materialize_eager(type(), snapshot(), [op()]) -> snapshot() | {error, reason()}.
materialize_eager(_Type, Snapshot, []) ->
    Snapshot;
materialize_eager(Type, Snapshot, [Op | Rest]) ->
    case update_snapshot(Type, Snapshot, Op) of
        {error, Reason} ->
            {error, Reason};
        {ok, Result} ->
            materialize_eager(Type, Result, Rest)
    end.

%% @doc Check that in a list of operations, all of them are correctly typed.
-spec check_operations(list()) -> ok | {error, term()}.
check_operations([]) ->
    ok;
check_operations([Op | Rest]) ->
    case check_operation(Op) of
        true ->
            check_operations(Rest);
        false ->
            {error, {type_check, Op}}
    end.

%% @doc Check that an operation is correctly typed.
-spec check_operation(term()) -> boolean().
check_operation(Op) ->
    case Op of
        {update, {_, Type, {OpParams, _Actor}}} ->
            (riak_dt:is_riak_dt(Type) or is_crdt(Type)) andalso
                Type:is_operation(OpParams);
        {read, {_, Type}} ->
            (riak_dt:is_riak_dt(Type) or is_crdt(Type));
        _ ->
            false
    end.


%% @doc Check that an atom is an op_based CRDT type.
%%      The list of op_based CRDTS is defined in antidote.hrl
-spec is_crdt(term()) -> boolean().
is_crdt(Term) ->
    is_atom(Term) andalso lists:member(Term, ?CRDTS).
