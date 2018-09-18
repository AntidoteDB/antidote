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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module that contains some common and utility
%%%      functions for, but not exclusively to, the indexing and
%%%      query_optimizer modules.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(querying_utils).
-behavior(gen_statem).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(LOG_UTIL, mock_partition).
-else.
-define(LOG_UTIL, log_utilities).
-endif.

-include("antidote.hrl").
-include("querying.hrl").
-include("lock_mgr.hrl").
-include("lock_mgr_es.hrl").

-record(state, {
    from :: undefined | {pid(), term()} | pid(),
    meta :: term()
}).

%% API
%% API
-export([start_link/0]).

-export([build_keys/3, build_keys_from_table/3,
    read_keys/3, read_keys/2,
    read_function/3, read_function/2,
    write_keys/2, write_keys/1,
    start_transaction/0, commit_transaction/1,
    get_locks/3, get_locks/4, release_locks/2]).

-export([to_atom/1,
    to_list/1,
    to_binary/1,
    to_term/1,
    remove_duplicates/1,
    is_list_of_lists/1,
    replace/3,
    first_occurrence/2]).

%% gen_statem callbacks
-export([
    init/1,
    code_change/4,
    callback_mode/0,
    terminate/3,
    stop/1
]).

%% states
-export([
    execute/3,
    receive_read_async/3
]).

build_keys([], _Types, _Bucket) -> [];
build_keys(Keys, Types, Bucket) when is_list(Keys) and is_list(Types) ->
    build_keys(Keys, Types, Bucket, []);
build_keys(Keys, Type, Bucket) when is_list(Keys) ->
    Len = length(Keys),
    build_keys(Keys, lists:duplicate(Len, Type), Bucket);
build_keys(Key, Type, Bucket) ->
    build_keys([Key], [Type], Bucket).

build_keys([Key | Tail1], [Type | Tail2], Bucket, Acc) ->
    BucketAtom = to_atom(Bucket),
    TypeAtom = to_atom(Type),
    ObjKey = {Key, TypeAtom, BucketAtom},
    build_keys(Tail1, Tail2, Bucket, lists:append(Acc, [ObjKey]));
build_keys([], [], _Bucket, Acc) ->
    Acc.

build_keys_from_table(Keys, Table, TxId) when is_list(Keys) ->
    build_keys_from_table(Keys, Table, TxId, []);
build_keys_from_table(Key, Table, TxId) ->
    build_keys_from_table([Key], Table, TxId).

build_keys_from_table([{AtomKey, RawKey} | Keys], Table, TxId, Acc) ->
    TName = table_utils:name(Table),
    PartCol = table_utils:partition_column(Table),

    BoundKey =
        case PartCol of
            [_] ->
                {ok, Index} = index_manager:read_index(primary, TName, TxId),
                {_, BObj} = lists:keyfind(RawKey, 1, Index),
                BObj;
            undefined ->
                [BObj] = build_keys(AtomKey, ?TABLE_DT, TName),
                BObj
        end,
    build_keys_from_table(Keys, Table, TxId, lists:append(Acc, [BoundKey]));
build_keys_from_table([], _Table, _TxId, Acc) ->
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%     Read Values or States    %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

read_keys(_StateOrValue, [], _TxId) -> [[]];
read_keys(StateOrValue, ObjKeys, ignore) ->
    read_keys(StateOrValue, ObjKeys);
read_keys(StateOrValue, ObjKeys, TxId) when is_list(ObjKeys) ->
    read_crdts(StateOrValue, ObjKeys, TxId);
read_keys(StateOrValue, ObjKey, TxId) ->
    read_keys(StateOrValue, [ObjKey], TxId).

read_keys(_StateOrValue, []) -> [[]];
read_keys(StateOrValue, ObjKeys) when is_list(ObjKeys) ->
    read_crdts(StateOrValue, ObjKeys);
read_keys(StateOrValue, ObjKey) ->
    read_keys(StateOrValue, [ObjKey]).

%% Applying a function on a set of keys implies returning the values
%% of the CRDTs mapped by those keys.
%-spec read_function(bound_object() | [bound_object()], {atom(), [term()]}, txid() | {txid(), [term()], [term()]}) ->
%    term() | {error, reason()} | [term() | {error, reason()}].
read_function([], _Func, _TxId) -> [[]];
read_function(ObjKeys, Function, ignore) ->
    read_function(ObjKeys, Function);

read_function(ObjKeys, {Function, Args}, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->

    Reads = lists:map(fun(Key) -> {Key, {Function, Args}} end, ObjKeys),
    read_crdts(value, Reads, TxId);
read_function(ObjKeys, {Function, Args}, {TxId, ReadSet, UpdatedPartitions})
    when is_list(ObjKeys) andalso is_record(TxId, transaction)  ->

    lists:map(fun({Key, Type, Bucket}) ->
        Partition = ?LOG_UTIL:get_key_partition({Key, Bucket}),
        WriteSet = get_write_set(Partition, UpdatedPartitions),

        case clocksi_object_function:sync_execute_object_function(
            TxId, Partition, {Key, Bucket}, Type, {Function, Args}, WriteSet, ReadSet) of
            {ok, {Key, Type, _, _, Value}} ->
                Value;
            {error, Reason} ->
                {error, Reason}
        end
        %Args = {Partition, TxId, WriteSet, Key, Type},
        %case gen_statem:call(?MODULE, {read_async, Args}) of
        %    {ok, Snapshot} ->
        %        case Type:is_operation({Function, Args}) of
        %            true ->
        %                Value = Type:value({Function, Args}, Snapshot),
        %                Value;
        %            false ->
        %                {error, {function_not_supported, {Function, Args}}}
        %        end;
        %    {error, Reason} ->
        %        {error, Reason}
        %end
    end, ObjKeys);
read_function(ObjKey, Range, TxId) ->
    read_function([ObjKey], Range, TxId).

%-spec read_function(bound_object() | [bound_object()], {atom(), [term()]}) ->
%    term() | {error, reason()} | [term() | {error, reason()}].
read_function([], _Func) -> [[]];
read_function(ObjKeys, {Function, Args}) when is_list(ObjKeys) ->
    Reads = lists:map(fun(Key) -> {Key, Function, Args} end, ObjKeys),
    read_crdts(value, Reads).

write_keys(Updates, TxId) when is_list(Updates) ->
    cure:update_objects(Updates, TxId);
write_keys(Update, TxId) when is_tuple(Update) ->
    write_keys([Update], TxId).

write_keys(Updates) when is_list(Updates) ->
    cure:update_objects(ignore, [], Updates);
write_keys(Update) when is_tuple(Update) ->
    write_keys([Update]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%         Transaction          %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_transaction() ->
    cure:start_transaction(ignore, []).

commit_transaction(TxId) ->
    cure:commit_transaction(TxId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%            Locks             %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_locks(default, Locks, TxId) ->
    get_locks(?How_LONG_TO_WAIT_FOR_LOCKS, TxId, Locks);
get_locks(Timeout, Locks, TxId) ->
    Res = clocksi_interactive_coord:get_locks(Timeout, TxId, Locks),
    case Res of
        {ok, _} -> ok;
        {missing_locks, Keys} ->
            ErrorMsg = io_lib:format("One or more locks are missing: ~p", [Keys]),
            throw(lists:flatten(ErrorMsg));
        {locks_in_use, {UsedExclusive, _UsedShared}} ->
            FilterNotThisTx =
                lists:filter(fun({TxId0, _LockList}) -> TxId0 /= TxId end, UsedExclusive),
            case FilterNotThisTx of
                [] -> ok;
                _ ->
                    ErrorMsg = io_lib:format("One or more exclusive locks are being used by other transactions: ~p", [FilterNotThisTx]),
                    throw(lists:flatten(ErrorMsg))
            end
    end.

get_locks(default, SharedLocks, ExclusiveLocks, TxId) ->
    get_locks(?How_LONG_TO_WAIT_FOR_LOCKS_ES, TxId, SharedLocks, ExclusiveLocks);
get_locks(Timeout, SharedLocks, ExclusiveLocks, TxId) ->
    Res = clocksi_interactive_coord:get_locks(Timeout, TxId, SharedLocks, ExclusiveLocks),
    case Res of
        {ok, _} -> ok;
        {missing_locks, Keys} ->
            ErrorMsg = io_lib:format("One or more locks are missing: ~p", [Keys]),
            throw(lists:flatten(ErrorMsg));
        {locks_in_use, {UsedExclusive, _UsedShared}} ->
            FilterNotThisTx =
                lists:filter(fun({TxId0, _LockList}) -> TxId0 /= TxId end, UsedExclusive),
            case FilterNotThisTx of
                [] -> ok;
                _ ->
                    ErrorMsg = io_lib:format("One or more exclusive locks are being used by other transactions: ~p", [FilterNotThisTx]),
                    throw(lists:flatten(ErrorMsg))
            end
    end.

release_locks(Type, TxId) ->
    clocksi_interactive_coord:release_locks(Type, TxId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%          Utilities           %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

to_atom(Term) when is_list(Term) ->
    list_to_atom(Term);
to_atom(Term) when is_integer(Term) ->
    List = integer_to_list(Term),
    list_to_atom(List);
to_atom(Term) when is_binary(Term) ->
    List = binary_to_list(Term),
    list_to_atom(List);
to_atom(Term) when is_atom(Term) ->
    Term.

to_list(Term) when is_list(Term) ->
    Term;
to_list(Term) when is_integer(Term) ->
    integer_to_list(Term);
to_list(Term) when is_binary(Term) ->
    binary_to_list(Term);
to_list(Term) when is_atom(Term) ->
    atom_to_list(Term).

to_binary(Term) when is_list(Term) ->
    list_to_binary(Term);
to_binary(Term) when is_integer(Term) ->
    integer_to_binary(Term);
to_binary(Term) when is_binary(Term) ->
    Term;
to_binary(Term) when is_atom(Term) ->
    ToList = atom_to_list(Term),
    list_to_binary(ToList).

to_term(Term) when is_binary(Term) ->
    binary_to_term(Term);
to_term(Term) ->
    Term.

remove_duplicates(List) when is_list(List) ->
    Aux = sets:from_list(List),
    sets:to_list(Aux);
remove_duplicates(Other) ->
    case sets:is_set(Other) of
        true -> Other;
        false ->
            ErrorMsg = io_lib:format("Cannot remove duplicates in this object: ~p", [Other]),
            throw(lists:flatten(ErrorMsg))
    end.

is_list_of_lists(List) when is_list(List) ->
    NotDropped = lists:dropwhile(fun(Elem) -> is_list(Elem) end, List),
    NotDropped =:= [];
is_list_of_lists(_) -> false.

replace(N, Element, List) when N >= 0 andalso N < length(List)->
    {First, [_H | Second]} = lists:split(N, List),
    lists:append(First, [Element | Second]).

first_occurrence(Predicate, [Elem | List]) ->
    case Predicate(Elem) of
        true -> Elem;
        false -> first_occurrence(Predicate, List)
    end;
first_occurrence(_Predicate, []) -> undefined.

%% ====================================================================
%% gen_statem functions
%% ====================================================================

start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    State = #state{},
    {ok, execute, State}.

execute({call, Sender}, {Op, Args}, State) ->
    case execute_util(Op, Args, Sender, State) of
        {receive_read_async, NewState} ->
            {next_state, receive_read_async, NewState}
    end.

execute_util(read_async, Args, Sender, _State) ->
    {Partition, Transaction, WriteSet, Key, Type} = Args,
    ok = clocksi_vnode:async_read_data_item(Partition, Transaction, Key, Type),
    {receive_read_async, #state{from = Sender, meta = WriteSet}}.

receive_read_async(cast, Response, _State = #state{from = Sender, meta = WriteSet}) ->
    case Response of
        {ok, {_, Key, Type, Snapshot}} ->
            Updates2 = clocksi_vnode:reverse_and_filter_updates_per_key(WriteSet, Key),
            Snapshot2 = clocksi_materializer:materialize_eager(Type, Snapshot, Updates2),
            {next_state, execute, #state{from = undefined},
                [{reply, Sender, {ok, Snapshot2}}]};
        {error, Reason} ->
            {next_state, execute, #state{from = undefined},
                [{reply, Sender, {error, Reason}}]}
    end.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.
callback_mode() -> state_functions.

stop(Pid) -> gen_statem:stop(Pid).

%% ====================================================================
%% Internal functions
%% ====================================================================


read_crdts(StateOrValue, ObjKeys, {TxId, _ReadSet, _UpdatedPartitions} = Transaction)
    when is_list(ObjKeys) andalso is_record(TxId, transaction) ->
    {ok, Objs} = read_data_items(StateOrValue, ObjKeys, Transaction),
    Objs;
read_crdts(StateOrValue, ObjKey, {TxId, _ReadSet, _UpdatedPartitions} = Transaction)
    when is_record(TxId, transaction) ->
    read_crdts(StateOrValue, [ObjKey], Transaction);

read_crdts(value, ObjKeys, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->
    {ok, Objs} = cure:read_objects(ObjKeys, TxId),
    Objs;
read_crdts(state, ObjKeys, TxId)
    when is_list(ObjKeys) andalso is_record(TxId, tx_id) ->
    {ok, Objs} = cure:get_objects(ObjKeys, TxId),
    Objs;
read_crdts(StateOrValue, ObjKey, TxId) ->
    read_crdts(StateOrValue, [ObjKey], TxId).

read_crdts(value, ObjKeys) when is_list(ObjKeys) ->
    {ok, Objs, _} = cure:read_objects(ignore, [], ObjKeys),
    Objs;
read_crdts(state, ObjKeys) when is_list(ObjKeys) ->
    {ok, Objs, _} = cure:get_objects(ignore, [], ObjKeys),
    Objs;
read_crdts(StateOrValue, ObjKey) ->
    read_crdts(StateOrValue, [ObjKey]).

read_data_items(StateOrValue, ObjKeys, Transaction) when is_list(ObjKeys) ->
    ReadObjects = lists:map(fun({_Key, Type, _Bucket} = ObjKey) ->
        {ok, Snapshot} = read_data_item(ObjKey, Transaction),
        case StateOrValue of
            value -> Type:value(Snapshot);
            state -> Snapshot
        end
    end, ObjKeys),
    {ok, ReadObjects}.

read_data_item({Key, Type, Bucket}, {Transaction, ReadSet, UpdatedPartitions}) ->
    SendKey = {Key, Bucket},
    case orddict:find(SendKey, ReadSet) of
        error ->
            Partition = ?LOG_UTIL:get_key_partition(SendKey),
            WriteSet = get_write_set(Partition, UpdatedPartitions),

            Args = {Partition, Transaction, WriteSet, SendKey, Type},

            {ok, Snapshot} = gen_statem:call(?MODULE, {read_async, Args}),% clocksi_vnode:read_data_item(Partition, Transaction, SendKey, Type, WriteSet),
            {ok, Snapshot};
        {ok, State} ->
            {ok, State}
    end.

get_write_set(Partition, Partitions) ->
    case lists:keyfind(Partition, 1, Partitions) of
        false -> [];
        {Partition, WS} -> WS
    end.

