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

-module(cure).

-include("antidote.hrl").


-export([
         start_transaction/2,
         commit_transaction/1,
         abort_transaction/1,
         read_objects/2,
         get_objects/2,
         read_objects/3,
         get_objects/3,
         update_objects/2,
         update_objects/3,
         obtain_objects/4,
         %% Following functions should be only used for testing
         clocksi_iprepare/1,
         clocksi_icommit/1
        ]).


-spec start_transaction(snapshot_time() | ignore, txn_properties())
                       -> {ok, txid()} | {error, reason()}.
start_transaction(Clock, Properties) ->
    clocksi_istart_tx(Clock, Properties).


-spec abort_transaction(txid()) -> ok | {error, reason()}.
abort_transaction(TxId) ->
    case gen_statem:call(TxId#tx_id.server_pid, {abort, []}) of
        {error, aborted} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec commit_transaction(txid()) ->
                                {ok, snapshot_time()} | {error, reason()}.
commit_transaction(TxId) ->
    case clocksi_full_icommit(TxId) of
        {ok, {_TxId, CommitTime}} ->
            {ok, CommitTime};
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, Other}
    end.

-spec read_objects([bound_object()], txid()) -> {ok, [term()]} | {error, reason()}.
read_objects(Objects, TxId) ->
    obtain_objects(Objects, TxId, object_value).
-spec get_objects([bound_object()], txid()) -> {ok, [term()]} | {error, reason()}.
get_objects(Objects, TxId) ->
    obtain_objects(Objects, TxId, object_state).


-spec obtain_objects([bound_object()], txid(), object_value|object_state) -> {ok, [term()]} | {error, reason()}.
obtain_objects(Objects, TxId, StateOrValue) ->
    FormattedObjects = format_read_params(Objects),
    case gen_statem:call(TxId#tx_id.server_pid, {read_objects, FormattedObjects}, ?OP_TIMEOUT) of
        {ok, Res} ->
            {ok, transform_reads(Res, StateOrValue, Objects)};
        {error, Reason} -> {error, Reason}
    end.

-spec update_objects([{bound_object(), op_name(), op_param()}], txid())
                    -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    FormattedUpdates = format_update_params(Updates),
    case gen_statem:call(TxId#tx_id.server_pid, {update_objects, FormattedUpdates}, ?OP_TIMEOUT) of
        ok ->
            ok;
        {aborted, TxId} ->
            {error, aborted};
        {error, Reason} ->
            {error, Reason}
    end.

%% For static transactions: bulk updates and bulk reads
-spec update_objects(snapshot_time() | ignore , list(), [{bound_object(), op_name(), op_param()}]) ->
                            {ok, snapshot_time()} | {error, reason()}.
update_objects(_Clock, _Properties, []) ->
    {ok, vectorclock:new()};
update_objects(ClientCausalVC, Properties, Updates) ->
    {ok, TxId} = clocksi_istart_tx(ClientCausalVC, Properties),
    case update_objects(Updates, TxId) of
        ok -> commit_transaction(TxId);
        {error, Reason} -> {error, Reason}
    end.

-spec read_objects(snapshot_time() | ignore, txn_properties(), [bound_object()]) ->
    {ok, list(), vectorclock()} | {error, reason()}.
read_objects(Clock, Properties, Objects) ->
    obtain_objects(Clock, Properties, Objects, object_value).
get_objects(Clock, Properties, Objects) ->
    obtain_objects(Clock, Properties, Objects, object_state).


-spec obtain_objects(snapshot_time() | ignore, txn_properties(), [bound_object()], object_value|object_state) ->
                          {ok, list(), vectorclock()} | {error, reason()}.
obtain_objects(Clock, Properties, Objects, StateOrValue) ->
    SingleKey = case Objects of
                    [_O] -> %% Single key update
                        case Clock of
                            ignore -> true;
                            _ -> false
                        end;
                    [_H|_T] -> false
                end,
    case SingleKey of
        true -> %% Execute the fast path
            FormattedObjects = format_read_params(Objects),
            [{Key, Type}] = FormattedObjects,
            {ok, Val, CommitTime} = clocksi_interactive_coord:
                perform_singleitem_operation(Clock, Key, Type, Properties),
            {ok, transform_reads([Val], StateOrValue, Objects), CommitTime};
        false ->
            case application:get_env(antidote, txn_prot) of
                {ok, clocksi} ->
                    {ok, TxId} = clocksi_istart_tx(Clock, Properties),
                    case obtain_objects(Objects, TxId, StateOrValue) of
                        {ok, Res} ->
                            {ok, CommitTime} = commit_transaction(TxId),
                            {ok, Res, CommitTime};
                        {error, Reason} -> {error, Reason}
                    end;
                {ok, gr} ->
                    case Objects of
                        [_Op] -> %% Single object read = read latest value
                            {ok, TxId} = clocksi_istart_tx(Clock, Properties),
                            case obtain_objects(Objects, TxId, StateOrValue) of
                                {ok, Res} ->
                                    {ok, CommitTime} = commit_transaction(TxId),
                                    {ok, Res, CommitTime};
                                {error, Reason} -> {error, Reason}
                            end;
                        [_|_] -> %% Read Multiple objects  = read from a snapshot
                            %% Snapshot includes all updates committed at time GST
                            %% from local and remore replicas
                            case gr_snapshot_obtain(Clock, Objects, StateOrValue) of
                                {ok, Result, CommitTime} ->
                                    {ok, Result, CommitTime};
                                {error, Reason} -> {error, Reason}
                            end
                    end
            end
        end.


transform_reads(States, StateOrValue, Objects) ->
    case StateOrValue of
            object_state -> States;
            object_value -> lists:map(fun({State, {_Key, Type, _Bucket}}) ->
                                          Type:value(State) end,
                                      lists:zip(States, Objects))
    end.


%% @doc Starts a new ClockSI interactive transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Returns: an ok message along with the new TxId.
%%
-spec clocksi_istart_tx(snapshot_time() | ignore, txn_properties()) ->
                               {ok, txid()} | {error, reason()}.
clocksi_istart_tx(Clock, Properties) ->
    {ok, Pid} = clocksi_interactive_coord_sup:start_fsm(),
    gen_statem:call(Pid, {start_tx, Clock, Properties}).


-spec clocksi_full_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}}
                                          | {error, reason()}.
clocksi_full_icommit(TxId)->
    case gen_statem:call(TxId#tx_id.server_pid, {prepare, empty}, ?OP_TIMEOUT) of
        {ok, _PrepareTime} ->
            gen_statem:call(TxId#tx_id.server_pid, commit, ?OP_TIMEOUT);
        Msg ->
            Msg
    end.

%%% Snapshot read for Gentlerain protocol
gr_snapshot_obtain(ClientClock, Objects, StateOrValue) ->
    %% GST = scalar stable time
    %% VST = vector stable time with entries for each dc
    {ok, GST, VST} = dc_utilities:get_scalar_stable_time(),
    DcId = dc_utilities:get_my_dc_id(),
    Dt = vectorclock:get(DcId, ClientClock),
    case Dt =< GST of
        true ->
            %% Set all entries in snapshot as GST
            ST = vectorclock:set_all(GST, VST),
            %% ST doesnot contain entry for local dc, hence explicitly
            %% add it in snapshot time
            SnapshotTime = vectorclock:set(DcId, GST, ST),
            {ok, TxId} = clocksi_istart_tx(SnapshotTime, [{update_clock, false}]),
            case obtain_objects(Objects, TxId, StateOrValue) of
                {ok, Res} ->
                    {ok, CommitTime} = commit_transaction(TxId),
                    {ok, Res, CommitTime};
                {error, Reason} -> {error, Reason}
            end;
        false ->
            timer:sleep(10),
            gr_snapshot_obtain(ClientClock, Objects, StateOrValue)
    end.

format_read_params(ReadObjects) ->
    lists:map(fun({Key, Type, Bucket}) ->
                      {{Key, Bucket}, Type}
              end, ReadObjects).

format_update_params(Updates) ->
    lists:map(fun({{Key, Type, Bucket}, Op, Param}) ->
                      {{Key, Bucket}, Type, {Op, Param}}
              end, Updates).

%% The following function are usefull for testing. They shouldn't be used in normal operations.
-spec clocksi_iprepare(txid()) -> {aborted, txid()} | {ok, non_neg_integer()}.
clocksi_iprepare(TxId)->
    case gen_statem:call(TxId#tx_id.server_pid, {prepare, two_phase}, ?OP_TIMEOUT) of
        {error, {aborted, TxId}} ->
            {aborted, TxId};
        Reply ->
            Reply
    end.

-spec clocksi_icommit(txid()) -> {aborted, txid()} | {ok, {txid(), snapshot_time()}}.
clocksi_icommit(TxId)->
    gen_statem:call(TxId#tx_id.server_pid, commit, ?OP_TIMEOUT).
