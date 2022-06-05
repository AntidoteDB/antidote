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
-module(antidotec_pb).

-include_lib("antidote_pb_codec/include/antidote_pb.hrl").


-export([start_transaction/2,
         start_transaction/3,
         abort_transaction/2,
         commit_transaction/2,
         update_objects/3,
         read_objects/3,
         read_values/3]).

-define(TIMEOUT, 10000).

-spec start_transaction(Pid::pid(), TimeStamp::binary() | ignore)
        -> {ok, {interactive, term()} | {static, {term(), term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp) ->
    start_transaction(Pid, TimeStamp, []).

-spec start_transaction(Pid::pid(), TimeStamp::binary() | ignore, TxnProperties::term())
        -> {ok, {interactive, binary()} | {static, {binary(), term()}}} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    EncTimestamp = case TimeStamp of
                ignore -> term_to_binary(ignore);
                Binary -> Binary
            end,
    case is_static(TxnProperties) of
        true ->
            {ok, {static, {EncTimestamp, TxnProperties}}};
        false ->
            EncMsg = antidote_pb_codec:encode_request({start_transaction, EncTimestamp, TxnProperties}),
            Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
            case Result of
                {error, timeout} ->
                    {error, timeout};
                _ ->
                    case antidote_pb_codec:decode_response(Result) of
                        {start_transaction_response, {ok, TxId}} ->
                            {ok, {interactive, TxId}};
                        {start_transaction_response, {error, Reason}} ->
                            {error, Reason};
                        {error_response, Reason} ->
                            {error, Reason};
                        Other ->
                            {error, Other}
                    end
            end
    end.

-spec abort_transaction(Pid::pid(), {interactive, TxId::binary()}) -> ok | {error, term()}.
abort_transaction(Pid, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec:encode_request({abort_transaction, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {operation_response, ok} -> ok;
                {operation_response, {error, Reason}} -> {error, Reason};
                {error_response, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

-spec commit_transaction(Pid::pid(), TxId::{interactive, binary()} | {static, binary()}) ->
                                {ok, binary()} | {error, term()}.
commit_transaction(Pid, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec:encode_request({commit_transaction, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction_response, {ok, CommitTimeStamp}} -> {ok, CommitTimeStamp};
                {commit_transaction_response, {error, Reason}} -> {error, Reason};
                {error_response, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;
commit_transaction(Pid, {static, _TxId}) ->
    case antidotec_pb_socket:get_last_commit_time(Pid) of
        {ok, CommitTime} ->
             {ok, CommitTime}
    end.

-spec update_objects(Pid::pid(), Updates::[{term(), term(), term()}], {interactive | static, TxId::binary()}) -> ok | {error, term()}.
update_objects(Pid, Updates, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec: encode_request({update_objects, Updates, TxId}),
    Result = antidotec_pb_socket: call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec: decode_response(Result) of
                {operation_response, ok} -> ok;
                {operation_response, {error, Reason}} -> {error, Reason};
                {error_response, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;

update_objects(Pid, Updates, {static, TxId}) ->
    {Clock, Properties} = TxId,
    EncMsg = antidote_pb_codec:encode_request({static_update_objects, Clock, Properties, Updates}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction_response, {ok, CommitTimeStamp}} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    ok;
                {commit_transaction_response, {error, Reason}} ->
                    {error, Reason};
                {error_response, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

-spec read_objects(Pid::pid(), Objects::[term()], {interactive | static, TxId::binary()}) -> {ok, [term()]}  | {error, term()}.
read_objects(Pid, Objects, Transaction) ->
    case read_values(Pid, Objects, Transaction) of
        {ok, Values} ->
            ResObjects = lists:map(
                fun({Type, Val}) ->
                    Mod = antidotec_datatype:module_for_type(Type),
                    Mod:new(Val)
                end, Values),
            {ok, ResObjects};
        Other ->
            Other
    end.

-spec read_values(Pid::pid(), Objects::[term()], {interactive | static, TxId::binary()}) -> {ok, [term()]}  | {error, term()}.
read_values(Pid, Objects, {interactive, TxId}) ->
    EncMsg = antidote_pb_codec:encode_request({read_objects, Objects, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {read_objects_response, {ok, Values}} ->
                    {ok, Values};
                {read_objects_response, {error, Reason}} ->
                    {error, Reason};
                {error_response, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end;
read_values(Pid, Objects, {static, TxId}) ->
    {Clock, Properties} = TxId,
    EncMsg = antidote_pb_codec:encode_request({static_read_objects, Clock, Properties, Objects}),
    Result = antidotec_pb_socket:call_infinity(Pid, {req, EncMsg, ?TIMEOUT}),
    case Result of
        {error, timeout} -> {error, timeout};
        _ ->
            case antidote_pb_codec:decode_response(Result) of
                {static_read_objects_response, {Values, CommitTimeStamp}} ->
                    antidotec_pb_socket:store_commit_time(Pid, CommitTimeStamp),
                    {ok, Values};
                {error_response, Reason} -> {error, Reason}
            end
    end.


is_static(TxnProperties) ->
    case TxnProperties of
        [{static, true}] ->
            true;
        _ -> false
    end.
