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

-include_lib("riak_pb/include/antidote_pb.hrl").


-export([start_transaction/3,
         abort_transaction/2,
         commit_transaction/2,
         update_objects/3,
         read_objects/3]).

-define(TIMEOUT, 10000).

-spec start_transaction(Pid::term(), TimeStamp::term(), TxnProperties::term()) -> {ok, term()} | {error, term()}.
start_transaction(Pid, TimeStamp, TxnProperties) ->
    EncMsg = antidote_pb_codec:encode(start_transaction, {TimeStamp, TxnProperties}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of 
        {error, timeout} -> {error, timeout};
        _ ->  
            case antidote_pb_codec:decode_response(Result) of
                {start_transaction, TxId} ->
                    {ok, TxId};
                {error, Reason} ->
                    {error, Reason};
                Other ->
                    {error, Other}
            end
    end.

-spec abort_transaction(Pid::term(), TxId::term()) -> ok.
abort_transaction(Pid, TxId) ->
    EncMsg = antidote_pb_codec:encode(abort_transaction, TxId),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of 
        {error, timeout} -> {error, timeout};
        _ ->  
            case antidote_pb_codec:decode_response(Result) of
                {opresponse, ok} -> ok;
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

-spec commit_transaction(Pid::term(), TxId::term()) -> {ok, term()} | {error, term()}.
commit_transaction(Pid, TxId) ->
    EncMsg = antidote_pb_codec:encode(commit_transaction, TxId),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of 
        {error, timeout} -> {error, timeout};
        _ ->       
            case antidote_pb_codec:decode_response(Result) of
                {commit_transaction, CommitTimeStamp} -> {ok, CommitTimeStamp};
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

update_objects(Pid, Updates, TxId) ->
    EncMsg = antidote_pb_codec: encode(update_objects, {Updates, TxId}),
    Result = antidotec_pb_socket: call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of 
        {error, timeout} -> {error, timeout};
        _ ->  
            case antidote_pb_codec: decode_response(Result) of
                {opresponse, ok} -> ok;
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.

read_objects(Pid, Objects, TxId) ->
    EncMsg = antidote_pb_codec:encode(read_objects, {Objects, TxId}),
    Result = antidotec_pb_socket:call_infinity(Pid,{req, EncMsg, ?TIMEOUT}),
    case Result of 
        {error, timeout} -> {error, timeout};
        _ ->  
            case antidote_pb_codec:decode_response(Result) of
                {read_objects, Values} -> {ok, Values};
                {error, Reason} -> {error, Reason};
                Other -> {error, Other}
            end
    end.
