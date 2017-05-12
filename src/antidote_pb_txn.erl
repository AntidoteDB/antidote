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
-module(antidote_pb_txn).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/antidote_pb.hrl").
-include("antidote.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #apbstarttransaction{} ->
            {ok, Msg, {"antidote.startxn",<<>>}};
        #apbaborttransaction{} ->
            {ok, Msg, {"antidote.aborttxn",<<>>}};
        #apbcommittransaction{} ->
            {ok, Msg, {"antidote.committxn",<<>>}};
        #apbreadobjects{} ->
            {ok, Msg, {"antidote.readobjects",<<>>}};
        #apbupdateobjects{} ->
            {ok, Msg, {"antidote.updateobjects",<<>>}};
        #apbstaticupdateobjects{} ->
            {ok, Msg, {"antidote.staticupdateobjects",<<>>}};
        #apbstaticreadobjects{} ->
            {ok, Msg, {"antidote.staticreadobjects",<<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

process(#apbstarttransaction{timestamp=BClock, properties = BProperties},
        State) ->
    Clock = case BClock of
                    undefined -> ignore;
                    _ -> binary_to_term(BClock)
            end,
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    Response = antidote:start_transaction(Clock, Properties),
    case Response of
        {ok, TxId} ->
            {reply, antidote_pb_codec:encode(start_transaction_response,
                                             {ok, TxId}),
             State};
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(start_transaction_response,
                                             {error, Reason}),
             State}
    end;

process(#apbaborttransaction{transaction_descriptor=Td}, State) ->
    TxId = binary_to_term(Td),
    Response = antidote:abort_transaction(TxId),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(operation_response,
                                             {error, Reason}),
             State}
            %% TODO: client initiated abort is not implemented yet
            %% Add the following only after it is implemented to avoid dialyzer errors
            %% ok ->
            %%     {reply, antidote_pb_codec:encode(operation_response, ok),
            %%      State}
    end;

process(#apbcommittransaction{transaction_descriptor=Td}, State) ->
    TxId = binary_to_term(Td),
    Response = antidote:commit_transaction(TxId),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(commit_response,
                                             {error, Reason}), State};
        {ok, CommitTime} ->
            {reply, antidote_pb_codec:encode(commit_response, {ok, CommitTime}),
             State}
    end;

process(#apbreadobjects{boundobjects=BoundObjects, transaction_descriptor=Td},
        State) ->
    Objects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                        BoundObjects),

    TxId = binary_to_term(Td),
    Response = antidote:read_objects(Objects, TxId),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(read_objects_response,
                                             {error, Reason}), State};
        {ok, Results} ->
            {reply, antidote_pb_codec:encode(read_objects_response,
                                             {ok, lists:zip(Objects, Results)}),
             State}
    end;

process(#apbupdateobjects{updates=BUpdates, transaction_descriptor=Td},
        State) ->
    Updates = lists:map(fun(O) ->
                                antidote_pb_codec:decode(update_object, O) end,
                        BUpdates),

    TxId = binary_to_term(Td),
    Response = antidote:update_objects(Updates, TxId),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(operation_response,
                                             {error, Reason}),
             State};
        ok ->
            {reply, antidote_pb_codec:encode(operation_response, ok),
             State}
    end;
process(#apbstaticupdateobjects{
           transaction=#apbstarttransaction{timestamp=BClock, properties = BProperties},
           updates=BUpdates},
        State) ->

    Clock = case BClock of
                    undefined -> ignore;
                    _ -> binary_to_term(BClock)
            end,
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    Updates = lists:map(fun(O) ->
                                antidote_pb_codec:decode(update_object, O) end,
                        BUpdates),
    Response = antidote:update_objects(Clock, Properties, Updates),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(commit_response,
                                             {error, Reason}), State};
        {ok, CommitTime} ->
            {reply, antidote_pb_codec:encode(commit_response, {ok, CommitTime}),
             State}
    end;
process(#apbstaticreadobjects{
           transaction=#apbstarttransaction{timestamp=BClock, properties = BProperties},
           objects=BoundObjects},
        State) ->
    Clock = case BClock of
                    undefined -> ignore;
                    _ -> binary_to_term(BClock)
            end,
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    Objects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                        BoundObjects),
    Response = antidote:read_objects(Clock, Properties, Objects),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(commit_response,
                                             {error, Reason}), State};
        {ok, Results, CommitTime} ->
            {reply, antidote_pb_codec:encode(static_read_objects_response,
                                             {ok, lists:zip(Objects, Results), CommitTime}),
             State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_, _, State) ->
    {ignore, State}.
