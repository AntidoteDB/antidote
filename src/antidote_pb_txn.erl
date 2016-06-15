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
            {ok, Msg, {"antidote.staticreadobjects",<<>>}};
        #apbgetobjects{} ->
            {ok, Msg, {"antidote.getobjects",<<>>}};
        #apbgetlogoperations{} ->
            {ok, Msg, {"antidote.getlogoperations",<<>>}};
	#apbjsonrequest{} ->
            {ok, Msg, {"antidote.jsonrequest",<<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

process(#apbstarttransaction{timestamp=BClock, properties = BProperties},
        State) ->
    Clock = antidote_pb_codec:decode(vectorclock,BClock),
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    process({start_transaction, Clock, Properties, proto_buf}, State);

process({start_transaction, Clock, Properties, ReplyType}, State) ->
    Response = antidote:start_transaction(Clock, Properties, true),
    EncodeType = 
	case ReplyType of
	    proto_buf -> start_transaction_response;
	    json -> start_transaction_response_json end,
    case Response of
        {ok, TxId} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {ok, TxId}),
             State};
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}),
             State}
    end;

process(#apbaborttransaction{transaction_descriptor=Td}, State) ->
    TxId = binary_to_term(Td),
    process({abort_transaction, TxId, proto_buf}, State);

process({abort_transaction, TxId, ReplyType}, State) ->
    Response = antidote:abort_transaction(TxId),
    EncodeType = 
	case ReplyType of
	    proto_buf -> operation_response;
	    json -> operation_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
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
    process({commit_transaction,TxId,proto_buf},State);

process({commit_transaction,TxId,ReplyType},State) ->
    Response = antidote:commit_transaction(TxId),
    EncodeType = 
	case ReplyType of
	    proto_buf -> commit_response;
	    json -> commit_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}), State};
        {ok, CommitTime} ->
	    lager:info("Commit time ~p", [CommitTime]),
            {reply, antidote_pb_codec:encode(EncodeType, {ok, CommitTime}),
             State}
    end;

process(#apbreadobjects{boundobjects=BoundObjects, transaction_descriptor=Td},
        State) ->
    Objects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                        BoundObjects),
    TxId = binary_to_term(Td),
    process({read_objects,Objects,TxId,proto_buf},State);

process({read_objects,Objects,TxId,ReplyType},State) ->
    lager:info("the read ~p", [{read_objects,Objects,TxId,ReplyType}]),
    Response = antidote:read_objects(Objects, TxId),
    lager:info("the response ~p", [Response]),
    EncodeType = 
	case ReplyType of
	    proto_buf -> read_objects_response;
	    json -> read_objects_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}), State};
        {ok, Results} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {ok, lists:zip(Objects,Results)}),
             State}
    end;

process(#apbupdateobjects{updates=BUpdates, transaction_descriptor=Td},
        State) ->
    Updates = lists:map(fun(O) ->
                                antidote_pb_codec:decode(update_object, O) end,
                        BUpdates),
    TxId = binary_to_term(Td),
    process({update_objects,Updates,TxId,proto_buf},State);

process({update_objects,Updates,TxId,ReplyType},State) ->
    lager:info("The request ~p", [{update_objects,Updates,TxId,ReplyType}]),
    Response = antidote:update_objects(Updates, TxId),
    EncodeType = 
	case ReplyType of
	    proto_buf -> operation_response;
	    json -> operation_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}),
             State};
        ok ->
            {reply, antidote_pb_codec:encode(EncodeType, ok),
             State}
    end;

process(#apbstaticupdateobjects{
           transaction=#apbstarttransaction{timestamp=BClock, properties = BProperties},
           updates=BUpdates},
        State) ->
    Clock = antidote_pb_codec:decode(vectorclock,BClock),
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    Updates = lists:map(fun(O) ->
                                antidote_pb_codec:decode(update_object, O) end,
                        BUpdates),
    process({static_update_objects,Clock,Updates,Properties,proto_buf},State);

process({static_update_objects,Clock,Updates,Properties,ReplyType},State) ->
    lager:info("the request ~p", [{static_update_objects,Clock,Updates,Properties,ReplyType}]),
    Response = antidote:update_objects(Clock, Properties, Updates, true),
    EncodeType = 
	case ReplyType of
	    proto_buf -> commit_response;
	    json -> commit_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}), State};
        {ok, CommitTime} ->
            {reply, antidote_pb_codec:encode(EncodeType, {ok, CommitTime}),
             State}
    end;            

process(#apbstaticreadobjects{
           transaction=#apbstarttransaction{timestamp=BClock, properties = BProperties},
           objects=BoundObjects},
        State) ->
    Clock = antidote_pb_codec:decode(vectorclock,BClock),
    Properties = antidote_pb_codec:decode(txn_properties, BProperties),
    Objects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                        BoundObjects),
    process({static_read_objects,Clock,Properties,Objects,proto_buf},State);

process({static_read_objects,Clock,Properties,Objects,ReplyType},State) ->
    Response = antidote:read_objects(Clock, Properties, Objects, true),
    EncodeType = 
	case ReplyType of
	    proto_buf -> static_read_objects_response;
	    json -> static_read_objects_response_json end,
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {error, Reason}), State};
        {ok, Results, CommitTime} ->
            {reply, antidote_pb_codec:encode(EncodeType,
                                             {ok, lists:zip(Objects,Results), CommitTime}),
             State}
    end;

%% For legion clients
process(#apbjsonrequest{value=JValue},State) ->
    lager:info("The request is ~p",[jsx:decode(JValue,[{labels,atom}])]),
    Req = antidote_pb_codec:decode_json(jsx:decode(JValue,[{labels,atom}])),
    process(Req,State);

process(#apbgetobjects{boundobjects=BoundObjects},State) ->
    Objects = lists:map(fun(O) ->
                                antidote_pb_codec:decode(bound_object, O) end,
                        BoundObjects),
    process({get_objects,Objects,proto_buf},State);

process({get_objects,Objects,Type},State) ->
    ReplyType = case Type of
		    proto_buf ->
			get_objects_response;
		    _ ->
			%% Default to json
			get_objects_response_json
		end,
    Response = antidote:get_objects(Objects,[]),
    Reply = case Response of
		{error, Reason} ->
		    antidote_pb_codec:encode(ReplyType,
                                             {error, Reason});
		{ok, Results} ->
		    antidote_pb_codec:encode(ReplyType,
					     {ok, lists:zip(Objects,Results)})
	    end,
    {reply, Reply, State};

process(#apbgetlogoperations{timestamps=BClocks,boundobjects=BoundObjects}, State) ->
    Objects =
	lists:map(fun(O) ->
			  antidote_pb_codec:decode(bound_object, O)
		  end, BoundObjects),
    Clocks =
	lists:map(fun(C) ->
			  antidote_pb_codec:decode(vectorclock, C)
		  end, BClocks),
    process({get_log_operations,Objects,Clocks,proto_buf},State);

process({get_log_operations,Objects,Clocks,Type},State) ->
    ReplyType = case Type of
		    proto_buf ->
			get_log_operations_response;
		    _ ->
			%% Default to json
			get_log_operations_response_json
		end,
    ObjectClockPairs = lists:zip(Objects,Clocks),
    Response = antidote:get_log_operations(ObjectClockPairs),
    case Response of
        {error, Reason} ->
            {reply, antidote_pb_codec:encode(ReplyType,
                                             {error, Reason}), State};
        {ok, Results} ->
	    %% {opid, #clocksi_payload}
            {reply, antidote_pb_codec:encode(ReplyType,
                                             {ok, lists:zip(Objects,Results)}),
             State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.
