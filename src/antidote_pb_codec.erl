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
-module(antidote_pb_codec).

-include("antidote_pb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([decode_request/1, decode_response/1, encode_request/1, encode_response/1]).

% these are all top-level messages which can be sent on the wire
-export_type([request/0, response_in/0, response_out/0, update/0, read_result/0]).

-type bound_object() :: {Key :: binary(), Type :: atom(), Bucket :: binary()}.
-type update() :: {Object :: bound_object(), Op :: atom(), Param :: any()}.
-type error_code() :: unknown |  timeout | no_permissions | aborted | {error_code, integer()}.
-type read_result() ::
  {counter, integer()}
| {set, [binary()]}
| {reg, binary()}
| {mvreg, [binary()]}
| {map, [{{Key :: binary(), Type :: atom()}, Value :: read_result()}]}
| {flag, boolean()}.

-type read_result_in() ::
  {antidote_crdt_counter_fat, integer()}
| {antidote_crdt_counter_pn, integer()}
| {antidote_crdt_set_aw, [binary()]}
| {antidote_crdt_set_rw, [binary()]}
| {antidote_crdt_register_lww, binary()}
| {antidote_crdt_register_mv, [binary()]}
| {antidote_crdt_map_go, [{{Key :: binary(), Type :: atom()}, Value :: read_result_in()}]}
| {antidote_crdt_map_rr, [{{Key :: binary(), Type :: atom()}, Value :: read_result_in()}]}
| {antidote_crdt_flag_dw, boolean()}
| {antidote_crdt_flag_ew, boolean()}
.


-type request() ::
  {start_transaction, Clock :: binary(), Properties :: list()}
| {abort_transaction, TxId :: binary()}
| {commit_transaction, TxId :: binary()}
| {update_objects, Updates :: [update()], TxId :: binary()}
| {static_update_objects, Clock :: binary(), Properties :: list(), Updates :: [update()]}
| {static_read_objects, Clock :: binary(), Properties :: list(), Objects :: [bound_object()]}
| {read_objects, Objects :: [bound_object()], TxId :: binary()}
| {create_dc, NodeNames :: [node()]}
| get_connection_descriptor
| {connect_to_dcs, Descriptors :: [binary()]}.

-type response_out() ::
  {error_response, {ErrorCode :: error_code(), Message :: binary()}}
| {start_transaction_response, {ok, TxId :: binary()} | {error, Reason :: error_code()}}
| {commit_response, {ok, CommitTime :: binary()} | {error, Reason :: error_code()}}
| {static_read_objects_response, {Results :: [read_result()], CommitTime :: binary()}}
| {read_objects_response, {ok, Resp :: [read_result()]} | {error, Reason :: error_code()}}
| {operation_response, ok | {error, Reason :: error_code()}}
| {get_connection_descriptor_resp, {ok, Descriptor :: binary()} | {error, Reason :: error_code()}}.

-type response_in() ::
  {error_response, {ErrorCode :: error_code(), Message :: binary()}}
| {start_transaction_response, {ok, TxId :: binary()}}
| {commit_response, {ok, CommitTime :: binary()}| {error, Reason :: error_code()}}
| {static_read_objects_response, {Results :: [read_result_in()], CommitTime :: binary()}}
| {read_objects_response, {ok, Resp :: [read_result_in()]} | {error, Reason :: error_code()}}
| {operation_response, ok | {error, Reason :: error_code()}}
| {get_connection_descriptor_resp, {ok, Descriptor :: binary()} | {error, Reason :: error_code()}}.


-type sendable() ::
  #'ApbErrorResp'{}
| #'ApbStartTransaction'{}
| #'ApbStartTransactionResp'{}
| #'ApbAbortTransaction'{}
| #'ApbCommitTransaction'{}
| #'ApbCommitResp'{}
| #'ApbUpdateObjects'{}
| #'ApbStaticUpdateObjects'{}
| #'ApbStaticReadObjects'{}
| #'ApbStaticReadObjectsResp'{}
| #'ApbReadObjects'{}
| #'ApbReadObjectsResp'{}
| #'ApbOperationResp'{}
| #'ApbCreateDC'{}
| #'ApbGetConnectionDescriptor'{}
| #'ApbGetConnectionDescriptorResp'{}
| #'ApbConnectToDCs'{}
.

-define(ASSERT_BINARY(X), case is_binary(X) of true -> ok; false -> throw({not_binary, X}) end).
-define(ASSERT_ALL_BINARY(Xs), [?ASSERT_BINARY(X) || X <- Xs]).

-spec decode_request(binary()) -> request().
decode_request(Data) ->
    <<MsgCode:8, MsgData/binary>> = Data,
    decode(MsgCode, MsgData).

-spec decode_response(binary()) -> response_out().
decode_response(Data) ->
    <<MsgCode:8, MsgData/binary>> = Data,
    decode(MsgCode, MsgData).

-spec encode_request(request()) -> iolist().
encode_request(Data) ->
    encode(Data).

-spec encode_response(response_in()) -> iolist().
encode_response(Data) ->
    TransformReadResponse = case Data of
        {static_read_objects_response, {Results, CommitTime}} ->
            TransformedResults = [ {encode_crdt_type(Type), Value} || {{_Key, Type, _Bucket}, Value} <- Results],
            {static_read_objects_response, {TransformedResults, CommitTime}};
        {read_objects_response, {ok, Results}} ->
            TransformedResults = [ {encode_crdt_type(Type), Value} || {{_Key, Type, _Bucket}, Value} <- Results],
            {read_objects_response, {ok, TransformedResults}};
        _ -> Data
    end,
    encode(TransformReadResponse).

-type message() :: term().


message_type_to_code('ApbErrorResp')             -> 0;
message_type_to_code('ApbRegUpdate')             -> 107;
message_type_to_code('ApbGetRegResp')            -> 108;
message_type_to_code('ApbCounterUpdate')         -> 109;
message_type_to_code('ApbGetCounterResp')        -> 110;
message_type_to_code('ApbOperationResp')         -> 111;
message_type_to_code('ApbSetUpdate')             -> 112;
message_type_to_code('ApbGetSetResp')            -> 113;
message_type_to_code('ApbTxnProperties')         -> 114;
message_type_to_code('ApbBoundObject')           -> 115;
message_type_to_code('ApbReadObjects')           -> 116;
message_type_to_code('ApbUpdateOp')              -> 117;
message_type_to_code('ApbUpdateObjects')         -> 118;
message_type_to_code('ApbStartTransaction')      -> 119;
message_type_to_code('ApbAbortTransaction')      -> 120;
message_type_to_code('ApbCommitTransaction')     -> 121;
message_type_to_code('ApbStaticUpdateObjects')   -> 122;
message_type_to_code('ApbStaticReadObjects')     -> 123;
message_type_to_code('ApbStartTransactionResp')  -> 124;
message_type_to_code('ApbReadObjectResp')        -> 125;
message_type_to_code('ApbReadObjectsResp')       -> 126;
message_type_to_code('ApbCommitResp')            -> 127;
message_type_to_code('ApbStaticReadObjectsResp') -> 128;
message_type_to_code('ApbCreateDC')                    -> 129;
message_type_to_code('ApbConnectToDCs')                -> 130;
message_type_to_code('ApbGetConnectionDescriptor')     -> 131;
message_type_to_code('ApbGetConnectionDescriptorResp') -> 132.

message_code_to_type(0)   -> 'ApbErrorResp';
message_code_to_type(107) -> 'ApbRegUpdate';
message_code_to_type(108) -> 'ApbGetRegResp';
message_code_to_type(109) -> 'ApbCounterUpdate';
message_code_to_type(110) -> 'ApbGetCounterResp';
message_code_to_type(111) -> 'ApbOperationResp';
message_code_to_type(112) -> 'ApbSetUpdate';
message_code_to_type(113) -> 'ApbGetSetResp';
message_code_to_type(114) -> 'ApbTxnProperties';
message_code_to_type(115) -> 'ApbBoundObject';
message_code_to_type(116) -> 'ApbReadObjects';
message_code_to_type(117) -> 'ApbUpdateOp';
message_code_to_type(118) -> 'ApbUpdateObjects';
message_code_to_type(119) -> 'ApbStartTransaction';
message_code_to_type(120) -> 'ApbAbortTransaction';
message_code_to_type(121) -> 'ApbCommitTransaction';
message_code_to_type(122) -> 'ApbStaticUpdateObjects';
message_code_to_type(123) -> 'ApbStaticReadObjects';
message_code_to_type(124) -> 'ApbStartTransactionResp';
message_code_to_type(125) -> 'ApbReadObjectResp';
message_code_to_type(126) -> 'ApbReadObjectsResp';
message_code_to_type(127) -> 'ApbCommitResp';
message_code_to_type(128) -> 'ApbStaticReadObjectsResp';
message_code_to_type(129) -> 'ApbCreateDC';
message_code_to_type(130) -> 'ApbConnectToDCs';
message_code_to_type(131) -> 'ApbGetConnectionDescriptor';
message_code_to_type(132) -> 'ApbGetConnectionDescriptorResp'.

-spec encode(message()) -> iolist().
encode(Msg) ->
    X = encode_message(Msg),
    encode_msg(X).



-spec decode(integer(), binary()) -> any().
decode(Code, Msg) ->
    decode_message(decode_msg(Code, Msg)).

-spec encode_msg(sendable()) -> iolist().
encode_msg(Msg) ->
  MsgType = element(1, Msg),
  [message_type_to_code(MsgType), [antidote_pb:encode_msg(Msg)]].

-spec decode_msg(integer(), binary()) -> sendable().
decode_msg(Code, Msg) ->
  MsgType = message_code_to_type(Code),
  antidote_pb:decode_msg(Msg, MsgType).

-spec encode_message(message()) -> sendable().
encode_message({start_transaction, Clock, Properties}) ->
  encode_start_transaction(Clock, Properties);
encode_message({abort_transaction, TxId}) ->
  encode_abort_transaction(TxId);
encode_message({commit_transaction, TxId}) ->
  encode_commit_transaction(TxId);
encode_message({update_objects, Updates, TxId}) ->
  encode_update_objects(Updates, TxId);
encode_message({static_update_objects, Clock, Properties, Updates}) ->
  encode_static_update_objects(Clock, Properties, Updates);
encode_message({static_read_objects, Clock, Properties, Objects}) ->
  encode_static_read_objects(Clock, Properties, Objects);
encode_message({read_objects, Objects, TxId}) ->
  encode_read_objects(Objects, TxId);
encode_message({error_response, {ErrorCode, Message}}) ->
  encode_error_resp(ErrorCode, Message);
encode_message({start_transaction_response, Resp}) ->
  encode_start_transaction_response(Resp);
encode_message({commit_response, Resp}) ->
  encode_commit_response(Resp);
encode_message({static_read_objects_response, Resp}) ->
  encode_static_read_objects_response(Resp);
encode_message({read_objects_response, Resp}) ->
  encode_read_objects_response(Resp);
encode_message({operation_response, Resp}) ->
  encode_operation_response(Resp);
encode_message(get_connection_descriptor) ->
  encode_get_connection_descriptor();
encode_message({get_connection_descriptor_resp, Resp}) ->
  encode_get_connection_descriptor_resp(Resp);
encode_message({create_dc, Nodes}) ->
  encode_create_dc(Nodes);
encode_message({connect_to_dcs, Descriptors}) ->
  encode_connect_to_dcs(Descriptors).

-spec decode_message(sendable()) -> message().
decode_message(#'ApbStartTransaction'{properties = Properties, timestamp = Clock}) ->
  {start_transaction, Clock, decode_txn_properties(Properties)};
decode_message(#'ApbAbortTransaction'{transaction_descriptor = TxId}) ->
  {abort_transaction, TxId};
decode_message(#'ApbCommitTransaction'{transaction_descriptor = TxId}) ->
  {commit_transaction, TxId};
decode_message(#'ApbUpdateObjects'{updates = Updates, transaction_descriptor = TxId}) ->
  {update_objects, [decode_update_op(U) || U <- Updates], TxId};
decode_message(#'ApbStaticUpdateObjects'{updates = Updates, transaction = Tx}) ->
  Clock = Tx#'ApbStartTransaction'.timestamp,
  Properties = decode_txn_properties(Tx#'ApbStartTransaction'.properties),
  {static_update_objects, Clock, Properties, [decode_update_op(U) || U <- Updates]};
decode_message(#'ApbStaticReadObjects'{objects = Objects, transaction = Tx}) ->
  Clock = Tx#'ApbStartTransaction'.timestamp,
  Properties = decode_txn_properties(Tx#'ApbStartTransaction'.properties),
  {static_read_objects, Clock, Properties, [decode_bound_object(O) || O <- Objects]};
decode_message(#'ApbReadObjects'{boundobjects = Objects, transaction_descriptor = TxId}) ->
  {read_objects, [decode_bound_object(O) || O <- Objects], TxId};

decode_message(#'ApbCreateDC'{nodes = Nodes}) ->
  {create_dc, [binary_to_atom(N, utf8) || N <- Nodes]};
decode_message(#'ApbGetConnectionDescriptor'{}) ->
  get_connection_descriptor;
decode_message(#'ApbGetConnectionDescriptorResp'{success = false, descriptor = _D, errorcode = E}) ->
  {get_connection_descriptor_resp, {error, decode_error_code(E)}};
decode_message(#'ApbGetConnectionDescriptorResp'{success = true, descriptor = Descriptor, errorcode = _E}) ->
  {get_connection_descriptor_resp, {ok, Descriptor}};
decode_message(#'ApbConnectToDCs'{descriptors = Descriptors}) ->
  {connect_to_dcs, Descriptors};

decode_message(#'ApbErrorResp'{errcode = ErrorCode, errmsg = Message}) ->
  {error_response, {decode_error_code(ErrorCode), Message}};
decode_message(#'ApbStartTransactionResp'{success = Success, transaction_descriptor = TxId, errorcode = ErrorCode}) ->
  Resp = case Success of
    true -> {ok, TxId};
    false -> {error, decode_error_code(ErrorCode)}
  end,
  {start_transaction_response, Resp};
decode_message(#'ApbCommitResp'{success = Success, errorcode = ErrorCode, commit_time = Time}) ->
  Resp = case Success of
    true -> {ok, Time};
    false -> {error, decode_error_code(ErrorCode)}
  end,
  {commit_response, Resp};
decode_message(#'ApbStaticReadObjectsResp'{
    objects = #'ApbReadObjectsResp'{objects = Objects},
    committime = #'ApbCommitResp'{commit_time = Time}}) ->
  Results = [decode_read_object_resp(O) || O <- Objects],
  {static_read_objects_response, {Results, Time}};
decode_message(#'ApbReadObjectsResp'{success = Success, errorcode = ErrorCode, objects = Objects}) ->
  case Success of
    true ->
      Resp = [decode_read_object_resp(O) || O <- Objects],
      {read_objects_response, {ok, Resp}};
    false ->
      {read_objects_response, {error, decode_error_code(ErrorCode)}}
  end;
decode_message(#'ApbOperationResp'{success = S, errorcode = E}) ->
  case S of
    true ->
      {operation_response, ok};
    false ->
      {operation_response, {error, decode_error_code(E)}}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%
% error codes

encode_error_code(unknown) -> 0;
encode_error_code(timeout) -> 1;
encode_error_code(no_permissions) -> 2;
encode_error_code(aborted) -> 3;
encode_error_code({error_code, X})  -> X.

decode_error_code(0) -> unknown;
decode_error_code(1) -> timeout;
decode_error_code(2) -> no_permissions;
decode_error_code(3) -> aborted;
decode_error_code(X) -> {error_code, X}.


encode_error_resp(ErrorCode, Message) ->
  #'ApbErrorResp'{errcode = encode_error_code(ErrorCode), errmsg = Message}.

%%%%%%%%%%%%%%%%%%%%%%%
% Transactions

encode_start_transaction(Clock, Properties) ->
      #'ApbStartTransaction'{timestamp = Clock,
        properties                     = encode_txn_properties(Properties)}.

encode_commit_transaction(TxId) ->
  #'ApbCommitTransaction'{transaction_descriptor = TxId}.

encode_abort_transaction(TxId) ->
  #'ApbAbortTransaction'{transaction_descriptor = TxId}.


encode_txn_properties(_Props) ->
  %%TODO: Add more property parameters
  #'ApbTxnProperties'{}.

decode_txn_properties(_Properties) ->
  %%TODO: Add more property parameters
  [].


%%%%%%%%%%%%%%%%%%%%%
%% Updates

% bound objects

encode_bound_object({Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket).
encode_bound_object(Key, Type, Bucket) ->
  #'ApbBoundObject'{key = Key, type = encode_type(Type), bucket = Bucket}.

decode_bound_object(Obj) ->
  #'ApbBoundObject'{key = Key, type = Type, bucket = Bucket} = Obj,
  {Key, decode_type(Type), Bucket}.


% static_update_objects

encode_static_update_objects(Clock, Properties, Updates) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #'ApbStaticUpdateObjects'{transaction = EncTransaction,
    updates                             = EncUpdates}.


decode_update_op(Obj) ->
  #'ApbUpdateOp'{boundobject = Object, operation = Operation} = Obj,
  {Op, OpParam} = decode_update_operation(Operation),
  {decode_bound_object(Object), Op, OpParam}.


encode_update_op({Object, Op, Param}) ->
  encode_update_op(Object, Op, Param).
encode_update_op(Object, Op, Param) ->
  {_Key, Type, _Bucket} = Object,
  EncObject = encode_bound_object(Object),
  Operation = encode_update_operation(Type, {Op, Param}),
  #'ApbUpdateOp'{boundobject = EncObject, operation = Operation}.

encode_update_objects(Updates, TxId) ->
  EncUpdates = lists:map(fun(Update) ->
    encode_update_op(Update) end,
    Updates),
  #'ApbUpdateObjects'{updates = EncUpdates, transaction_descriptor = TxId}.


%%%%%%%%%%%%%%%%%%%%%%%%
%% Responses

encode_start_transaction_response({error, Reason}) ->
  #'ApbStartTransactionResp'{success = false, errorcode = encode_error_code(Reason)};
encode_start_transaction_response({ok, TxId}) ->
  ?ASSERT_BINARY(TxId),
  #'ApbStartTransactionResp'{success = true, transaction_descriptor = TxId}.

encode_operation_response({error, Reason}) ->
  #'ApbOperationResp'{success = false, errorcode = encode_error_code(Reason)};
encode_operation_response(ok) ->
  #'ApbOperationResp'{success = true}.

encode_commit_response({error, Reason}) ->
  #'ApbCommitResp'{success = false, errorcode = encode_error_code(Reason)};

encode_commit_response({ok, CommitTime}) ->
  ?ASSERT_BINARY(CommitTime),
  #'ApbCommitResp'{success = true, commit_time = CommitTime}.

%%%%%%%%%%%%%%%%%%%%%%
%% Reading objects

encode_static_read_objects_response({Results, CommitTime}) ->
  ?ASSERT_BINARY(CommitTime),
  #'ApbStaticReadObjectsResp'{
    objects    =  encode_read_objects_response({ok, Results}),
    committime =  encode_commit_response({ok, CommitTime})}.

encode_read_objects_response({ok, Results}) ->
  #'ApbReadObjectsResp'{
    success    = true,
    objects    = [ encode_read_object_resp(Type, Value) || {Type, Value} <- Results ]};
encode_read_objects_response({error, Reason}) ->
  #'ApbReadObjectsResp'{
    success    = false,
    errorcode = encode_error_code(Reason)}.

encode_static_read_objects(Clock, Properties, Objects) ->
  ?ASSERT_BINARY(Clock),
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #'ApbStaticReadObjects'{transaction = EncTransaction,
    objects                           = EncObjects}.

encode_read_objects(Objects, TxId) ->
  BoundObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #'ApbReadObjects'{boundobjects = BoundObjects, transaction_descriptor = TxId}.

%%%%%%%%%%%%%%%%%%%
%% Crdt types

%%COUNTER = 3;
%%ORSET = 4;
%%LWWREG = 5;
%%MVREG = 6;
%%INTEGER = 7;
%%GMAP = 8;
%%AWMAP = 9;
%%RWSET = 10;

encode_type(antidote_crdt_counter_pn)   -> 'COUNTER';
encode_type(antidote_crdt_counter_fat)  -> 'FATCOUNTER';
encode_type(antidote_crdt_counter_b)    -> 'BCOUNTER';
encode_type(antidote_crdt_set_aw)       -> 'ORSET';
encode_type(antidote_crdt_set_rw)       -> 'RWSET';
encode_type(antidote_crdt_register_lww) -> 'LWWREG';
encode_type(antidote_crdt_register_mv)  -> 'MVREG';
encode_type(antidote_crdt_map_go)       -> 'GMAP';
encode_type(antidote_crdt_map_rr)       -> 'RRMAP';
encode_type(antidote_crdt_flag_ew)      -> 'FLAG_EW';
encode_type(antidote_crdt_flag_dw)      -> 'FLAG_DW';
encode_type(T)                          -> erlang:error({unknown_crdt_type, T}).


decode_type('COUNTER')    -> antidote_crdt_counter_pn;
decode_type('FATCOUNTER') -> antidote_crdt_counter_fat;
decode_type('BCOUNTER')   -> antidote_crdt_counter_b;
decode_type('ORSET')      -> antidote_crdt_set_aw;
decode_type('LWWREG')     -> antidote_crdt_register_lww;
decode_type('MVREG')      -> antidote_crdt_register_mv;
decode_type('GMAP')       -> antidote_crdt_map_go;
decode_type('RWSET')      -> antidote_crdt_set_rw;
decode_type('RRMAP')      -> antidote_crdt_map_rr;
decode_type('FLAG_EW')    -> antidote_crdt_flag_ew;
decode_type('FLAG_DW')    -> antidote_crdt_flag_dw;
decode_type(T)            -> erlang:error({unknown_crdt_type_protobuf, T}).


%%%%%%%%%%%%%%%%%%%%%%
% CRDT operations


encode_update_operation(_Type, {reset, {}}) ->
  #'ApbUpdateOperation'{resetop = #'ApbCrdtReset'{}};
encode_update_operation(antidote_crdt_counter_pn, Op_Param) ->
  #'ApbUpdateOperation'{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_counter_fat, Op_Param) ->
  #'ApbUpdateOperation'{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_counter_b, Op_Param) ->
  #'ApbUpdateOperation'{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_set_aw, Op_Param) ->
  #'ApbUpdateOperation'{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_set_rw, Op_Param) ->
  #'ApbUpdateOperation'{setop = encode_set_update(Op_Param)};
encode_update_operation(antidote_crdt_register_lww, Op_Param) ->
  #'ApbUpdateOperation'{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_register_mv, Op_Param) ->
  #'ApbUpdateOperation'{regop = encode_reg_update(Op_Param)};
encode_update_operation(antidote_crdt_map_go, Op_Param) ->
  #'ApbUpdateOperation'{mapop = encode_map_update(Op_Param)};
encode_update_operation(antidote_crdt_map_rr, Op_Param) ->
  #'ApbUpdateOperation'{mapop = encode_map_update(Op_Param)};
encode_update_operation(antidote_crdt_flag_ew, Op_Param) ->
  #'ApbUpdateOperation'{flagop = encode_flag_update(Op_Param)};
encode_update_operation(antidote_crdt_flag_dw, Op_Param) ->
  #'ApbUpdateOperation'{flagop = encode_flag_update(Op_Param)};
encode_update_operation(Type, _Op) ->
  throw({invalid_type, Type}).

decode_update_operation(#'ApbUpdateOperation'{counterop = Op}) when Op /= undefined ->
  decode_counter_update(Op);
decode_update_operation(#'ApbUpdateOperation'{setop = Op}) when Op /= undefined ->
  decode_set_update(Op);
decode_update_operation(#'ApbUpdateOperation'{regop = Op}) when Op /= undefined ->
  decode_reg_update(Op);
decode_update_operation(#'ApbUpdateOperation'{mapop = Op}) when Op /= undefined ->
  decode_map_update(Op);
decode_update_operation(#'ApbUpdateOperation'{flagop = Op}) when Op /= undefined ->
  decode_flag_update(Op);
decode_update_operation(#'ApbUpdateOperation'{resetop = #'ApbCrdtReset'{}}) ->
  {reset, {}}.


encode_crdt_type(antidote_crdt_register_lww) ->
    reg;
encode_crdt_type(antidote_crdt_register_mv) ->
    mvreg;
encode_crdt_type(antidote_crdt_counter_pn) ->
    counter;
encode_crdt_type(antidote_crdt_counter_fat) ->
    counter;
encode_crdt_type(antidote_crdt_set_aw) ->
    set;
encode_crdt_type(antidote_crdt_set_rw) ->
    set;
encode_crdt_type(antidote_crdt_map_go) ->
    map;
encode_crdt_type(antidote_crdt_map_rr) ->
    map;
encode_crdt_type(antidote_crdt_flag_ew) ->
    flag;
encode_crdt_type(antidote_crdt_flag_dw) ->
    flag.

encode_read_object_resp(reg, Val) ->
  #'ApbReadObjectResp'{reg = #'ApbGetRegResp'{value = Val}};
encode_read_object_resp(mvreg, Val) ->
  #'ApbReadObjectResp'{mvreg = #'ApbGetMVRegResp'{values = Val}};
encode_read_object_resp(counter, Val) ->
  #'ApbReadObjectResp'{counter = #'ApbGetCounterResp'{value = Val}};
encode_read_object_resp(set, Val) ->
  #'ApbReadObjectResp'{set = #'ApbGetSetResp'{value = Val}};
encode_read_object_resp(map, Val) ->
  #'ApbReadObjectResp'{map = encode_map_get_resp(Val)};
encode_read_object_resp(flag, Val) ->
  #'ApbReadObjectResp'{flag = #'ApbGetFlagResp'{value = Val}}.

decode_read_object_resp(#'ApbReadObjectResp'{counter = #'ApbGetCounterResp'{value = Val}}) ->
  {counter, Val};
decode_read_object_resp(#'ApbReadObjectResp'{set = #'ApbGetSetResp'{value = Val}}) ->
  {set, Val};
decode_read_object_resp(#'ApbReadObjectResp'{reg = #'ApbGetRegResp'{value = Val}}) ->
  {reg, Val};
decode_read_object_resp(#'ApbReadObjectResp'{mvreg = #'ApbGetMVRegResp'{values = Vals}}) ->
  {mvreg, Vals};
decode_read_object_resp(#'ApbReadObjectResp'{map = MapResp = #'ApbGetMapResp'{}}) ->
  {map, decode_map_get_resp(MapResp)};
decode_read_object_resp(#'ApbReadObjectResp'{flag = #'ApbGetFlagResp'{value = Val}}) ->
  {flag, Val}.

% set updates

encode_set_update({add, Elem}) ->
  ?ASSERT_BINARY(Elem),
  #'ApbSetUpdate'{optype = 'ADD', adds = [Elem]};
encode_set_update({add_all, Elems}) ->
  ?ASSERT_ALL_BINARY(Elems),
  #'ApbSetUpdate'{optype = 'ADD', adds = Elems};
encode_set_update({remove, Elem}) ->
  ?ASSERT_BINARY(Elem),
  #'ApbSetUpdate'{optype = 'REMOVE', rems = [Elem]};
encode_set_update({remove_all, Elems}) ->
  ?ASSERT_ALL_BINARY(Elems),
  #'ApbSetUpdate'{optype = 'REMOVE', rems = Elems}.

decode_set_update(Update) ->
  #'ApbSetUpdate'{optype = OpType, adds = A, rems = R} = Update,
  case OpType of
    'ADD' ->
      case A of
        undefined -> [];
        [Elem] -> {add, Elem};
        AddElems when is_list(AddElems) -> {add_all, AddElems}
      end;
    'REMOVE' ->
      case R of
        undefined -> [];
        [Elem] -> {remove, Elem};
        Elems when is_list(Elems) -> {remove_all, Elems}
      end
  end.

% counter updates

encode_counter_update({increment, Amount}) ->
  #'ApbCounterUpdate'{inc = Amount};
encode_counter_update({decrement, Amount}) ->
  #'ApbCounterUpdate'{inc = -Amount}.


decode_counter_update(Update) ->
  #'ApbCounterUpdate'{inc = I} = Update,
  case I of
    undefined -> {increment, 1};
    I -> {increment, I} % negative value for I indicates decrement
  end.


% register updates

encode_reg_update(Update) ->
  {assign, Value} = Update,
  #'ApbRegUpdate'{value = Value}.


decode_reg_update(Update) ->
  #'ApbRegUpdate'{value = Value} = Update,
  {assign, Value}.

% flag updates

encode_flag_update({enable, {}}) ->
  #'ApbFlagUpdate'{value = true};
encode_flag_update({disable, {}}) ->
  #'ApbFlagUpdate'{value = false}.

decode_flag_update(#'ApbFlagUpdate'{value = true}) ->
  {enable, {}};
decode_flag_update(#'ApbFlagUpdate'{value = false}) ->
  {disable, {}}.

% map updates

encode_map_update({update, Ops}) when is_list(Ops) ->
  encode_map_update({batch, {Ops, []}});
encode_map_update({update, Op}) ->
  encode_map_update({batch, {[Op], []}});
encode_map_update({remove, Keys}) when is_list(Keys) ->
  encode_map_update({batch, {[], Keys}});
encode_map_update({remove, Key}) ->
  encode_map_update({batch, {[], [Key]}});
encode_map_update({batch, {Updates, RemovedKeys}}) ->
  UpdatesEnc = [encode_map_nested_update(U) || U <- Updates],
  RemovedKeysEnc = [encode_map_key(K) || K <- RemovedKeys],
  #'ApbMapUpdate'{updates = UpdatesEnc, removedKeys = RemovedKeysEnc}.


decode_map_update(#'ApbMapUpdate'{updates = [Update], removedKeys = []}) ->
  {update, decode_map_nested_update(Update)};
decode_map_update(#'ApbMapUpdate'{updates = Updates, removedKeys = []}) ->
  {update, [decode_map_nested_update(U) || U <- Updates]};
decode_map_update(#'ApbMapUpdate'{updates = [], removedKeys = [Key]}) ->
  {remove, decode_map_key(Key)};
decode_map_update(#'ApbMapUpdate'{updates = [], removedKeys = Keys}) ->
  {remove, [decode_map_key(K) || K <- Keys]};
decode_map_update(#'ApbMapUpdate'{updates = Updates, removedKeys = Keys}) ->
  {batch, {[decode_map_nested_update(U) || U <- Updates], [decode_map_key(K) || K <- Keys]}}.


encode_map_nested_update({{Key, Type}, Update}) ->
  #'ApbMapNestedUpdate'{
    key    = encode_map_key({Key, Type}),
    update = encode_update_operation(Type, Update)
  }.

decode_map_nested_update(#'ApbMapNestedUpdate'{key = KeyEnc, update = UpdateEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  Update = decode_update_operation(UpdateEnc),
  {{Key, Type}, Update}.

encode_map_key({Key, Type}) ->
  ?ASSERT_BINARY(Key),
  #'ApbMapKey'{
    key  = Key,
    type = encode_type(Type)
  }.

decode_map_key(#'ApbMapKey'{key = Key, type = Type}) ->
  {Key, decode_type(Type)}.

% map responses

encode_map_get_resp(Entries) ->
  #'ApbGetMapResp'{entries = [encode_map_entry(E) || E <- Entries]}.

decode_map_get_resp(#'ApbGetMapResp'{entries = Entries}) ->
  [decode_map_entry(E) || E <- Entries].

encode_map_entry({{Key, Type}, Val}) ->
    Value = encode_read_object_resp(encode_crdt_type(Type), Val),
  #'ApbMapEntry'{
    key   = encode_map_key({Key, Type}),
    value = Value
  }.

decode_map_entry(#'ApbMapEntry'{key = KeyEnc, value = ValueEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  {_Tag, Value} = decode_read_object_resp(ValueEnc),
  {{Key, Type}, Value}.


%% Cluster Management

encode_create_dc(Nodes) ->
    #'ApbCreateDC'{nodes = [if
      is_atom(N) -> atom_to_binary(N, utf8);
      is_list(N) -> list_to_binary(N);
      is_binary(N) -> N;
      true -> throw({invalid_node_value, N})
    end || N <- Nodes]}.

encode_get_connection_descriptor() ->
    #'ApbGetConnectionDescriptor'{}.

encode_get_connection_descriptor_resp({error, Reason}) ->
    #'ApbGetConnectionDescriptorResp'{
        success = false,
        errorcode = encode_error_code(Reason)
    };
encode_get_connection_descriptor_resp({ok, Descriptor}) ->
    #'ApbGetConnectionDescriptorResp'{
        success = true,
        descriptor = Descriptor
    }.

encode_connect_to_dcs(Descriptors) ->
    #'ApbConnectToDCs'{descriptors = Descriptors}.

-ifdef(TEST).

check_response(Input, Output) ->
  Data = encode_response(Input),
  Result = decode_response(iolist_to_binary(Data)),
  ?assertEqual(Output, Result).

check_response(Input) ->
  check_response(Input, Input).

check_request(Input, Output) ->
  Data = encode_request(Input),
  Result = decode_request(iolist_to_binary(Data)),
  ?assertEqual(Output, Result).

check_request(Input) ->
  check_request(Input, Input).



%% Tests encode and decode
start_test() ->
  check_request({start_transaction, <<"opaque_binary">>, []}),
  check_response({start_transaction_response, {ok, <<"opaque_binary">>}}).

commit_test() ->
  check_request({commit_transaction, <<"opaque_binary">>}),
  check_response({commit_response, {ok, <<"opaque_binary">>}}).

abort_test() ->
  check_request({abort_transaction, <<"opaque_binary">>}).

read_test() ->
  Objects = [{<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
    {<<"key2">>, antidote_crdt_set_aw, <<"bucket2">>}],
  check_request({read_objects, Objects, <<"opaque_binary">>}),
  check_request({static_read_objects, <<"opaque_binary">>, [], Objects}),

  Map = [{<<"key1">>, antidote_crdt_map_rr, <<"bucket1">>}],
  check_request({read_objects, Map, <<"opaque_binary">>}),

  InMap = [{{<<"key1">>, antidote_crdt_map_rr, <<"bucket1">>}, [{{<<"mapkey1">>, antidote_crdt_counter_pn}, 7}]}],
  OutMap = [{map, [{{<<"mapkey1">>, antidote_crdt_counter_pn}, 7}]}],

  InputMap = {read_objects_response, {ok, InMap}},
  OutputMap = {read_objects_response, {ok, OutMap}},
  check_response(InputMap, OutputMap),

  In = [{{<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>}, 1},
         {{<<"key2">>, antidote_crdt_set_aw, <<"bucket2">>}, [<<"a">>, <<"b">>]},
         {{<<"key3">>, antidote_crdt_flag_dw, <<"bucket3">>}, true},
         {{<<"key4">>, antidote_crdt_register_lww, <<"bucket4">>}, <<"c">>},
         {{<<"key4">>, antidote_crdt_register_mv, <<"bucket4">>}, [<<"d">>, <<"e">>]}
         ],
  Out = [{counter, 1}, {set, [<<"a">>, <<"b">>]}, {flag, true}, {reg, <<"c">>}, {mvreg, [<<"d">>, <<"e">>]}],
  Input = {read_objects_response, {ok, In}},
  Output = {read_objects_response, {ok, Out}},
  check_response(Input, Output),

  InputStatic = {static_read_objects_response, {In, <<"opaque_binary">>}},
  OutputStatic = {static_read_objects_response, {Out, <<"opaque_binary">>}},
  check_response(InputStatic, OutputStatic).

update_test() ->
  Updates = [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, increment, 1},
    {{<<"K2">>, antidote_crdt_counter_fat, <<"B">>}, increment, 1},
    {{<<"K3">>, antidote_crdt_set_aw, <<"B">>}, add, <<"3">>},
    {{<<"K4">>, antidote_crdt_set_aw, <<"B">>}, add_all, [<<"5">>, <<"6">>]},
    {{<<"K5">>, antidote_crdt_set_rw, <<"B">>}, remove, <<"3">>},
    {{<<"K5">>, antidote_crdt_set_rw, <<"B">>}, remove_all, [<<"1">>, <<"2">>]},
    {{<<"K6">>, antidote_crdt_flag_dw, <<"B">>}, enable, {}},
    {{<<"K7">>, antidote_crdt_flag_ew, <<"B">>}, disable, {}},
    {{<<"K8">>, antidote_crdt_register_lww, <<"B">>}, assign, <<"Hello">>},
    {{<<"K9">>, antidote_crdt_register_mv, <<"B">>}, assign, <<"World">>},
    {{<<"K6">>, antidote_crdt_flag_ew, <<"B">>}, disable, {}}
  ],
  check_request({update_objects, Updates, <<"opaque_binary">>}),
  check_request({static_update_objects, <<"opaque_binary">>, [], Updates}),

  check_request({update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, decrement, 10}], <<"opaque_binary">>},
    {update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, increment, -10}], <<"opaque_binary">>}),

  check_request({update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, increment, undefined}], <<"opaque_binary">>},
    {update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, increment, 1}], <<"opaque_binary">>}),

  check_request({update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, reset, {}}], <<"opaque_binary">>},
    {update_objects, [{{<<"K1">>, antidote_crdt_counter_pn, <<"B">>}, reset, {}}], <<"opaque_binary">>}),

  MapUpdates = [
     {{<<"M">>, antidote_crdt_map_go, <<"B">>}, update, [{{<<"A">>, antidote_crdt_counter_pn}, {increment, 5}}, {{<<"B">>, antidote_crdt_counter_pn}, {increment, 5}}]},
     {{<<"M">>, antidote_crdt_map_rr, <<"B">>}, remove, [{<<"A">>, antidote_crdt_counter_pn}, {<<"B">>, antidote_crdt_set_aw}]}
  ],
  check_request({update_objects, MapUpdates, <<"opaque_binary">>}),

  check_response({operation_response, ok}).

error_messages_test() ->
  check_response({start_transaction_response, {error, unknown}}),
  check_response({commit_response, {error, unknown}}),
  check_response({operation_response, {error, unknown}}),
  check_response({read_objects_response, {error, unknown}}),
  check_response({get_connection_descriptor_resp, {error, unknown}}),
  check_response({error_response, {unknown, <<"Message">>}}),
  check_response({error_response, {timeout, <<"Message">>}}),
  check_response({error_response, {no_permissions, <<"Message">>}}),
  check_response({error_response, {aborted, <<"Message">>}}),
  check_response({error_response, {{error_code, 123}, <<"Message">>}}, {error_response, {{error_code, 123}, <<"Message">>}}).

dc_management_test() ->
    Nodes = [antidote@host1, antidote@host2],
    check_request({create_dc, Nodes}),
    check_request(get_connection_descriptor),

    Descriptor = <<"some_opaque_binary_descriptor">>,
    check_response({get_connection_descriptor_resp, {ok, Descriptor}}),

    Descriptors = [<<"opaque_binary_descriptor1">>, <<"opaque_binary_descriptor2">>, <<"opaque_binary_descriptor3">>],
    check_request({connect_to_dcs, Descriptors}),
    ok.

-endif.
