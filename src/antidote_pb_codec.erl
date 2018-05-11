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

-export([encode/2,
  decode/2,
  decode_response/1, encode_read_objects/2, decode_bound_object/1, encode_update_objects/2, decode_update_op/1, encode_msg/1, decode_msg/2, encode_start_transaction/2, encode_txn_properties/1, encode_abort_transaction/1, encode_commit_transaction/1, encode_update_op/3, encode_static_update_objects/3, encode_bound_object/3, encode_type/1, encode_static_read_objects/3, encode_start_transaction_response/1, encode_operation_response/1, encode_commit_response/1, encode_read_objects_response/1, encode_read_object_resp/1, encode_static_read_objects_response/2, encode_error_code/1, decode_txn_properties/1, decode_type/1, decode_error_code/1, encode_error_resp/2, decode_error_resp/1, encode_message/1, decode_message/1]).

-define(TYPE_COUNTER, counter).
-define(TYPE_SET, set).


-define(assert_binary(X), case is_binary(X) of true -> ok; false -> throw({not_binary, X}) end).
-define(assert_all_binary(Xs), [?assert_binary(X) || X <- Xs]).

% encoding on wire (used to be decode_msg and encode_msg)

% these are all top-level messages which can be sent on the wire
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
.

-type binary_message_with_code() :: {pos_integer(), iolist()}.

-type request() ::
    {start_transaction, {Clock::binary(), Properties::list()}}
  | {abort_transaction, TxId::binary()}
  | {commit_transaction, TxId::binary()}
  | {update_objects, {Updates::list(), TxId::binary()}}
  | {static_update_objects, {Clock::binary(), Properties::list(), Updates::list()}}
  | {static_read_objects, {Clock::binary(), Properties::list(), Objects::list()}}
  | {read_objects, {Objects::list(), TxId::binary()}}.

-type response() ::
    {error_resp, {ErrorCode::integer(), Message::binary()}}
  | {start_transaction_resp, Resp::any()}
  | {commit_resp, Resp::any()}
  | {static_read_objects_resp, {ok, Results::list(), CommitTime::binary()}}
  | {read_objects_resp, Resp::list()}.

-type message() :: request() | response().




messageTypeToCode('ApbErrorResp') -> 0;
messageTypeToCode('ApbRegUpdate') -> 107;
messageTypeToCode('ApbGetRegResp') -> 108;
messageTypeToCode('ApbCounterUpdate') -> 109;
messageTypeToCode('ApbGetCounterResp') -> 110;
messageTypeToCode('ApbOperationResp') -> 111;
messageTypeToCode('ApbSetUpdate') -> 112;
messageTypeToCode('ApbGetSetResp') -> 113;
messageTypeToCode('ApbTxnProperties') -> 114;
messageTypeToCode('ApbBoundObject') -> 115;
messageTypeToCode('ApbReadObjects') -> 116;
messageTypeToCode('ApbUpdateOp') -> 117;
messageTypeToCode('ApbUpdateObjects') -> 118;
messageTypeToCode('ApbStartTransaction') -> 119;
messageTypeToCode('ApbAbortTransaction') -> 120;
messageTypeToCode('ApbCommitTransaction') -> 121;
messageTypeToCode('ApbStaticUpdateObjects') -> 122;
messageTypeToCode('ApbStaticReadObjects') -> 123;
messageTypeToCode('ApbStartTransactionResp') -> 124;
messageTypeToCode('ApbReadObjectResp') -> 125;
messageTypeToCode('ApbReadObjectsResp') -> 126;
messageTypeToCode('ApbCommitResp') -> 127;
messageTypeToCode('ApbStaticReadObjectsResp') -> 128.

messageCodeToType(0) -> 'ApbErrorResp';
messageCodeToType(107) -> 'ApbRegUpdate';
messageCodeToType(108) -> 'ApbGetRegResp';
messageCodeToType(109) -> 'ApbCounterUpdate';
messageCodeToType(110) -> 'ApbGetCounterResp';
messageCodeToType(111) -> 'ApbOperationResp';
messageCodeToType(112) -> 'ApbSetUpdate';
messageCodeToType(113) -> 'ApbGetSetResp';
messageCodeToType(114) -> 'ApbTxnProperties';
messageCodeToType(115) -> 'ApbBoundObject';
messageCodeToType(116) -> 'ApbReadObjects';
messageCodeToType(117) -> 'ApbUpdateOp';
messageCodeToType(118) -> 'ApbUpdateObjects';
messageCodeToType(119) -> 'ApbStartTransaction';
messageCodeToType(120) -> 'ApbAbortTransaction';
messageCodeToType(121) -> 'ApbCommitTransaction';
messageCodeToType(122) -> 'ApbStaticUpdateObjects';
messageCodeToType(123) -> 'ApbStaticReadObjects';
messageCodeToType(124) -> 'ApbStartTransactionResp';
messageCodeToType(125) -> 'ApbReadObjectResp';
messageCodeToType(126) -> 'ApbReadObjectsResp';
messageCodeToType(127) -> 'ApbCommitResp';
messageCodeToType(128) -> 'ApbStaticReadObjectsResp'.


-spec encode_msg(sendable()) -> iolist().
encode_msg(Msg) ->
  MsgType = element(1, Msg),
  [messageTypeToCode(MsgType), [antidote_pb:encode_msg(Msg)]].


-spec decode_msg(integer(), binary()) -> sendable().
decode_msg(Code, Msg) ->
  MsgType = messageCodeToType(Code),
  antidote_pb:decode_msg(Msg, MsgType).


-spec encode_message(message()) -> sendable().
encode_message({start_transaction, {Clock, Properties}}) ->
  encode_start_transaction(Clock, Properties);
encode_message({abort_transaction, TxId}) ->
  encode_abort_transaction(TxId);
encode_message({commit_transaction, TxId}) ->
  encode_commit_transaction(TxId);
encode_message({update_objects, {Updates, TxId}}) ->
  encode_update_objects(Updates, TxId);
encode_message({static_update_objects, {Clock, Properties, Updates}}) ->
  encode_static_update_objects(Clock, Properties, Updates);
encode_message({static_read_objects, {Clock, Properties, Objects}}) ->
  encode_static_read_objects(Clock, Properties, Objects);
encode_message({read_objects, {Objects, TxId}}) ->
  encode_read_objects(Objects, TxId);
encode_message({error_response, {ErrorCode, Message}}) ->
  encode_error_resp(ErrorCode, Message);
encode_message({start_transaction_response, Resp}) ->
  encode_start_transaction_response(Resp);
encode_message({commit_response, Resp}) ->
  encode_commit_response(Resp);
encode_message({static_read_objects_response, {ok, Results, CommitTime}}) ->
  encode_static_read_objects_response(Results, CommitTime);
encode_message({read_objects_response, Resp}) ->
  encode_read_objects_response(Resp).

-spec decode_message(sendable()) -> message().
decode_message(#'ApbStartTransaction'{}) -> ok;
decode_message(#'ApbAbortTransaction'{}) -> ok;
decode_message(#'ApbCommitTransaction'{}) -> ok;
decode_message(#'ApbUpdateObjects'{}) -> ok;
decode_message(#'ApbStaticUpdateObjects'{}) -> ok;
decode_message(#'ApbStaticReadObjects'{}) -> ok;
decode_message(#'ApbReadObjects'{}) -> ok;

decode_message(#'ApbErrorResp'{}) -> ok;
decode_message(#'ApbStartTransactionResp'{}) -> ok;
decode_message(#'ApbCommitResp'{}) -> ok;
decode_message(#'ApbStaticReadObjectsResp'{}) -> ok;
decode_message(#'ApbReadObjectsResp'{}) -> ok.


% general encode function
encode(start_transaction, {Clock, Properties}) ->
  encode_start_transaction(Clock, Properties);
encode(txn_properties, Props) ->
  encode_txn_properties(Props);
encode(abort_transaction, TxId) ->
  encode_abort_transaction(TxId);
encode(commit_transaction, TxId) ->
  encode_commit_transaction(TxId);
encode(update_objects, {Updates, TxId}) ->
  encode_update_objects(Updates, TxId);
encode(update_op, {Object, Op, Param}) ->
  encode_update_op(Object, Op, Param);
encode(static_update_objects, {Clock, Properties, Updates}) ->
  encode_static_update_objects(Clock, Properties, Updates);
encode(bound_object, {Key, Type, Bucket}) ->
  encode_bound_object(Key, Type, Bucket);
encode(type, Type) ->
  encode_type(Type);
encode(reg_update, Update) ->
  encode_reg_update(Update);
encode(counter_update, Update) ->
  encode_counter_update(Update);
encode(set_update, Update) ->
  encode_set_update(Update);
encode(read_objects, {Objects, TxId}) ->
  encode_read_objects(Objects, TxId);
encode(static_read_objects, {Clock, Properties, Objects}) ->
  encode_static_read_objects(Clock, Properties, Objects);
encode(start_transaction_response, Resp) ->
  encode_start_transaction_response(Resp);
encode(operation_response, Resp) ->
  encode_operation_response(Resp);
encode(commit_response, Resp) ->
  encode_commit_response(Resp);
encode(read_objects_response, Resp) ->
  encode_read_objects_response(Resp);
encode(read_object_resp, Resp) ->
  encode_read_object_resp(Resp);
encode(static_read_objects_response, {ok, Results, CommitTime}) ->
  encode_static_read_objects_response(Results, CommitTime);
encode(error_code, Code) ->
  encode_error_code(Code);
encode(_Other, _) ->
  erlang:error("Incorrect operation/Not yet implemented").

% general decode function
decode(txn_properties, Properties) ->
  decode_txn_properties(Properties);
decode(bound_object, Obj) ->
  decode_bound_object(Obj);
decode(type, Type) ->
  decode_type(Type);
decode(error_code, Code) ->
  decode_error_code(Code);
decode(update_object, Obj) ->
  decode_update_op(Obj);
decode(reg_update, Update) ->
  decode_reg_update(Update);
decode(counter_update, Update) ->
  decode_counter_update(Update);
decode(set_update, Update) ->
  decode_set_update(Update);
decode(_Other, _) ->
  erlang:error("Unknown message").


%%%%%%%%%%%%%%%%%%%%%%%%%%%
% error codes

encode_error_code(unknown) -> 0;
encode_error_code(timeout) -> 1;
encode_error_code(_Other) -> 0.

decode_error_code(0) -> unknown;
decode_error_code(1) -> timeout;
decode_error_code(C) -> {error_code, C}.


encode_error_resp(ErrorCode, Message) ->
  #'ApbErrorResp'{errcode = encode_error_code(ErrorCode), errmsg = Message}.

decode_error_resp(#'ApbErrorResp'{errcode = ErrorCode, errmsg = Message}) ->
  {decode_error_code(ErrorCode), Message}.


%%%%%%%%%%%%%%%%%%%%%%%
% Transactions

encode_start_transaction(Clock, Properties) ->
  case Clock of
    ignore ->
      #'ApbStartTransaction'{
        properties = encode_txn_properties(Properties)};
    _ ->
      #'ApbStartTransaction'{timestamp = Clock,
        properties = encode_txn_properties(Properties)}
  end.


encode_commit_transaction(TxId) ->
  #'ApbCommitTransaction'{transaction_descriptor = TxId}.

encode_abort_transaction(TxId) ->
  #'ApbAbortTransaction'{transaction_descriptor = TxId}.


encode_txn_properties(_Props) ->
  %%TODO: Add more property parameters
  #'ApbTxnProperties'{}.

decode_txn_properties(_Properties) ->
  {}.


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
    updates = EncUpdates}.


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

encode_static_read_objects_response(Results, CommitTime) ->
  #'ApbStaticReadObjectsResp'{
    objects = encode_read_objects_response({ok, Results}),
    committime = encode_commit_response({ok, CommitTime})}.


encode_read_objects_response({error, Reason}) ->
  #'ApbReadObjectsResp'{success = false, errorcode = encode_error_code(Reason)};
encode_read_objects_response({ok, Results}) ->
  EncResults = lists:map(fun(R) ->
    encode_read_object_resp(R) end,
    Results),
  #'ApbReadObjectsResp'{success = true, objects = EncResults}.


encode_start_transaction_response({error, Reason}) ->
  #'ApbStartTransactionResp'{success = false, errorcode = encode_error_code(Reason)};
encode_start_transaction_response({ok, TxId}) ->
  #'ApbStartTransactionResp'{success = true, transaction_descriptor = term_to_binary(TxId)}.

encode_operation_response({error, Reason}) ->
  #'ApbOperationResp'{success = false, errorcode = encode_error_code(Reason)};
encode_operation_response(ok) ->
  #'ApbOperationResp'{success = true}.

encode_commit_response({error, Reason}) ->
  #'ApbCommitResp'{success = false, errorcode = encode_error_code(Reason)};

encode_commit_response({ok, CommitTime}) ->
  #'ApbCommitResp'{success = true, commit_time = term_to_binary(CommitTime)}.

decode_response(#'ApbOperationResp'{success = true}) ->
  {opresponse, ok};
decode_response(#'ApbOperationResp'{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#'ApbStartTransactionResp'{success = true,
  transaction_descriptor = TxId}) ->
  {start_transaction, TxId};
decode_response(#'ApbStartTransactionResp'{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#'ApbCommitResp'{success = true, commit_time = TimeStamp}) ->
  {commit_transaction, TimeStamp};
decode_response(#'ApbCommitResp'{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#'ApbReadObjectsResp'{success = false, errorcode = Reason}) ->
  {error, decode_error_code(Reason)};
decode_response(#'ApbReadObjectsResp'{success = true, objects = Objects}) ->
  Resps = lists:map(fun(O) ->
    decode_response(O) end,
    Objects),
  {read_objects, Resps};
decode_response(#'ApbReadObjectResp'{} = ReadObjectResp) ->
  decode_read_object_resp(ReadObjectResp);
decode_response(#'ApbStaticReadObjectsResp'{objects = Objects,
  committime = CommitTime}) ->
  {read_objects, Values} = decode_response(Objects),
  {commit_transaction, TimeStamp} = decode_response(CommitTime),
  {static_read_objects_resp, Values, TimeStamp};
decode_response(Other) ->
  erlang:error("Unexpected message: ~p", [Other]).

%%%%%%%%%%%%%%%%%%%%%%
%% Reading objects


encode_static_read_objects(Clock, Properties, Objects) ->
  EncTransaction = encode_start_transaction(Clock, Properties),
  EncObjects = lists:map(fun(Object) ->
    encode_bound_object(Object) end,
    Objects),
  #'ApbStaticReadObjects'{transaction = EncTransaction,
    objects = EncObjects}.

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

encode_type(antidote_crdt_counter_pn) -> 'COUNTER';
encode_type(antidote_crdt_counter_fat) -> 'FATCOUNTER';
encode_type(antidote_crdt_set_aw) -> 'ORSET';
encode_type(antidote_crdt_register_lww) -> 'LWWREG';
encode_type(antidote_crdt_register_mv) -> 'MVREG';
encode_type(antidote_crdt_map_go) -> 'GMAP';
encode_type(antidote_crdt_set_rw) -> 'RWSET';
encode_type(antidote_crdt_map_rr) -> 'RRMAP';
encode_type(antidote_crdt_flag_ew) -> 'FLAG_EW';
encode_type(antidote_crdt_flag_dw) -> 'FLAG_DW';
encode_type(T) -> erlang:error({unknown_crdt_type, T}).


decode_type('COUNTER') -> antidote_crdt_counter_pn;
decode_type('FATCOUNTER') -> antidote_crdt_counter_fat;
decode_type('ORSET') -> antidote_crdt_set_aw;
decode_type('LWWREG') -> antidote_crdt_register_lww;
decode_type('MVREG') -> antidote_crdt_register_mv;
decode_type('GMAP') -> antidote_crdt_map_go;
decode_type('RWSET') -> antidote_crdt_set_rw;
decode_type('RRMAP') -> antidote_crdt_map_rr;
decode_type('FLAG_EW') -> antidote_crdt_flag_ew;
decode_type('FLAG_DW') -> antidote_crdt_flag_dw;
decode_type(T) -> erlang:error({unknown_crdt_type_protobuf, T}).


%%%%%%%%%%%%%%%%%%%%%%
% CRDT operations


encode_update_operation(_Type, {reset, {}}) ->
  #'ApbUpdateOperation'{resetop = #'ApbCrdtReset'{}};
encode_update_operation(antidote_crdt_counter_pn, Op_Param) ->
  #'ApbUpdateOperation'{counterop = encode_counter_update(Op_Param)};
encode_update_operation(antidote_crdt_counter_fat, Op_Param) ->
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

%%decode_update_operation(#'Apbupdateoperation'{mapop = Op}) when Op /= undefined ->
%%  decode_map_update(Op);
%%decode_update_operation(#'Apbupdateoperation'{resetop = Op}) when Op /= undefined ->
%%  decode_reset_update(Op).

% general encoding of CRDT responses

encode_read_object_resp({{_Key, Type, _Bucket}, Val}) ->
  encode_read_object_resp(Type, Val).

encode_read_object_resp(antidote_crdt_register_lww, Val) ->
    #'ApbReadObjectResp'{reg=#'ApbGetRegResp'{value=Val}};
encode_read_object_resp(antidote_crdt_register_mv, Vals) ->
    #'ApbReadObjectResp'{mvreg = #'ApbGetMVRegResp'{values = Vals}};
encode_read_object_resp(antidote_crdt_counter_pn, Val) ->
    #'ApbReadObjectResp'{counter=#'ApbGetCounterResp'{value=Val}};
encode_read_object_resp(antidote_crdt_counter_fat, Val) ->
    #'ApbReadObjectResp'{counter=#'ApbGetCounterResp'{value=Val}};
encode_read_object_resp(antidote_crdt_set_aw, Val) ->
    #'ApbReadObjectResp'{set=#'ApbGetSetResp'{value=Val}};
encode_read_object_resp(antidote_crdt_set_rw, Val) ->
    #'ApbReadObjectResp'{set=#'ApbGetSetResp'{value=Val}};
encode_read_object_resp(antidote_crdt_map_go, Val) ->
    #'ApbReadObjectResp'{map = encode_map_get_resp(Val)};
encode_read_object_resp(antidote_crdt_map_rr, Val) ->
    #'ApbReadObjectResp'{map = encode_map_get_resp(Val)};
encode_read_object_resp(antidote_crdt_flag_ew, Val) ->
    #'ApbReadObjectResp'{flag = #'ApbGetFlagResp'{value = Val}};
encode_read_object_resp(antidote_crdt_flag_dw, Val) ->
    #'ApbReadObjectResp'{flag = #'ApbGetFlagResp'{value = Val}}.

% TODO why does this use counter instead of antidote_crdt_counter etc.?
decode_read_object_resp(#'ApbReadObjectResp'{counter = #'ApbGetCounterResp'{value = Val}}) ->
  {counter, Val};
decode_read_object_resp(#'ApbReadObjectResp'{set = #'ApbGetSetResp'{value = Val}}) ->
  {set, Val};
decode_read_object_resp(#'ApbReadObjectResp'{reg = #'ApbGetRegResp'{value = Val}}) ->
  {reg, Val};
decode_read_object_resp(#'ApbReadObjectResp'{mvreg = #'ApbGetMVRegResp'{values = Vals}}) ->
  {mvreg, Vals};
decode_read_object_resp(#'ApbReadObjectResp'{map = MapResp=#'ApbGetMapResp'{}}) ->
    {map, decode_map_get_resp(MapResp)};
decode_read_object_resp(#'ApbReadObjectResp'{flag = #'ApbGetFlagResp'{value = Val}}) ->
    {flag, Val}.

% set updates

encode_set_update({add, Elem}) ->
  ?assert_binary(Elem),
  #'ApbSetUpdate'{optype = 'ADD', adds = [Elem]};
encode_set_update({add_all, Elems}) ->
  ?assert_all_binary(Elems),
  #'ApbSetUpdate'{optype = 'ADD', adds = Elems};
encode_set_update({remove, Elem}) ->
  ?assert_binary(Elem),
  #'ApbSetUpdate'{optype = 'REMOVE', rems = [Elem]};
encode_set_update({remove_all, Elems}) ->
  ?assert_all_binary(Elems),
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
    key = encode_map_key({Key, Type}),
    update = encode_update_operation(Type, Update)
  }.

decode_map_nested_update(#'ApbMapNestedUpdate'{key = KeyEnc, update = UpdateEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  Update = decode_update_operation(UpdateEnc),
  {{Key, Type}, Update}.

encode_map_key({Key, Type}) ->
  ?assert_binary(Key),
  #'ApbMapKey'{
    key = Key,
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
  #'ApbMapEntry'{
    key = encode_map_key({Key, Type}),
    value = encode_read_object_resp(Type, Val)
  }.

decode_map_entry(#'ApbMapEntry'{key = KeyEnc, value = ValueEnc}) ->
  {Key, Type} = decode_map_key(KeyEnc),
  {_Tag, Value} = decode_read_object_resp(ValueEnc),
  {{Key, Type}, Value}.





-ifdef(TEST).

%% Tests encode and decode
start_transaction_test() ->
  Clock = term_to_binary(ignore),
  Properties = {},
  EncRecord = antidote_pb_codec:encode(start_transaction,
    {Clock, Properties}),
  [MsgCode, MsgData] = encode_msg(EncRecord),
  Msg = decode_msg(MsgCode, list_to_binary(MsgData)),
  ?assertMatch(true, is_record(Msg, 'ApbStartTransaction')),
  ?assertMatch(ignore, binary_to_term(Msg#'ApbStartTransaction'.timestamp)),
  ?assertMatch(Properties,
    antidote_pb_codec:decode(txn_properties,
      Msg#'ApbStartTransaction'.properties)).

read_transaction_test() ->
  Objects = [{<<"key1">>, antidote_crdt_counter_pn, <<"bucket1">>},
    {<<"key2">>, antidote_crdt_set_aw, <<"bucket2">>}],
  TxId = term_to_binary({12}),
  %% Dummy value, structure of TxId is opaque to client
  EncRecord = antidote_pb_codec:encode_read_objects(Objects, TxId),
  ?assertMatch(true, is_record(EncRecord, 'ApbReadObjects')),
  [MsgCode, MsgData] = encode_msg(EncRecord),
  Msg = decode_msg(MsgCode, list_to_binary(MsgData)),
  ?assertMatch(true, is_record(Msg, 'ApbReadObjects')),
  DecObjects = lists:map(fun(O) ->
    antidote_pb_codec:decode_bound_object(O) end,
    Msg#'ApbReadObjects'.boundobjects),
  ?assertMatch(Objects, DecObjects),
  %% Test encoding error
  ErrEnc = antidote_pb_codec:encode(read_objects_response,
    {error, someerror}),
  [ErrMsgCode, ErrMsgData] = encode_msg(ErrEnc),
  ErrMsg = decode_msg(ErrMsgCode, list_to_binary(ErrMsgData)),
  ?assertMatch({error, unknown},
    antidote_pb_codec:decode_response(ErrMsg)),

  %% Test encoding results
  Results = [1, [<<"a">>, <<"b">>]],
  ResEnc = antidote_pb_codec:encode(read_objects_response,
    {ok, lists:zip(Objects, Results)}
  ),
  [ResMsgCode, ResMsgData] = encode_msg(ResEnc),
  ResMsg = decode_msg(ResMsgCode, list_to_binary(ResMsgData)),
  ?assertMatch({read_objects, [{counter, 1}, {set, [<<"a">>, <<"b">>]}]},
    antidote_pb_codec:decode_response(ResMsg)).

update_types_test() ->
    Updates = [ {{<<"1">>, antidote_crdt_counter_pn, <<"2">>}, increment , 1},
                {{<<"2">>, antidote_crdt_counter_pn, <<"2">>}, increment , 1},
                {{<<"a">>, antidote_crdt_set_aw, <<"2">>}, add , <<"3">>},
                {{<<"b">>, antidote_crdt_counter_pn, <<"2">>}, increment , 2},
                {{<<"c">>, antidote_crdt_set_aw, <<"2">>}, add, <<"4">>},
                {{<<"a">>, antidote_crdt_set_aw, <<"2">>}, add_all , [<<"5">>,<<"6">>]}
  ],
  TxId = term_to_binary({12}),
  %% Dummy value, structure of TxId is opaque to client
  EncRecord = antidote_pb_codec:encode_update_objects(Updates, TxId),
  ?assertMatch(true, is_record(EncRecord, 'ApbUpdateObjects')),
  [MsgCode, MsgData] = encode_msg(EncRecord),
  Msg = decode_msg(MsgCode, list_to_binary(MsgData)),
  ?assertMatch(true, is_record(Msg, 'ApbUpdateObjects')),
  DecUpdates = lists:map(fun(O) ->
    antidote_pb_codec:decode_update_op(O) end,
    Msg#'ApbUpdateObjects'.updates),
  ?assertMatch(Updates, DecUpdates).

error_messages_test() ->
  EncRecord1 = antidote_pb_codec:encode(start_transaction_response,
    {error, someerror}),
  [MsgCode1, MsgData1] = encode_msg(EncRecord1),
  Msg1 = decode_msg(MsgCode1, list_to_binary(MsgData1)),
  Resp1 = antidote_pb_codec:decode_response(Msg1),
  ?assertMatch(Resp1, {error, unknown}),

  EncRecord2 = antidote_pb_codec:encode(operation_response,
    {error, someerror}),
  [MsgCode2, MsgData2] = encode_msg(EncRecord2),
  Msg2 = decode_msg(MsgCode2, list_to_binary(MsgData2)),
  Resp2 = antidote_pb_codec:decode_response(Msg2),
  ?assertMatch(Resp2, {error, unknown}),

  EncRecord3 = antidote_pb_codec:encode(read_objects_response,
    {error, someerror}),
  [MsgCode3, MsgData3] = encode_msg(EncRecord3),
  Msg3 = decode_msg(MsgCode3, list_to_binary(MsgData3)),
  Resp3 = antidote_pb_codec:decode_response(Msg3),
  ?assertMatch(Resp3, {error, unknown}).

-define(TestCrdtOperationCodec(Type, Op, Param),
  ?assertEqual(
    {{<<"key">>, Type, <<"bucket">>}, Op, Param},
    decode_update_op(encode_update_op({<<"key">>, Type, <<"bucket">>}, Op, Param)))
).

-define(TestCrdtResponseCodec(Type, ExpectedType, Val),
  ?assertEqual(
    {ExpectedType, Val},
    decode_read_object_resp(encode_read_object_resp(Type, Val)))
).

crdt_encode_decode_test() ->
  %% encoding the following operations and decoding them again, should give the same result

  % Counter
  ?TestCrdtOperationCodec(antidote_crdt_counter_pn, increment, 1),
  ?TestCrdtResponseCodec(antidote_crdt_counter_pn, counter, 42),

  % lww-register
  ?TestCrdtOperationCodec(antidote_crdt_register_lww, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_register_lww, reg, <<"blub">>),


  % mv-register
  ?TestCrdtOperationCodec(antidote_crdt_register_mv, assign, <<"hello">>),
  ?TestCrdtResponseCodec(antidote_crdt_register_mv, mvreg, [<<"a">>, <<"b">>, <<"c">>]),

  % set
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_aw, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_set_aw, set, [<<"a">>, <<"b">>, <<"c">>]),

  % same for remove wins set:
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, add_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove, <<"hello">>),
  ?TestCrdtOperationCodec(antidote_crdt_set_rw, remove_all, [<<"a">>, <<"b">>, <<"c">>]),
  ?TestCrdtResponseCodec(antidote_crdt_set_rw, set, [<<"a">>, <<"b">>, <<"c">>]),

  % map
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, update, {{<<"key">>, antidote_crdt_register_mv}, {assign, <<"42">>}}),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, update, [
    {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
    {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, remove, {<<"key">>, antidote_crdt_register_mv}),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, remove, [
    {<<"a">>, antidote_crdt_register_mv},
    {<<"b">>, antidote_crdt_register_mv}]),
  ?TestCrdtOperationCodec(antidote_crdt_map_rr, batch, {
    [ {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
      {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}],
    [ {<<"a">>, antidote_crdt_register_mv},
      {<<"b">>, antidote_crdt_register_mv}]}),

  ?TestCrdtResponseCodec(antidote_crdt_map_rr, map, [
    {{<<"a">>, antidote_crdt_register_mv}, <<"42">>}
  ]),

  % gmap
  ?TestCrdtOperationCodec(antidote_crdt_map_go, update, {{<<"key">>, antidote_crdt_register_mv}, {assign, <<"42">>}}),
  ?TestCrdtOperationCodec(antidote_crdt_map_go, update, [
    {{<<"a">>, antidote_crdt_register_mv}, {assign, <<"42">>}},
    {{<<"b">>, antidote_crdt_set_aw}, {add, <<"x">>}}]),
  ?TestCrdtResponseCodec(antidote_crdt_map_go, map, [
    {{<<"a">>, antidote_crdt_register_mv}, <<"42">>}
  ]),

  % flag
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, enable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, disable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, reset, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, enable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, disable, {}),
  ?TestCrdtOperationCodec(antidote_crdt_flag_ew, reset, {}),

  ok.



-endif.




