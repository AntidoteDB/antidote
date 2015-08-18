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
-module(inter_dc_utils).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-define(PARTITION_BYTE_LENGTH, 20).

-export([
  now_millisec/0,
  bin_to_txn/1,
  txn_to_bin/1,
  partition_to_bin/1]).

now_millisec() ->
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

pad(Width, Binary) ->
  case Width - byte_size(Binary) of
    N when N =< 0 -> Binary;
    N -> <<0:(N*8), Binary/binary>>
  end.

partition_to_bin(Partition) -> pad(?PARTITION_BYTE_LENGTH, binary:encode_unsigned(Partition)).

txn_to_bin(Txn = #interdc_txn{partition = P}) ->
  Prefix = partition_to_bin(P),
  Msg = term_to_binary(Txn),
  <<Prefix/binary, Msg/binary>>.

bin_to_txn(Bin) ->
  L = byte_size(Bin),
  Msg = binary_part(Bin, {?PARTITION_BYTE_LENGTH, L - ?PARTITION_BYTE_LENGTH}),
  binary_to_term(Msg).

