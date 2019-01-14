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

-module(binary_utilities).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

-export([
          check_message_version/1,
          check_version_and_req_id/1
        ]).

%% Check a binary message version for inter_dc messages
%% performed by inter_dc_query
-spec check_message_version(<<_:?VERSION_BITS, _:_*8>>) -> <<_:_*8>>.
check_message_version(<<Version:?VERSION_BYTES/binary, Rest/binary>>) ->
    %% Only support one version now
    ?MESSAGE_VERSION = Version,
    Rest.

%% Check a binary message version and the message id for inter_dc messages
%% performed by inter_dc_query
-spec check_version_and_req_id(<<_:?MESSAGE_HEADER_BIT_LENGTH, _:_*8>>) -> {<<_:?REQUEST_ID_BIT_LENGTH>>, binary()}.
check_version_and_req_id(Binary) ->
    <<ReqId:?REQUEST_ID_BYTE_LENGTH/binary, Rest/binary>> = check_message_version(Binary),
    {ReqId, Rest}.
