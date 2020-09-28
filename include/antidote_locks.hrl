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

-record(read_crdt_state, {
    snapshot_time :: snapshot_time(),
    objects :: [bound_object()],
    data :: antidote_lock_server_state:read_crdt_state_onresponse_data()
}).

-record(update_crdt_state, {
    snapshot_time :: snapshot_time(),
    updates :: [{bound_object(), op_name(), op_param()}],
    data :: antidote_lock_server_state:update_crdt_state_onresponse_data()
}).

-record(send_inter_dc_message, {
    receiver :: dcid(),
    message :: antidote_lock_server_state:inter_dc_message()
}).

-record(accept_request, {
    requester :: requester(),
    clock :: snapshot_time()
}).

-record(abort_request, {
    requester :: requester()
}).

-record(set_timeout, {
    timeout :: antidote_lock_server_state:milliseconds(),
    message :: any()
}).
