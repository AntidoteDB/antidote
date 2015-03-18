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

-module(inter_dc_manager).

-include("antidote.hrl").

-export([get_my_dc/0,
         start_receiver/1,
         get_dcs/0,
         add_dc/1,
         add_list_dcs/1,
	 get_my_read_dc/0,
         start_read_receiver/1,
         get_read_dcs/0,
         add_read_dc/1,
         add_list_read_dcs/1,
	 set_replication_keys/1
	]).

-define(META_PREFIX_DC, {dcid,port}).
-define(META_PREFIX_MY_DC, {mydcid,port}).
-define(META_PREFIX_READ_DC, {dcidread,port}).
-define(META_PREFIX_MY_READ_DC, {mydcidread,port}).

%% ===================================================================
%% Public API
%% ===================================================================


get_my_dc() ->
    riak_core_metadata:get(?META_PREFIX_MY_DC,mydc).

start_receiver({DcIp, Port}) ->
    riak_core_metadata:put(?META_PREFIX_MY_DC,mydc,{DcIp,Port}),
    ok.


%% Returns all DCs known to this DC.
-spec get_dcs() ->{ok, [dc_address()]}.
get_dcs() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({DC,[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec add_dc(dc_address()) -> ok.
add_dc(NewDC) ->
    riak_core_metadata:put(?META_PREFIX_DC,NewDC,0),
    ok.

%% Add a list of DCs to this DC
-spec add_list_dcs([dc_address()]) -> ok.
add_list_dcs(DCs) ->
    lists:foldl(fun(DC,_Acc) ->
			riak_core_metadata:put(?META_PREFIX_DC,DC,0)
		end, 0, DCs),
    ok.


get_my_read_dc() ->
    riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc).

start_read_receiver({DcIp,Port}) ->
    riak_core_metadata:put(?META_PREFIX_MY_READ_DC,mydc,{DcIp,Port}),
    ok.

get_read_dcs() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
    lists:foldl(fun({DC,[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

add_read_dc(NewDC) ->
    riak_core_metadata:put(?META_PREFIX_READ_DC,NewDC,0),
    ok.

add_list_read_dcs(DCs) ->
    lists:foldl(fun(DC,_Acc) ->
			riak_core_metadata:put(?META_PREFIX_READ_DC,DC,0)
		end, 0, DCs),
    ok.

set_replication_keys(KeyDescription) ->
    replication_check:set_replication(KeyDescription).

