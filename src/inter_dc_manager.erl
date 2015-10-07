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
	 get_my_dc_wid/0,
         start_receiver/1,
         get_dcs/0,
	 get_dcs_wids/0,
         add_dc/1,
         add_list_dcs/1,
	 get_my_read_dc/0,
	 get_my_read_dc_wid/0,
         start_read_receiver/1,
         get_read_dcs/0,
	 get_read_dcs_wids/0,
         add_read_dc/1,
         add_list_read_dcs/1,
	 set_replication_fun/2,
	 set_replication_list/1,
         stop_receiver/0]).

-define(META_PREFIX_DC, {dcid,port}).
-define(META_PREFIX_MY_DC, {mydcid,port}).
-define(META_PREFIX_READ_DC, {dcidread,port}).
-define(META_PREFIX_MY_READ_DC, {mydcidread,port}).

%% ===================================================================
%% Public API
%% ===================================================================

get_my_dc_wid() ->
    riak_core_metadata:get(?META_PREFIX_MY_DC,mydc).

get_my_dc() ->
    {_Id,Dc} = riak_core_metadata:get(?META_PREFIX_MY_DC,mydc),
    Dc.

start_receiver({Id,{DcIp, Port}}) ->
    riak_core_metadata:put(?META_PREFIX_MY_DC,mydc,{Id,{DcIp,Port}}),
    ok.

stop_receiver() ->
    antidote_sup:stop_rep().

%% Returns all DCs known to this DC.
-spec get_dcs() ->[dc_address()].
get_dcs() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({{_Id,DC},[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

get_dcs_wids() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_DC),
    lists:foldl(fun({{Id,DC},[0|_T]},NewAcc) ->
			lists:append([{Id,DC}],NewAcc) end,
		[], DcList).


%% Add info about a new DC. This info could be
%% used by other modules to communicate to other DC
-spec add_dc(dc_address()) -> ok.
add_dc({Id,{Ip,Port}}) ->
    riak_core_metadata:put(?META_PREFIX_DC,{Id,{Ip,Port}},0),
    ok.

%% Add a list of DCs to this DC
-spec add_list_dcs([dc_address()]) -> ok.
add_list_dcs(DCs) ->
    lists:foldl(fun({Id,{Ip,Port}},_Acc) ->
			riak_core_metadata:put(?META_PREFIX_DC,{Id,{Ip,Port}},0)
		end, 0, DCs),
    ok.


get_my_read_dc() ->
    {_Id,Dc} = riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc),
    Dc.
    
get_my_read_dc_wid() ->
    riak_core_metadata:get(?META_PREFIX_MY_READ_DC,mydc).

start_read_receiver({Id,{DcIp,Port}}) ->
    riak_core_metadata:put(?META_PREFIX_MY_READ_DC,mydc,{Id,{DcIp,Port}}),
    ok.

get_read_dcs() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
    lists:foldl(fun({{_Id,DC},[0|_T]},NewAcc) ->
			lists:append([DC],NewAcc) end,
		[], DcList).

get_read_dcs_wids() ->
    DcList = riak_core_metadata:to_list(?META_PREFIX_READ_DC),
    lists:foldl(fun({{Id,DC},[0|_T]},NewAcc) ->
			lists:append([{Id,DC}],NewAcc) end,
		[], DcList).


add_read_dc({Id,{Ip,Port}}) ->
    riak_core_metadata:put(?META_PREFIX_READ_DC,{Id,{Ip,Port}},0),
    ok.

add_list_read_dcs(DCs) ->
    lists:foldl(fun({Id,{Ip,Port}},_Acc) ->
			riak_core_metadata:put(?META_PREFIX_READ_DC,{Id,{Ip,Port}},0)
		end, 0, DCs),
    ok.

set_replication_fun(KeyDescription,Id) ->
    replication_check:set_replication_fun(KeyDescription,Id).

set_replication_list(List) ->
    replication_check:set_replication_list(List).
