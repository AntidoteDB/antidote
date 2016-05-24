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
-module(json_utilities).

-export([
	 convert_to_json/1,
	 deconvert_from_json/1,
	 list_to_json_binary_list/1,
	 json_binary_list_to_list/1,
	 json_binary_to_erlang_term/1,
	 json_binary_to_binary/1,
	 erlang_term_to_json_binary/1,
	 binary_to_json_binary/1
	]).
	 
convert_to_json(Elem) ->
    IsJSON = jsx:is_term(Elem),
    {IsUtf8,IsBinary} =
	case is_binary(Elem) of
	    true ->
		case unicode:characters_to_list(Elem,utf8) of
		    Res when is_list(Res) ->
			{true,true};
		    _ ->
			{false,true}
		end;
	    false ->
		{false,false}
	end,
    case Elem of
	Elem when IsUtf8 ->
	    [{json_value,Elem}];
	Elem when IsBinary ->
	    [{binary64,binary_to_json_binary(Elem)}];
	Elem when IsJSON ->
	    [{json_value,Elem}];
	Elem ->
	    [{erlang_term_binary64,erlang_term_to_json_binary(Elem)}]
    end.

deconvert_from_json([{ObjectType,Object}]) ->
    case ObjectType of
	json_value ->
	    Object;
	erlang_term_binary64 ->
	    json_binary_to_erlang_term(Object);
	binary64 ->
	    json_binary_to_binary(Object)
    end.

list_to_json_binary_list(List) ->
    lists:map(fun(Item) ->
		      convert_to_json(Item)
	      end,List).

json_binary_list_to_list(List) ->
    lists:map(fun(Item) ->
		      deconvert_from_json(Item)
	      end,List).

json_binary_to_erlang_term(Binary) ->
    binary_to_term(json_binary_to_binary(Binary)).
json_binary_to_binary(Binary) ->
    base64:decode(Binary).

erlang_term_to_json_binary(Item) ->
    binary_to_json_binary(term_to_binary(Item)).
binary_to_json_binary(Binary) ->
    base64:encode(Binary).
