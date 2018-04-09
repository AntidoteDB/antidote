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

%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote module that contains some common and utility
%%%      functions for, but not exclusively to, the indexing and
%%%      query_optimizer modules.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(querying_commons).

%% API
-export([build_keys/3,
         read_keys/2,
         write_keys/3,
         to_atom/1,
         to_list/1,
         remove_duplicates/1]).

build_keys([], _Types, _Bucket) -> [];
build_keys(Keys, Types, Bucket) when is_list(Keys) and is_list(Types) ->
    build_keys(Keys, Types, Bucket, []);
build_keys(Keys, Type, Bucket) when is_list(Keys) ->
    Len = length(Keys),
    build_keys(Keys, lists:duplicate(Len, Type), Bucket);
build_keys(Key, Type, Bucket) ->
    build_keys([Key], [Type], Bucket).

build_keys([Key | Tail1], [Type | Tail2], Bucket, Acc) ->
    BucketAtom = to_atom(Bucket),
    TypeAtom = to_atom(Type),
    ObjKey = {Key, TypeAtom, BucketAtom},
    build_keys(Tail1, Tail2, Bucket, lists:append(Acc, [ObjKey]));
build_keys([], [], _Bucket, Acc) ->
    Acc.

read_keys([], _TxId) -> [[]];
read_keys(ObjKeys, TxId) when is_list(ObjKeys) ->
    %% TODO read objects from Cure or Materializer?
    {ok, Objs} = cure:read_objects(ObjKeys, TxId),
    Objs;
read_keys(ObjKey, TxId) ->
    read_keys([ObjKey], TxId).

write_keys(_ObjKeys, Updates, TxId) ->
    cure:update_objects(Updates, TxId).

to_atom(Term) when is_list(Term) ->
    list_to_atom(Term);
to_atom(Term) when is_integer(Term) ->
    List = integer_to_list(Term),
    list_to_atom(List);
to_atom(Term) when is_atom(Term) ->
    Term.

to_list(Term) when is_list(Term) ->
    Term;
to_list(Term) when is_integer(Term) ->
    integer_to_list(Term);
to_list(Term) when is_atom(Term) ->
    atom_to_list(Term).

remove_duplicates(List) when is_list(List) ->
    Aux = sets:from_list(List),
    sets:to_list(Aux);
remove_duplicates(Other) ->
    case sets:is_set(Other) of
        true -> Other;
        false -> throw(lists:concat(["Cannot remove duplicates in this object: ", Other]))
    end.
