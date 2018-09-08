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
%%% @doc An Antidote module that contains some built-in functions that
%%%      can be used in queries.
%%%      The idea is to implement in the future some of the basic SQL
%%%      functions, such as AVG, SUM, COUNT, etc.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(builtin_functions).

-include("querying.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MALFORMED_FUNC(Func), io_lib:format("Malformed function header: ~p", [Function])).
-define(ADD_WINS, add).
-define(REMOVE_WINS, remove).

%% API
-export([exec/2, is_function/1, replace_args/4]).
-export([find_last/3, assert_visibility/5]).

%% This function receives a function name and its parameters, and computes
%% the result of applying the parameters to the function.
%% If the function is a string, it parses the string, validates it, and
%% calls the function with its arguments.
exec({Function, Args}, TxId) ->
    case validate_func(Function, Args) of
        {_F, _A} -> apply_fun(ignore, Function, lists:append(Args, [TxId]));
        false -> throw(lists:flatten(?MALFORMED_FUNC(Function)))
    end;
exec(Function, TxId) ->
    exec(parse_function(Function), TxId).

%% Given a list of items (Values) search the item that appears last
%% in the second list (List)
find_last(Values, List, _TxId) when is_list(Values) andalso is_list(List) ->
    [First | Tail] = Values,
    find_last0(First, Tail, List);
find_last(Value, _List, _TxId) -> Value.

assert_visibility(State, Rule, Versions, SourceTable, TxId) ->
    Policy = table_utils:policy(SourceTable),
    ExplicitState = find_last(State, Rule, ignore) =/= d,
    case table_crps:dep_level(Policy) of
        undefined ->
            ExplicitState;
        _Else ->
            ExplicitState andalso check_versions(Versions, TxId)
    end.

check_versions([[Version, TName] | Versions], TxId) ->
    assert_visibility(Version, TName, TxId) andalso
        check_versions(Versions, TxId);
check_versions([], _TxId) -> true.

assert_visibility({Key, Version}, TableName, TxId) ->
    Table = table_utils:table_metadata(TableName, TxId),
    KeyAtom = querying_utils:to_atom(Key),
    %BoundObj = querying_utils:build_keys(KeyAtom, ?TABLE_DT, TableName),
    BoundObj = querying_utils:build_keys_from_table({KeyAtom, Key}, Table, TxId),
    [RefData] = record_utils:record_data(BoundObj, TxId),
    VersionKey = {?VERSION_COL, ?VERSION_COL_DT},
    RefVersion = record_utils:lookup_value(VersionKey, RefData),

    Policy = table_utils:policy(Table),
    %lager:info("Version: ~p", [Version]),
    %lager:info("BoundObj: ~p", [BoundObj]),
    %lager:info("RefData: ~p", [RefData]),
    %lager:info("RefVersion: ~p", [RefVersion]),
    %lager:info("Policy: ~p", [Policy]),
    RefDepLevel = table_crps:dep_level(Policy),
    RefPDepLevel = table_crps:p_dep_level(Policy),
    FinalRes =
        case RefPDepLevel of
            ?REMOVE_WINS ->
                RefVersion =:= Version andalso
                    is_visible(RefData, Table, TxId);
            _ ->
                case RefDepLevel of
                    ?REMOVE_WINS -> is_visible(RefData, Table, TxId);
                    _ -> true
                end
        end,

    %lager:info("{~p, ~p}: ~p", [Key, Version, FinalRes]),
    FinalRes.

is_function({FuncName, Args}) ->
    case validate_func(FuncName, Args) of
        {_Func, _Arity} -> true;
        _ -> false
    end.

replace_args({FuncName, Args}, TName, TCols, Record) ->
    replace_args(FuncName, Args, TName, TCols, Record, []).

replace_args(FName, [Arg | Args], TName, TCols, Record, AccArgs) when is_list(Arg) ->
    NewArg = replace_args({FName, Arg}, TName, TCols, Record),
    replace_args(FName, Args, TName, TCols, Record, lists:append(AccArgs, [NewArg]));
replace_args(FName, [Arg | Args], TName, TCols, Record, AccArgs) ->
    NewArg =
        case ?is_column(Arg) of
            true ->
                ?COLUMN(ColName) = Arg,
                case lists:member(ColName, TCols) of
                    true ->
                        ?ATTRIBUTE(_C, _T, ColValue) =
                            record_utils:get_column(ColName, Record),
                        lists:append(AccArgs, [ColValue]);
                    false ->
                        ErrorMsg =
                            io_lib:format("Column ~p in function ~p is invalid for table ~p", [ColName, FName, TName]),
                        throw(lists:flatten(ErrorMsg))
                end;
            false ->
                lists:append(AccArgs, [Arg])
        end,
    replace_args(FName, Args, TName, TCols, Record, NewArg);
replace_args(_FName, [], _TName, _TCols, _Record, Acc) ->
    Acc.

%% ===================================================================
%% Internal functions
%% ===================================================================

find_last0(V1, [V2 | Tail], List) ->
    Current = pick(V1, V2, List),
    find_last0(Current, Tail, List);
find_last0(V1, [], _List) -> V1.

pick(V1, V2, [V1 | _Tail]) -> V2;
pick(V1, V2, [V2 | _Tail]) -> V1;
pick(V1, V1, _List) -> V1;
pick(V1, V2, [_V3 | Tail]) -> pick(V1, V2, Tail);
pick(_, _, []) -> error.

is_visible(ObjData, Table, TxId) ->
    Rule = table_crps:get_rule(Table),
    ObjState = record_utils:lookup_value({?STATE_COL, ?STATE_COL_DT}, ObjData),

    FKeys = table_utils:foreign_keys(Table),

    %lager:info("Table: ~p", [Table]),
    %lager:info("Rule: ~p", [Rule]),
    %lager:info("ObjState: ~p", [ObjState]),
    %lager:info("FKeys: ~p", [FKeys]),
    %lager:info("ObjData: ~p", [ObjData]),

    [PKName] = table_utils:primary_key_name(Table),
    %lager:info("PKName: ~p", [PKName]),
    PKValue = querying_utils:to_atom(record_utils:lookup_value(PKName, ObjData)),
    %lager:info("PKValue: ~p", [PKValue]),
    ObjKey = {PKValue, ?TABLE_DT, table_utils:name(Table)},

    %% TODO delete ObjData
    find_last(ObjState, Rule, ignore) =/= d andalso
        (is_visible0(FKeys, ObjData, TxId) orelse
        record_utils:delete_record(ObjKey, TxId)).

is_visible0([?FK(FkName, _, FkTable, _, _) | Tail], Record, TxId)
    when length(FkName) == 1 ->
    ObjVersion = record_utils:lookup_value(FkName, Record),
    assert_visibility(ObjVersion, FkTable, TxId) andalso is_visible0(Tail, Record, TxId);
is_visible0([?FK(FkName, _, _, _, _) | Tail], Record, TxId)
    when length(FkName) > 1 ->
    is_visible0(Tail, Record, TxId);
is_visible0([], _Record, _TxId) -> true.

get_function_info(FunctionName) when is_atom(FunctionName) ->
    proplists:lookup(FunctionName, ?MODULE:module_info(exports)).

validate_func(FunctionName, Args) when is_atom(FunctionName) andalso is_list(Args) ->
    case get_function_info(FunctionName) of
        {FunctionName, Arity} = Pair ->
            case length(Args) =:= (Arity - 1) of
                true -> Pair;
                false -> false
            end;
        _ -> false
    end;
validate_func(FunctionName, Args) ->
    validate_func(querying_utils:to_atom(FunctionName), Args).

apply_fun(_, find_last, [Values, List, TxId]) ->
    find_last(Values, List, TxId);
apply_fun(_, assert_visibility, [State, Rule, Vrs, Table, TxId]) ->
    assert_visibility(State, Rule, Vrs, Table, TxId);
apply_fun(_, _, _) ->
    ok.

%% Parses a string that denotes the header of a function, on the form:
%% function(param1, param2, ... , paramN)
parse_function(Function) when is_atom(Function) ->
    FuncString = atom_to_list(Function),
    parse_function(FuncString);
parse_function(Function) when is_list(Function) ->
    try
        FParPos = string:str(Function, "("),
        LParPos = string:rstr(Function, ")"),
        FuncName = list_to_atom(string:sub_string(Function, 1, FParPos - 1)),
        Args = string:tokens(string:sub_string(Function, FParPos + 1, LParPos - 1), " ,"),
        validate_func(FuncName, Args)
    of
        {F, P} -> {F, P};
        false -> throw(lists:flatten(?MALFORMED_FUNC(Function)))
    catch
        Exception ->
            ErrorMsg = io_lib:format("An error ocurred when parsing a function: ~p", [Exception]),
            lager:error(lists:flatten(ErrorMsg))
    end.

-ifdef(TEST).

find_last_test() ->
    Values1 = [a, b, c],
    Values2 = [c, c, b],
    Values3 = [d],
    Values4 = [f, g],
    List1 = [a, b, c, d, e],
    List2 = [e, b, c, a, d],
    List3 = [d, e, c, a, b],
    ?assertEqual(c, find_last(Values1, List1, ignore)),
    ?assertEqual(a, find_last(Values1, List2, ignore)),
    ?assertEqual(b, find_last(Values2, List3, ignore)),
    ?assertEqual(d, find_last(Values3, List1, ignore)),
    ?assertEqual(error, find_last(Values4, List2, ignore)).

-endif.
