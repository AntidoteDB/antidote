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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(MALFORMED_FUNC(Func), lists:concat(["Malformed function header: ", Function])).

%% API
-export([exec/1, find_first/2, is_function/1]).

%% This function receives a function name and its parameters, and compute
%% the result of applying the parameters to the function.
%% If the function is a string, it parses the string, validates it, and
%% calls the function with its arguments.
exec({Function, Args}) ->
    %io:format(">> exec:~n"),
    %io:format("{Function, Args}: ~p~n", [{Function, Args}]),
    case validate_func(Function, Args) of
        {_F, _A} -> apply(?MODULE, Function, Args); % TODO apply(Function, Params)
        false -> throw(?MALFORMED_FUNC(Function))
    end;
exec(Function) ->
    exec(parse_function(Function)).

%% Given a list of items (Values) search the item that appears first
%% in the second list (List)
find_first(Values, List) when is_list(Values) andalso is_list(List) ->
    [First | Tail] = Values,
    find_first(First, Tail, List);
find_first(Value, _List) -> Value.

is_function({FuncName, Args}) ->
    case validate_func(FuncName, Args) of
        {_Func, _Arity} -> io:format("is_function: true~n"), true;
        _ -> io:format("is_function: false~n"), false
    end.

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
        false -> throw(?MALFORMED_FUNC(Function))
    catch
        Exception ->
            lager:error(lists:concat(["An error ocurred when parsing a function: ", Exception]))
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

find_first(V1, [V2 | Tail], List) ->
    Current = pick(V1, V2, List),
    find_first(Current, Tail, List);
find_first(V1, [], _List) -> V1.

pick(V1, _V2, [V1 | _Tail]) -> V1;
pick(_V1, V2, [V2 | _Tail]) -> V2;
pick(V1, V1, _List) -> V1;
pick(V1, V2, [_V3 | Tail]) -> pick(V1, V2, Tail);
pick(_, _, []) -> error.

get_function_info(FunctionName) when is_atom(FunctionName) ->
    proplists:lookup(FunctionName, ?MODULE:module_info(exports)).

validate_func(FunctionName, Args) when is_atom(FunctionName) andalso is_list(Args) ->
    case get_function_info(FunctionName) of
        {FunctionName, Arity} = Pair ->
            case length(Args) =:= Arity of
                true -> Pair;
                false -> false
            end;
        _ -> false
    end;
validate_func(FunctionName, Args) ->
    validate_func(querying_utils:to_atom(FunctionName), Args).

-ifdef(TEST).

find_first_test() ->
    Values1 = [a, b, c],
    Values2 = [c, c, b],
    Values3 = [d],
    Values4 = [f, g],
    List1 = [a, b, c, d, e],
    List2 = [e, b, c, a, d],
    List3 = [d, e, c, a, b],
    ?assertEqual(a, find_first(Values1, List1)),
    ?assertEqual(b, find_first(Values1, List2)),
    ?assertEqual(c, find_first(Values2, List3)),
    ?assertEqual(d, find_first(Values3, List1)),
    ?assertEqual(error, find_first(Values4, List2)).

-endif.
