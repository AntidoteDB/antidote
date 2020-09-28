-module(antidote_list_utils_SUITE).
-include_lib("antidote/include/antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, check_topsort/1]).

all() -> [
    check_topsort
].

check_topsort(_Config) ->
    dorer:check(fun() ->
        List = dorer:gen(dorer_generators:list(dorer_generators:set(dorer_generators:integer()))),
        dorer:log("List = ~p", [List]),
        Sorted = antidote_list_utils:topsort(fun cmp/2, List),
        ?assert(is_sorted(fun cmp/2, Sorted)),
        ?assert(lists:sort(List) == lists:sort(Sorted))
    end).

is_sorted(_Cmp, []) -> true;
is_sorted(Cmp, [X | Xs]) -> lists:all(fun(Y) -> not Cmp(Y, X) end, Xs) andalso is_sorted(Cmp, Xs).

cmp(X, Y) ->
    X /= Y andalso ordsets:is_subset(X, Y).

