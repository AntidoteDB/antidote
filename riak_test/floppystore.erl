-module(floppystore).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    Nodes = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    lager:info("Test run where nothing really happens."),

    pass.
