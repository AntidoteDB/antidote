%%%-------------------------------------------------------------------
%%% Created : 04. September 2018
%%%-------------------------------------------------------------------
-module(antidote_utils).

-include_lib("eunit/include/eunit.hrl").

-define(TYPE_PNC, antidote_crdt_counter_pn).
-define(TYPE_B, antidote_crdt_counter_b).

%% API
-export([
    increment_pn_counter/3,

    read_pn_counter/3,
    read_b_counter/3,
    read_b_counter_commit/4
]).


increment_pn_counter(Node, Key, Bucket) ->
    Obj = {Key, ?TYPE_PNC, Bucket},
    WriteResult = rpc:call(Node, antidote, update_objects, [ignore, [], [{Obj, increment, 1}]]),
    ?assertMatch({ok, _}, WriteResult),
    ok.


read_pn_counter(Node, Key, Bucket) ->
    Obj = {Key, ?TYPE_PNC, Bucket},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [ignore, [], [Obj]]),
    {Value, CommitTime}.


read_b_counter(Node, Key, Bucket) ->
    read_b_counter_commit(Node, Key, Bucket, ignore).

read_b_counter_commit(Node, Key, Bucket, CommitTime) ->
    Obj = {Key, ?TYPE_B, Bucket},
    {ok, [Value], CommitTime} = rpc:call(Node, antidote, read_objects, [CommitTime, [], [Obj]]),
    {?TYPE_B:permissions(Value), CommitTime}.
