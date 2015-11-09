%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(eiger_test).

-export([confirm/0,
         eiger_spawn_read/3]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes1] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes1]),

    eiger_test1(Nodes1),
    [Nodes2] = common:clean_clusters([Nodes1]),
    eiger_test2(Nodes1),
    [Nodes3] = common:clean_clusters([Nodes2]),
    eiger_test3(Nodes2),
    [Nodes4] = common:clean_clusters([Nodes3]),
    eiger_test4(Nodes3),
    [Nodes5] = common:clean_clusters([Nodes4]),
    eiger_test5(Nodes5),
    rt:clean_cluster(Nodes5),
    pass.

%% @doc The following function tests Eiger very basic behaviour
eiger_test1(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test1 started"),
    %% Empty transaction works,
    Result0=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[], []]),
    ?assertMatch({ok, empty}, Result0),

    Result1=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[]]),
    ?assertMatch({ok, empty}, Result1),

    % A simple read returns empty
    Result11=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[key1]]),
    ?assertMatch({ok, [{key1, <<>>}], _}, Result11),

    %% Read what you wrote
    Result2=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{key1, 1}, {key2, 2}], []]),
    ?assertMatch({ok, _}, Result2),
    {ok, Result22, _}=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[key1, key2]]),
    ?assertMatch(true, compare_multiple_results([{key1, 1},{key2, 2}], Result22)),
    pass.

eiger_test2(Nodes) ->
    FirstNode = hd(Nodes),
    Key = eiger_test2,
    lager:info("Test2 started"),
    Result0=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key,  1}], []]),
    ?assertMatch({ok, _}, Result0),
    {ok, Coord}=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key, 2}], [], debug]),
    LastNode= lists:last(Nodes),
    spawn(eiger_test, eiger_spawn_read, [LastNode, [Key], self()]),
    timer:sleep(2000),
    
    Result1=rpc:call(FirstNode, antidote, eiger_committx,
                    [Coord]),
    ?assertMatch({ok, _EVT2}, Result1),
    receive
        Result2 ->
            ?assertMatch({ok, [{Key, 2}], _}, Result2)
    end,
    Result3=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[Key]]),
    ?assertMatch({ok, [{Key, 2}], _}, Result3),
    pass.

eiger_test3(Nodes) ->
    FirstNode = hd(Nodes),
    Key = eiger_test4,
    lager:info("Test3 started"),
    Result0=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key, 1}], []]),
    ?assertMatch({ok, _}, Result0),
    {ok, Coord}=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key, 2}], [], debug]),
    LastNode= lists:last(Nodes),
    spawn(eiger_test, eiger_spawn_read, [LastNode, [Key], self()]),
    timer:sleep(2000),
    {ok, Coord2}=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key, 3}], [], debug]),

    Result1=rpc:call(FirstNode, antidote, eiger_committx,
                    [Coord]),
    ?assertMatch({ok, _EVT2}, Result1),
    Result2=rpc:call(FirstNode, antidote, eiger_committx,
                    [Coord2]),
    ?assertMatch({ok, _EVT3}, Result2),
    receive
        Result3 ->
            ?assertMatch({ok, [{Key, 2}], _}, Result3)
    end,
    Result4=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[Key]]),
    ?assertMatch({ok, [{Key, 3}], _}, Result4),
    pass.

eiger_test4(Nodes) ->
    FirstNode = hd(Nodes),
    Key1 = eiger_test5a,
    Key2 = eiger_test5b,
    lager:info("Test4 started"),
    Result0=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key1, 1},{Key2, 1}], []]),
    ?assertMatch({ok, _}, Result0),
    {ok, Coord}=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key1, 2}], [], debug]),
    LastNode= lists:last(Nodes),
    spawn(eiger_test, eiger_spawn_read, [LastNode, [Key1, Key2], self()]),
    timer:sleep(2000),

    Result1=rpc:call(FirstNode, antidote, eiger_committx,
                    [Coord]),
    ?assertMatch({ok, _EVT2}, Result1),
    receive
        {ok, Result2, _} ->
            ?assertMatch(true, compare_multiple_results([{Key1, 1},{Key2, 1}], Result2))
    end,
    {ok, Result3, _}=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[Key1, Key2]]),
    ?assertMatch(true, compare_multiple_results([{Key1, 2},{Key2, 1}], Result3)),
    pass.

eiger_test5(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("Test5 started"),
    {IndexNode1, Keys1} = n_keys_same_vnode(FirstNode, 2, [], [], empty),
    Key1a = hd(Keys1),
    Key1b = lists:last(Keys1),
    {_IndexNode2, Keys2} = n_keys_same_vnode(FirstNode, 2, [IndexNode1], [], empty),
    Key2a = hd(Keys2),
    Key2b = lists:last(Keys2),
    
    done = n_updates(FirstNode, Key1a, 5, 0),
    done = n_updates(FirstNode, Key1b, 5, 0),
    done = n_updates(FirstNode, Key2a, 20, 0),
    done = n_updates(FirstNode, Key2b, 1, 0),

    {ok, Result3, _}=rpc:call(FirstNode, antidote, eiger_readtx,
                    [[Key1a, Key2a]]),
    ?assertMatch(true, compare_multiple_results([{Key1a, 5},{Key2a, 20}], Result3)),
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).

n_keys_same_vnode(FirstNode, N, NotIn, Keys, Previous) ->
    case length(Keys) of
        N ->
            {Previous, Keys};
        _ ->
            Key = get_random_key(),
            Preflist = rpc:call(FirstNode, log_utilities, get_preflist_from_key, [Key]),
            IndexNode = hd(Preflist),
            case lists:member(IndexNode, NotIn) orelse lists:member(Key, Keys) of
                true ->
                    n_keys_same_vnode(FirstNode, N, NotIn, Keys, Previous);
                false ->
                    case Previous of
                        empty ->
                            n_keys_same_vnode(FirstNode, N, NotIn, Keys ++ [Key], IndexNode);
                        IndexNode ->
                            n_keys_same_vnode(FirstNode, N, NotIn, Keys ++ [Key], IndexNode);
                        _ ->
                            n_keys_same_vnode(FirstNode, N, NotIn, Keys, Previous)
                    end
            end
    end.

n_updates(FirstNode, Key, N, SoFar) ->
    case SoFar of
        N ->
            done;
        _ ->
            Result0=rpc:call(FirstNode, antidote, eiger_updatetx,
                    [[{Key, SoFar+1}], []]),
            ?assertMatch({ok, _}, Result0),
            n_updates(FirstNode, Key, N, SoFar+1)
    end.


compare_multiple_results([], _ActualResults) ->
    true;

compare_multiple_results([Expected|Rest], ActualResults) ->
    case lists:member(Expected, ActualResults) of
        true ->
            compare_multiple_results(Rest, ActualResults);
        false ->
            false
    end.

eiger_spawn_read(Node, Keys, Sender) ->
    Result=rpc:call(Node, antidote, eiger_readtx, [Keys]),
    Sender ! Result.
