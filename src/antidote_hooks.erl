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

%% Pre-Commit hooks
%% ------------------------
%% Pre-commit hooks are executed before an object is being updated. If
%% pre-commit hook fails, the entire transaction is aborted.
%%
%% The commit hook function must be a function which take one argument which is
%% of type update_object() and returns update_object().
%% The update operation corresponds to a data-type specific operation as defined
%% in the respective CRDT specification.
%%
%% fun (update_object()) -> {ok, update_object()} | {error, Reason}.
%% -type update_object() :: {{key(), bucket()}, crdt_type(), op()}

%% The returned update_object() is used for updating the object.
%% An example commit hook function is written in
%% antidote_hooks.erl : test_increment_hook/1.

%% Post-commit hooks
%% ------------------------
%% Post commit hooks are executed after the transaction is successfully committed
%% and before the reply is sent to the client. Currently if post commit hook is
%% failed, it is ignored and transaction is still considered to be successfully
%% committed and the client is notified of the success.
%% An example commit hook function is written in
%% antidote_hooks.erl: test_post_hook/1

%% An example of how to use these interfaces is given in
%% riak_test/commit_hooks_test.erl

-module(antidote_hooks).

-include("antidote.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([register_pre_hook/3,
         register_post_hook/3,
         get_hooks/2,
         unregister_hook/2,
         execute_pre_commit_hook/3,
         execute_post_commit_hook/3
        ]).

-ifdef(TEST).
-export([test_commit_hook/1,
         test_increment_hook/1,
         test_post_hook/1]).
-endif.

-define(PREFIX_PRE, {commit_hooks, pre}).
-define(PREFIX_POST, {commit_hooks, post}).

-spec register_post_hook(bucket(), module_name(), function_name()) ->
      ok | {error, reason()}.
register_post_hook(Bucket, Module, Function) ->
    register_hook(?PREFIX_POST, Bucket, Module, Function).

-spec register_pre_hook(bucket(), module_name(), function_name()) ->
      ok | {error, reason()}.
register_pre_hook(Bucket, Module, Function) ->
    register_hook(?PREFIX_PRE, Bucket, Module, Function).

%% Overwrites the previous commit hook
register_hook(Prefix, Bucket, Module, Function) ->
    case erlang:function_exported(Module, Function, 1) of
        true ->
            riak_core_metadata:put(Prefix, Bucket, {Module, Function}),
            ok;
        false ->
            {error, function_not_exported}
    end.

-spec unregister_hook(pre_commit | post_commit, bucket()) -> ok.
unregister_hook(pre_commit, Bucket) ->
    riak_core_metadata:delete(?PREFIX_PRE, Bucket);

unregister_hook(post_commit, Bucket) ->
    riak_core_metadata:delete(?PREFIX_POST, Bucket).

get_hooks(pre_commit, Bucket) ->
    riak_core_metadata:get(?PREFIX_PRE, Bucket);

get_hooks(post_commit, Bucket) ->
    riak_core_metadata:get(?PREFIX_POST, Bucket).

-spec execute_pre_commit_hook(term(), type(), op_param()) ->
        {term(), type(), op_param()} | {error, reason()}.
execute_pre_commit_hook({Key, Bucket}, Type, Param) ->
    Hook = get_hooks(pre_commit, Bucket),
    case Hook of
        undefined ->
            {{Key, Bucket}, Type, Param};
        {Module, Function} ->
            try Module:Function({{Key, Bucket}, Type, Param}) of
                {ok, Res} -> Res
            catch
                _:Reason -> {error, {pre_commit_hook, Reason}}
            end
    end;
%% The following is kept to be backward compatible with the old
%% interface where buckets are not used
execute_pre_commit_hook(Key, Type, Param) ->
    {Key, Type, Param}.

-spec execute_post_commit_hook(term(), type(), op_param()) ->
            {term(), type(), op_param()} | {error, reason()}.
execute_post_commit_hook({Key, Bucket}, Type, Param) ->
    Hook = get_hooks(post_commit, Bucket),
    case Hook of
        undefined ->
            {{Key, Bucket}, Type, Param};
        {Module, Function} ->
            try Module:Function({{Key, Bucket}, Type, Param}) of
                {ok, Res} -> Res
            catch
                _:Reason -> {error, {post_commit_hook, Reason}}
            end
    end;
execute_post_commit_hook(Key, Type, Param) ->
    {Key, Type, Param}.

-ifdef(TEST).
%% The following functions here provide commit hooks for the testing (test/commit_hook_SUITE).

test_commit_hook(Object) ->
    lager:info("Executing test commit hook"),
    {ok, Object}.

test_increment_hook({{Key, Bucket}, antidote_crdt_counter_pn, {increment, 1}}) ->
    {ok, {{Key, Bucket}, antidote_crdt_counter_pn, {increment, 2}}}.

test_post_hook({{Key, Bucket}, Type, OP}) ->
    {ok, _CT} = antidote:update_objects(ignore, [], [{{Key, antidote_crdt_counter_pn, commitcount}, increment, 1}]),
    {ok, {{Key, Bucket}, Type, OP}}.

-endif.
