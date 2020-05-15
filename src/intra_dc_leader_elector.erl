%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------
%% Gonçalo Cabrita's chain replication implementation:
%%   This module is responsible for maintaining the cluster of consensus
%%   groups for each partition, and keeping track of the nodes that may
%%   have partitioned or crashed.
%%   When the riak core ring changes the cluster should automatically
%%   update as well.

-module(intra_dc_leader_elector).
-include_lib("kernel/include/logger.hrl").
-behaviour(gen_server).

%% External API
-export([
    ring_changed/0,
    get_cluster/0,
    get_cluster/1,
    get_preflist/1,
    get_downed/0
]).

%% Internal API
-export([
    init/1,
    start_link/0,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    run_heartbeat/0,
    run_failure/1
]).

-define(HEARTBEAT_TIMER, 1000).
-define(FAILURE_TIMER, 1500).
-define(DEFAULT_REPLICAS, 1).

%% GenServer state
-record(state, {
    cluster :: [node()], % the list of unique antidote nodes in the cluster
    downed :: [node()], % the list of crashed/partitioned nodes in the cluster
    failure_timers :: map(), % map of nodes to their failure timers
    heartbeat_timer :: reference(), % this node's heartbeat timer
    partitions :: map() % map of partitions to their group status
}).

%%%% External API

% Signals that the riak core ring has changed, should only be called from antidote_ring_event_handler
ring_changed() ->
    gen_server:cast({global, generate_server_name(node())}, ring_changed).

% Returns the current cluster information for all of the groups
get_cluster() ->
    gen_server:call({global, generate_server_name(node())}, get_cluster).

% Returns the current cluster information for a the given group (partition)
get_cluster(Partition) ->
    gen_server:call({global, generate_server_name(node())}, {get_cluster, Partition}).

% Returns the preference list for the given partition
get_preflist(Partition) ->
    gen_server:call({global, generate_server_name(node())}, {get_preflist, Partition}).

%%%% Internal API

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({global, generate_server_name(node())}, ?MODULE, [], []).

init([]) ->
	Replicas = application:get_env(antidote, intra_dc_replicas, ?DEFAULT_REPLICAS),
    {ok, recompute_groups(Replicas, #state{downed = [], heartbeat_timer = none, failure_timers = #{}})}.

handle_call(get_downed, _From, #state{downed = Downed} = State) ->
    {reply, Downed, State};
handle_call(get_cluster, _From, #state{partitions = Partitions} = State) ->
    {reply, Partitions, State};
handle_call({get_cluster, Partition}, _From, #state{partitions = Partitions} = State) ->
    {reply, maps:get(Partition, Partitions), State};
handle_call({get_preflist, Partition}, _From, #state{partitions = Partitions} = State) ->
    {reply, maps:get(current, maps:get(Partition, Partitions)), State};
handle_call(_Info, _From, State) ->
    {reply, error, State}.

handle_cast(ring_changed, State) ->
	Replicas = application:get_env(antidote, intra_dc_replicas, ?DEFAULT_REPLICAS),
    {noreply, recompute_groups(Replicas, State)};
handle_cast({heartbeat, Node}, State = #state{downed = Downed, partitions = Partitions}) ->
    case lists:member(Node, Downed) of
        true ->
            NDowned = Downed -- [Node],
            NPartitions = maps:map(fun(_K, V) ->
                Members = maps:get(membership, V),
                Current = maps:get(current, V),
                PossibleN = hd(Members),
                NCurrent = case PossibleN of
                    {_, Node} -> % recovering node is original leader
                        [{_, Head} | Tail] = Current,
                        [OriginalHead] = lists:filter(fun({_, N}) ->
                            N == Head
                        end, Members),
                        [PossibleN, OriginalHead | Tail];
                    _ ->
                        lists:filter(fun({_P, N}) ->
                            not lists:member(N, NDowned)
                        end, Members)
                end,
                maps:put(current, NCurrent, V)
            end, Partitions),
            {noreply, set_failure_timer(Node, State#state{downed = NDowned, partitions = NPartitions})};
        false ->
            {noreply, set_failure_timer(Node, State)}
    end;
handle_cast(run_heartbeat, State = #state{cluster = Cluster}) ->
    Nodes = Cluster -- [node()],
    lists:map(fun(N) ->
        spawn(fun() ->
            gen_server:cast({global, generate_server_name(N)}, {heartbeat, node()})
        end)
    end, Nodes),
    {noreply, set_heartbeat_timer(State)};
handle_cast({run_failure, Node}, State = #state{downed = Downed, partitions = Partitions}) ->
    NPartitions = maps:map(fun(K, V) ->
        Members = maps:get(membership, V),
        Current = maps:get(current, V),
        FCurrent = lists:filter(fun({_P, N}) -> N /= Node end, Current),
        NCurrent = case hd(Members) of
            {_, Node} -> % original leader failed
                case FCurrent of
                    [] -> [];
                    _ ->
                        [{_, Head} | Tail] = FCurrent,
                    [{K, Head} | Tail] % change partition of 'backup node' to force the new vnode to launch
                end;
            _ -> FCurrent
        end,
        maps:put(current, NCurrent, V)
    end, Partitions),
    {noreply, State#state{downed = [Node | Downed], partitions = NPartitions}};
handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%% Private

run_heartbeat() ->
    gen_server:cast({global, generate_server_name(node())}, run_heartbeat).

run_failure(Node) ->
    gen_server:cast({global, generate_server_name(node())}, {run_failure, Node}).

get_downed() ->
    gen_server:call({global, generate_server_name(node())}, get_downed).

%% Creates the initial replication groups for a replication factor of N
recompute_groups(N, State = #state{downed = Downed}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Cluster = riak_core_ring:all_members(Ring),
    NN = case length(Cluster) < N of
        true -> 1;
        false -> N
    end,
    AllPrefs = riak_core_ring:all_preflists(Ring, NN),
    %Partitions is a map where the key is a partition and the value is a map from
    %an initial membership configuration to a current configuration
    Partitions = lists:foldl(fun([{Partition, Leader} | _] = Membership, Acc) ->
        FCurrent = lists:filter(fun({_P, MN}) -> not lists:member(MN, Downed) end, Membership),
        Current = case lists:member(Leader, Downed) of
            true -> % original leader failed
                case FCurrent of
                    [] -> FCurrent;
                    _ ->
                        [{_, Head} | Tail] = FCurrent,
                        [{Partition, Head} | Tail] % change partition of 'backup node' to force the new vnode to launch
                end;
            false -> FCurrent
        end,
        Map = #{
            membership => Membership,
            current => Current
        },
        maps:put(Partition, Map, Acc)
    end, #{}, AllPrefs),
    set_timers(State#state{downed = Downed, partitions = Partitions, cluster = Cluster}).

%% Generates a server name from a given node name.
generate_server_name(Node) ->
    list_to_atom("intradc_" ++ atom_to_list(Node)).

set_timers(State) ->
    set_failure_timers(set_heartbeat_timer(State)).

del_heartbeat_timer(State = #state{heartbeat_timer = T}) ->
    _ = timer:cancel(T),
    State#state{heartbeat_timer = none}.

set_heartbeat_timer(State = #state{cluster = Cluster}) when length(Cluster) < 5 ->
    State;
set_heartbeat_timer(State) ->
    S1 = del_heartbeat_timer(State),
    {ok, NT} = timer:apply_after(?HEARTBEAT_TIMER, intra_dc_leader_elector, run_heartbeat, []),
    S1#state{heartbeat_timer = NT}.

set_failure_timers(State = #state{cluster = Cluster}) when length(Cluster) < 5 ->
    State;
set_failure_timers(State = #state{cluster = Cluster, failure_timers = F}) ->
    maps:map(fun(_K, V) ->
        _ = timer:cancel(V)
    end, F),
    NF = lists:foldl(fun(N, Acc) ->
        {ok, T} = timer:apply_after(?FAILURE_TIMER, intra_dc_leader_elector, run_failure, [N]),
        maps:put(N, T, Acc)
    end, #{}, Cluster -- [node()]),
    State#state{failure_timers = NF}.

set_failure_timer(Node, State = #state{failure_timers = F}) ->
    case maps:is_key(Node, F) of
        true ->
            T = maps:get(Node, F),
            _ = timer:cancel(T);
        false -> ok
    end,
    {ok, NT} = timer:apply_after(?FAILURE_TIMER, intra_dc_leader_elector, run_failure, [Node]),
    NF = maps:put(Node, NT, F),
    State#state{failure_timers = NF}.
