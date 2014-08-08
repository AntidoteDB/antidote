-module(log_utilities).

-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition()  :: integer().

-export([get_logid_from_partition/1,
         get_logid_from_key/1,
         get_preflist_from_logid/1,
         get_apl_from_logid/2,
         remove_node_from_preflist/1]).

%% @doc get_logid_from_partition computes the log identifier from the
%%      partition id.
%%      Input:  Partition:  The partition identifier
%%      Return: Log id
%%
-spec get_logid_from_partition(partition()) -> logid().
get_logid_from_partition(Partition) ->
    Preflist = get_primaries_preflist(Partition),
    remove_node_from_preflist(Preflist).

%% @doc get_logid_from_key computes the log identifier from a key
%%      Input:  Key:    The key from which the log id is going to be computed
%%      Return: Log id
%%
-spec get_logid_from_key(key()) -> logid().
get_logid_from_key(Key) ->
    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PreflistAnn = get_primaries_preflist(HashedKey),
    remove_node_from_preflist(PreflistAnn).

%% @doc get_preflist_from_logid computes the preflist to which a logId belongs
%%      only primaries no matter down or up
%%      Input:  A log id
%%      Return: The primaries preflist
%%
-spec get_preflist_from_logid(logid()) -> preflist().
get_preflist_from_logid(LogId) ->
    Partition = hd(LogId),
    get_primaries_preflist(Partition).

%% @doc get_apl_from_logid computes the preflist to which a logId belongs
%%      only primaries and active nodes
%%      Input:  LogId: A log id
%%              Service: The service in the riak_core application
%%      Return: The active primaries preflist
%%
-spec get_apl_from_logid(logid(), atom()) -> preflist().
get_apl_from_logid(LogId, Service) ->
    Partition = hd(LogId),
    PreflistAnn = riak_core_apl:get_primary_apl(Partition, ?N, Service),
    [IndexNode || {IndexNode, _} <- PreflistAnn].

%% @doc get_primaries_preflist returns the preflist with the primary
%%      vnodes. No matter they are up or down.
%%      Input:  A hashed key
%%      Return: The primaries preflist
%%
-spec get_primaries_preflist(integer()) -> preflist().
get_primaries_preflist(HashedKey)->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Itr = chashbin:iterator(HashedKey, CHBin),
    {Primaries, _} = chashbin:itr_pop(?N, Itr),
    Primaries.

%% @doc remove_node_from_preflist: From each element of the input
%%      preflist, the node identifier is removed
%%      Input:  Preflist: list of pairs {Partition, Node}
%%      Return: List of Partition identifiers
%%
-spec remove_node_from_preflist([{integer(), term()}]) -> [integer()].
remove_node_from_preflist(Preflist) ->
    F = fun(Elem, Acc) ->
            {P,_} = Elem,
            lists:append(Acc, [P])
    end,
    lists:foldl(F, [], Preflist).

-ifdef(TEST).

%% @doc Testing remove_node_from_preflist
remove_node_from_preflist_test()->
    Preflist = [{partition1, node},
                {partition2, node},
                {partition3, node}],
    ?assertEqual([partition1, partition2, partition3],
                 remove_node_from_preflist(Preflist)).

-endif.
