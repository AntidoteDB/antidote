-module(log_utilities).
-include("floppy.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([get_logid_from_partition/1,
         get_logid_from_key/1,
         get_preflist_from_logid/1,
         get_apl_from_logid/2,
         remove_node_from_preflist/1]).

get_logid_from_partition(Partition) ->
    HashedKey = get_hashedkey_from_partition(Partition),
    Preflist = get_primaries_preflist(HashedKey),
    remove_node_from_preflist(Preflist).

get_logid_from_key(Key) ->
    HashedKey = riak_core_util:chash_key({?BUCKET,
                                       term_to_binary(Key)}),
    Preflist = get_primaries_preflist(HashedKey),
    remove_node_from_preflist(Preflist).

get_preflist_from_logid(LogId) ->
    Partition = hd(LogId),
    HashedKey = get_hashedkey_from_partition(Partition),
    get_primaries_preflist(HashedKey).

get_apl_from_logid(LogId, Service) ->
    Partition = hd(LogId),
    HashedKey = get_hashedkey_from_partition(Partition),
    PreflistAnn = riak_core_apl:get_primary_apl(HashedKey, ?N, Service),
    [IndexNode || {IndexNode, _} <- PreflistAnn].
 
get_hashedkey_from_partition(Partition) ->
    case Partition of
        0 ->
            ?MAXRING;
        _ ->
            Partition - 1
    end.

%% @doc Returns the preflist with the primary vnodes. No matter they are up or down.
get_primaries_preflist(HashedKey)->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Itr = chashbin:iterator(HashedKey, CHBin),
    {Primaries, _} = chashbin:itr_pop(?N, Itr),
    Primaries.

%% @doc remove_node_from_preflist:  From each element of the input preflist, the node identifier is removed
%%      Input:  Preflist: list of pairs {Partition, Node}
%%      Return: List of Partition identifiers
-spec remove_node_from_preflist(Preflist::[{Index::integer(), Node::term()}]) -> [integer()].
remove_node_from_preflist(Preflist) ->
    F = fun(Elem, Acc) ->
                {P,_} = Elem,
                lists:append(Acc, [P])
        end,
    lists:foldl(F, [], Preflist).

-ifdef(TEST).

%% @doc Testing remove_node_from_preflist
remove_node_from_preflist_test()->
    Preflist = [{partition1, node},{partition2, node},{partition3, node}],
    ?assertEqual([partition1, partition2, partition3], remove_node_from_preflist(Preflist)).

-endif.
