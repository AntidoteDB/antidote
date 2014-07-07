-module(vectorclock).

-include("floppy.hrl").

-export([get_clock/1, update_clock/3, get_clock_by_key/1,
         is_greater_than/2,
         get_clock_of_dc/2,
         get_clock_node/1,
         from_list/1]).

-export_type([vectorclock/0]).

-type vectorclock() :: dict().

get_clock_by_key(Key) ->
    DocIdx = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, vectorclock),
    lager:info("Preflist of vectorclokc ~p", [PrefList]),
    [{IndexNode, _Type}] = PrefList,
    lager:info("Get Clock"),
    riak_core_vnode_master:sync_command(IndexNode, {get_clock}, vectorclock_vnode_master).

-spec get_clock(Partition :: non_neg_integer()) -> {ok, vectorclock()}.
get_clock(Partition) ->
    Logid = log_utilities:get_logid_from_partition(Partition),
    Preflist = log_utilities:get_apl_from_logid(Logid, vectorclock),
    Indexnode = hd(Preflist),
    case riak_core_vnode_master:sync_command(Indexnode, {get_clock}, vectorclock_vnode_master) of
        {ok, Clock} ->
            {ok, Clock};
        {error, Reason} ->
            lager:info("Update vector clock failed: ~p",[Reason]),
            {error, Reason} 
    end.

get_clock_node(Node) ->
    Preflist = riak_core_apl:active_owners(vectorclock),
    Prefnode = [{Partition, Node1} || {{Partition, Node1},_Type} <- Preflist, Node1 =:= Node], %Partitions in current node
    %% Take a random vnode
    {A1,A2,A3} = now(),
    _Seed = random:seed(A1, A2, A3),
    Index = random:uniform(length(Prefnode)),
    VecNode = lists:nth(Index, Prefnode),
    riak_core_vnode_master:sync_command(VecNode, {get_clock}, vectorclock_vnode_master).

-spec update_clock(Partition :: non_neg_integer(), Dc_id :: term(), Timestamp :: non_neg_integer()) -> {ok, vectorclock()} | {error, term()}.
update_clock(Partition, Dc_id, Timestamp) -> 
    Logid = log_utilities:get_logid_from_partition(Partition),
    Preflist = log_utilities:get_apl_from_logid(Logid, vectorclock),
    Indexnode = hd(Preflist),
    case riak_core_vnode_master:sync_command(Indexnode, {update_clock, Dc_id, Timestamp}, vectorclock_vnode_master) of
        {ok, Clock} ->
            {ok, Clock};
        {error, Reason} ->
            lager:info("Update vector clock failed: ~p",[Reason]),
            {error, Reason}
    end.

%% @doc Return true if Clock1 > Clock2
-spec is_greater_than(Clock1 :: vectorclock(), Clock2 :: vectorclock()) -> boolean().
is_greater_than(Clock1, Clock2) ->
    dict:fold( fun(Dcid, Time2, Result) ->
                       case dict:find(Dcid, Clock1) of
                           {ok, Time1} ->
                               case Time1 > Time2 of
                                   true ->
                                       Result;
                                   false ->
                                       false
                               end;
                           error -> %%Localclock has not observered some dcid
                              false 
                       end 
               end,
               true, Clock2).

get_clock_of_dc(Dcid, VectorClock) ->
    dict:find(Dcid, VectorClock).

from_list(List) ->
    dict:from_list(List).
                               
