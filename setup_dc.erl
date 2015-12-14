-module(setup_dc).

-export([setup_dc_manager/0]).


setup_dc_manager() ->
    Clusters= [['dev1@127.0.0.1'], ['dev2@127.0.0.1'], ['dev3@127.0.0.1']],
    Ports=[12345, 12346, 12347],
    Clean = true,
    case Clean of
        true ->
            {_, CombinedList} = lists:foldl(fun(Cluster, {Index, Result}) ->
                                                {Index+1, Result ++ [{Cluster, lists:nth(Index, Ports)}]}
                                            end, {1, []}, Clusters),
            Heads = lists:foldl(fun({Cluster, Port}, Acc) ->
                                    Node = hd(Cluster),
                                    {ok, DC} = rpc:call(Node, inter_dc_manager, start_receiver,[Port]),
                                    Acc ++ [{Node, DC}]
                                end, [], CombinedList),
            lists:foreach(fun({Node, DC}) ->
                            Sublist = lists:subtract(Heads, [{Node, DC}]),
                            lists:foreach(fun({_, RemoteDC}) ->
                                            ok = rpc:call(Node, inter_dc_manager, add_dc,[RemoteDC])
                                          end, Sublist)
                          end, Heads),
            ok;
        false ->
            ok
    end.
