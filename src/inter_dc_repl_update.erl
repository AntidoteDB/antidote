-module(inter_dc_repl_update).

-include("inter_dc_repl.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init_state/0, enqueue_update/2, process_queue/1]).

init_state() ->
    {ok, #recvr_state{lastRecvd = orddict:new(), lastCommitted = orddict:new(), recQ = orddict:new(), dcs = [1,2]} }.

enqueue_update({Key, {Op, Ts, DepV}, FromDC}, State = #recvr_state{lastRecvd = LastRecvd, recQ = RecQ}) ->
    LastRecvdNew = set(FromDC, Ts, LastRecvd),
    RecQNew = enqueue(FromDC, {Key,{Op,Ts,DepV}}, RecQ),
    {ok, State#recvr_state{lastRecvd = LastRecvdNew, recQ = RecQNew}}.

%% Process one update from Q for each DC each Q. This method must be called repeatedly 
%% inorder to process all updates
process_queue(State=#recvr_state{recQ = RecQ}) ->
    NewState = orddict:fold( 
         fun(K, V, Res) -> 
             process_q_dc(K, V, Res)
         end, State, RecQ),
    {ok, NewState}.

%% private functions

%%Takes one update from DC queue, checks whether its depV is satisfied and apply the update locally.
process_q_dc(Dc, DcQ, StateData=#recvr_state{lastCommitted = LastCTS}) ->
    case queue:is_empty(DcQ) of
    false ->
        {Key, {Op, Ts, DepV}} = queue:get(DcQ),
        case orddict:find(Dc, LastCTS) of  % Check for duplicate 
        {ok, CTS} -> 
            if Ts > CTS ->
                   case check_dep(DepV, LastCTS) of
                       true ->
                       floppy_rep_vnode:append(Key, Op), %TODO add error handling if append failed
                       {ok, NewState} = finish_update_dc(Dc, DcQ, Ts, StateData),
                       NewState;
                       false -> StateData            
                   end ;        
               true ->
                   {ok, NewState} = finish_update_dc(Dc, DcQ, CTS, StateData), %Duplicate request, drop from queue
                   NewState
            end;
        _ -> 
             case check_dep(DepV, LastCTS) of
             true ->
                 floppy_rep_vnode:append(Key, Op), %TODO add error handling if append failed
                 {ok, NewState} = finish_update_dc(Dc, DcQ, Ts, StateData),
                 NewState;
             false -> StateData          
             end            
        end;

    true -> 
        StateData
    end.     

finish_update_dc(Dc, DcQ, Cts, State=#recvr_state{lastCommitted = LastCTS, recQ = RecQ}) ->
    DcQNew = queue:drop(DcQ),
    RecQNew = set(Dc, DcQNew, RecQ),
    LastCommNew = set(Dc, Cts, LastCTS),
    {ok, State#recvr_state{lastCommitted = LastCommNew, recQ = RecQNew}}.

%% Checks depV against the committed timestamps
check_dep(DepV, LastCTS) ->
                        %TODO Check dependency.
    DepVector = orddict:from_list(DepV),
    orddict:fold(
      fun(K,V,Res) -> 
          case orddict:find(K,LastCTS) of
          {ok,Ts} -> 
              if V > Ts -> false;
             true -> Res
              end;
          error -> false
          end
      end, true, DepVector).

%%Set a new value to the key.
set(Key, Value, Orddict) ->
    orddict:update(Key, fun(_Old) -> Value end, Value, Orddict).

%%Put a value to the Queue corresponding to Dc in RecQ orddict
enqueue(Dc, Data, RecQ) ->
    case orddict:find(Dc, RecQ) of
    {ok, Q} ->
        Q2 = queue:in(Data, Q),
        set(Dc, Q2, RecQ);
    error -> %key does not exist
        Q = queue:new(),
        Q2 = queue:in(Data,Q),
        set(Dc, Q2, RecQ)
    end.

-ifdef(TEST).
get_first(Dc, RecQ) ->
    case orddict:find(Dc, RecQ) of
    {ok, DcQ} ->
        case queue:is_empty(DcQ) of
        false ->
            {Key, {Op, Ts, DepV}} = queue:get(DcQ),
            {ok, {Key, {Op, Ts, DepV}}};
        true ->
            {ok, null}
        end;
    error ->
        {error, queue_not_found}
    end.

%% Eunit Tests


init_update_test_() ->
    {ok, State1} = init_state(),
    Payload1 = {{increment,a}, 1, {0,0}},
    {ok, State2} = enqueue_update({a, Payload1, 1}, State1),
    RecQ1 = State2#recvr_state.recQ,

    Payload2 = {{decrement,a}, 1, {0,0}},
    {ok, State3} = enqueue_update({a, Payload2, 1}, State2),
    RecQ2 = State3#recvr_state.recQ,

    Payload3 = {{increment,b},2,{0,0}},
    {ok, State4} = enqueue_update({a, Payload3, 2}, State3),
    RecQ3 = State4#recvr_state.recQ,
    [?_assert({ok, {a,Payload1}} =:= get_first(1, RecQ1)),
     ?_assert({ok, {a,Payload1}} =:= get_first(1, RecQ2)),
     ?_assert({ok, {a,Payload3}} =:= get_first(2, RecQ3))].


finish_update_dc_test_() ->
    {ok, State1 } = init_state(),
    Payload1 = {{increment,a},1,{0,0}},
    Dc = 1,
    Ts = 1,
    {ok, State2} = enqueue_update({a, Payload1, Dc}, State1),
    {ok,DcQ} = orddict:find(Dc, State2#recvr_state.recQ), 
    {ok, State3} = finish_update_dc(Dc, DcQ, Ts, State2),
    RecQ = State3#recvr_state.recQ,
    [?_assert({ok,null} =:= get_first(Dc,RecQ))] .

check_dep_test_() ->
    DepV = [{1,4}, {2,3}, {3,1}, {4,2}],
    LastCTS1 = orddict:from_list([{1,3},{2,2},{3,0},{4,0}]),
    LastCTS2 = orddict:from_list([{1,4},{2,2},{3,1},{4,2}]),
    LastCTS3 = orddict:from_list([{1,4},{2,3},{3,1},{4,3}]),
    LastCTS4 = orddict:from_list([{1,4}]),
    [ ?_assert(check_dep(DepV, LastCTS1) =:= false),
      ?_assert(check_dep(DepV, LastCTS2) =:= false),
      ?_assert(check_dep(DepV, LastCTS3) =:= true),
      ?_assert(check_dep(DepV, LastCTS4) =:= false)
    ].

-endif.
