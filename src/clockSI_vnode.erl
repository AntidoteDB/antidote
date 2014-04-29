-module(clockSI_vnode).
-behaviour(riak_core_vnode).
-include("floppy.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 %API begin
	 read_data_item/3,
	 update_data_item/4,
	 prepare/2,
	 commit/2,
	 %API end
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handleOp/6,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition,log}).





%%
%% Calculate the time difference (in microseconds) of two
%% erlang:now() timestamps, T2-T1.
%%
%-spec now_diff(T2, T1) -> Tdiff when
%      T1 :: erlang:timestamp(),
%      T2 :: erlang:timestamp(),
%      Tdiff :: integer().
%now_diff({A2, B2, C2}, {A1, B1, C1}) ->
%    ((A2-A1)*1000000 + B2-B1)*1000000 + C2-C1.







%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
    
    
    
handleOp(Leader, ToReply, OpType, Key, Param, SnapshotTime) ->
    	riak_core_vnode_master:command(Leader,
                                   {operate, ToReply, OpType, Key, Param, SnapshotTime},
                                   ?REPMASTER).

read_data_item(Node, Transaction, Key) ->
    riak_core_vnode_master:sync_command(Node, {read_data_item, Transaction, Key}, ?CLOCKSIMASTER).

update_data_item(Node, Transaction, Key, Op) ->
    riak_core_vnode_master:sync_command(Node, {update_data_item, Transaction, Key, Op}, ?CLOCKSIMASTER).

prepare(Node, Transaction) ->
    riak_core_vnode_master:sync_command(Node, {prepare, Transaction}, ?CLOCKSIMASTER).

commit(Node, Transaction) ->
    riak_core_vnode_master:sync_command(Node, {commit, Transaction}, ?CLOCKSIMASTER).





init([Partition]) ->

% It generates a dets file to store active transactions.

	
    TxLogFile = string:concat(integer_to_list(Partition), "clockSI_tx"),
    TxLogPath = filename:join(
            app_helper:get_env(riak_core, platform_data_dir), TxLogFile),
    case dets:open_file(TxLogFile, [{file, TxLogPath}, {type, bag}]) of
        {ok, TxLog} ->
            {ok, #state{partition=Partition, log=TxLog}};
        {error, Reason} ->
            {error, Reason}
    end.







handle_command({operate, ToReply, OpType, Key, Param, SnapshotTime}, 
	_Sender, #state{partition=Partition,log=TxLog}) ->
	
	
%	if oid is updated by T′ ∧
%T′.State = committing ∧
%T.SnapshotTime > T′.CommitTime then wait until T′ .State = committed
	
	
    case OpType of 
	%	read  ->
			%read_data_item();
	
			% If the operation was originated at another node,
	%		% check for clock skew.
	%		Now=now(),	
	%		if (Partition /= OriginPartition) and (SnapshotTime > Now) ->
	%			timer:sleep(now_diff(SnapshotTime, Now)/1000)
	%		end;
			%check if delay needed due to pending commit 
	%		 activeTx(Key)

			
	_ ->
	io:format("RepVNode: Wrong operations!~w~n", [OpType])
      end,
      io:format("ClockSIVNode: Start replication, clock: ~w~n",[SnapshotTime]),
      _ = floppy_rep_sup:start_fsm([ToReply, OpType, Key, Param, SnapshotTime]),
      {noreply, #state{partition=Partition, log= TxLog}};
     
     
    









 

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command_logging, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
