


% This module is supporting the strongly consistent transaction offered by antidote.
% One instance of this module is running on every DC, which are created on startup of Antidote.
% lock_mgr persistently stores which lock is owned by its DC, which lock are requested/currently
% in use by transactions started on this DC and which locks are requested by other lock_mgrs(and
% their respective DC).
% The functionality exported by this module are get_locks/2 and release_locks/1 which are used
% for acquiring and releasing the acquired locks.
% -----------
% Interactions with other modules:
% clocksi_interactive_coord - This module manages transactions (also those using locks) and
%   therefore uses get_locks/2 and release_locks/1 to implement strongly consistent transactions
% inter_dc_query - This module is used for the inter DC communication of the lock_mgr modules
%   Namely to manage the transfer of locks between the multiple lock_mgrs.
% dets - The dets module is used for the persistent storage of lock ownership
% dc_meta_data_utilities -This module is mainly used to get the dc_id of this and the other DCs
% -------
% Additional information:
% Used leader election strategy -   The leader is chosen based on the order of dc_ids
% Deadlock prevention strategy -    Deadlocks are prevented by prioritizing DCs according to the
%                                   order of dc_ids.



-module(lock_mgr).
-behaviour(gen_server).

% interface functions
-export([start_link/0,
        get_locks/2,
        release_locks/1]).

% gen_server interface functions
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

% functions used for inter-dc-communictaion
-export([
        request_response/2,
        send_locks_remote/1,
        request_locks_remote/1
        ]).

% functions not used
-export([
         sent/3
         ]).

% for testing
-export([stateinfo/0,
        local_locks_info/0,
        dets_info/0,
        am_i_leader/0
]).

-include("lock_mgr.hrl").
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% Data Type: state
%% local_locks: list of currently requeired and used locks  [{txid,{required,[locks()],timestamp}}|{txid,{using,[locks()]}} ]
%% lock_requests: list of locks requested by other DCs  [{dcid,[{lock,timestamp}]}]
%% transfer_timer: timer to shedule periodic lock transfers
%% dets_ref: dets table storing received and send locks [{lock,{{send,dcid,[{to,amount}]},{received,dcid,[{from,amount}]}}}]
-record(state, {local_locks,lock_requests, transfer_timer,dets_ref}).
-define(LOG_UTIL, log_utilities).
%-define(DETS_FILE_NAME, "lock_mgr_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id()))++ "_" ++lists:concat(tuple_to_list(element(2,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_FILE_NAME, filename:join(app_helper:get_env(riak_core, platform_data_dir),"lock_mgr_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_SETTINGS, [{access, read_write},{auto_save, 180000},{estimated_no_objects, 256},{file, ?DETS_FILE_NAME},
                        {min_no_slots, 256},{keypos, 1},{ram_file, false},{repair, true},{type, set}]).
-define(DC_UTIL, dc_utilities).
-define(DC_META_UTIL, dc_meta_data_utilities).
% ===================================================================
% Public API
% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    lager:info("Started Lock manager at node ~p", [node()]),
    Timer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
    {ok, Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    {ok, #state{local_locks = orddict:new(),lock_requests=orddict:new(), transfer_timer=Timer,dets_ref=Ref }}.

%% Locks : list of locks that are to be requested
%% TxId : transaction id for which the locks should be reserved
%% Tries to reserve the locks for the specified transaction
%% Returns {ok,[snapshot_time()]} if all locks were available. The snapshot times correspond to the latest time the locks were released
%% Returns {missing_locks, [key()]} if any requested keys are not currently owned by this dc
%% Returns {locks_in_use, [txid()]} if all keys are owned by this dc but are in use by other transacitons
-spec get_locks([key()],txid()) -> {ok,[snapshot_time()]} | {missing_locks, [key()]} | {locks_in_use, [txid()]}.
get_locks(Locks,TxId) ->
    gen_server:call(?MODULE, {get_locks, TxId,Locks}).

%% TxId : transaction id for which all locks are to be released
%% Releases all locks requested for the specified transaction and updates the corresponding last_changed timestamps
-spec release_locks(txid()) -> ok.
release_locks(TxId) ->
    gen_server:cast(?MODULE,{release_locks,TxId}).

% ===================================================================
% Private API
% ===================================================================

%% callback function used for inter dc communication
%% Handles incomming lock send to this dc by another dc
send_locks_remote({remote_send_lock, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_send_lock, TransferOp}).
%% callback function used for inter dc communication
%% Handles incomming remote lock requests comming from other dcs
request_locks_remote({remote_lock_request, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_lock_request, TransferOp}).

% ===================================================================
% For Testing
% ===================================================================

stateinfo() ->
    gen_server:call(?MODULE,{stateinfo}).
local_locks_info() ->
    gen_server:call(?MODULE,{lockinfo}).
dets_info() ->
    gen_server:call(?MODULE,{dets_info}).

% ===================================================================
% Private Functions
% ===================================================================

%  Data structure: local_locks - [{txid,{required,[locks()],timestamp}}|{txid,{using,[locks()]}}]
%  Used to store which transactions on this DC require or currently use locks

%% Locks : locks reuqired for the txid
%% TxId : transaction that requeires the specified locks
%% Timestamp : timestamp of the request
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated local_locks list
%% Adds {TxId,{required,Locks,timestamp}} to local_locks
-spec required([key()],txid(),erlang:timestamp(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
required(Locks,TxId,Timestamp,Local_Locks) ->
    _New_Local_Locks=orddict:store(TxId,{required,Locks,Timestamp},Local_Locks).

%% Locks : locks reuqired for the txid
%% TxId : transaction that requeires the specified locks
%% Timestamp : timestamp of the request
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated local_locks list
%% Adds {TxId,{required,Locks,timestamp}} to local_locks
%% Sends remote_lock_request(Locks,0,dcid) messages to all other DCs.
-spec required_remote([key()],[key()],txid(),erlang:timestamp(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
required_remote(Locks,Missing_Locks,TxId,Timestamp,Local_Locks) ->
    DCID = dc_meta_data_utilities:get_my_dc_id(),
    remote_lock_request(DCID, 0, Missing_Locks),  % TODO Key value ? (currently 0)
    _New_Local_Locks=orddict:store(TxId,{required,Locks,Timestamp},Local_Locks).


%% Locks : locks used by the txid
%% TxId : transaction that uses the locks
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated lokal_locks list if all specified locks are owned by this DC and not in use by another transaction.
%% Returns {missing_locks,Missing_Locks} if at least one lock is not owned by this DC or is currently used by another transaction.
%% Returns {locks_in_use,[{txid(),[locks]}]} if all locks are owned by this DC but are used by another Transaction
%% Updates the local_locks list if the locks were available by adding {TxId,{using,Locks}} to local_locks
-spec using([key()],txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}] | {atom(),[key()]} | {atom(),[{txid(),[key()]}]}.
using(Locks,TxId,Local_Locks) ->
        Missing_Locks = lists:foldl(fun(Lock,AccIn)->
                        case check_lock(Lock) of
                                true -> AccIn;
                                false -> [Lock|AccIn]
                        end
                end,
                [],Locks),
        case Missing_Locks of
                [] ->
                    case locks_used_by_other_tx(Locks,Local_Locks) of
                        [] ->
                            _New_Local_Locks = orddict:store(TxId,{using,Locks},Local_Locks);
                        Used_Locks_List ->
                            {locks_in_use, Used_Locks_List}
                    end;
                Missing_Locks_List ->
                    {missing_locks,Missing_Locks_List}
        end.

%% Locks : Locks that are to be checked if they are used by a transaction (of this dc)
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns [{txid(),[locks]}] for all transactions that currently use the locks specified by Locks (only the intersection is returned)
-spec locks_used_by_other_tx([key()],[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),[key()]}].
locks_used_by_other_tx(Locks,Local_Locks) ->
    _Used_Locks = lists:foldl(fun(Elem,AccIn) ->
        case Elem of
            {_TxId,{required,_Locks_required,_Timestamp}} ->
                AccIn;
            {TxId_other,{using,Locks_in_use}} ->
                Used_by_other_TxId = lists:foldl(fun(Elem2,AccIn2) ->
                        case lists:member(Elem2,Locks_in_use) of
                            true -> [Elem2|AccIn2];
                            false -> AccIn2
                        end
                    end,
                    [],Locks),
                case Used_by_other_TxId of
                    [] -> AccIn;
                    _ -> [{TxId_other,Used_by_other_TxId}|AccIn]
                end
            end
        end,
        [],Local_Locks).




%% TxId : transaction whose locks are to be released
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Updates dets_ref by updating the last_changed entry of all locks used by the specified TxId
%% Releases ownership and lock requests of all locks of the specified TxId
%% Returns the updated lokal_locks list
-spec release_locks(txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
release_locks(TxId,Local_Locks) ->
    case orddict:find(TxId,Local_Locks) of
        {ok,{using,Locks}} ->
            update_last_changed(Locks);
        error ->
            %lager:error("release_locks(~w,~w)~n",[TxId,Local_Locks]),
            ok
    end,
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).

%% Local_Locks : orddict managing all transaction requesting and using locks
%% Timeout : timeout value in ms
%% Removes lock requests that are older than the specified timeout value form local_locks
%% Returns the updated lokal_locks list
-spec remove_old_required_locks([{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}],non_neg_integer()) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
remove_old_required_locks(Local_Locks,Timeout) ->
    Current_Time = erlang:timestamp(),
    _New_Local_Locks=orddict:filter(
                fun(_Key,Value) ->
                        case Value of
                                {required,_Lock_List,Timestamp} ->
                                         timer:now_diff(Current_Time,Timestamp)< Timeout;
                                {using,_Lock_List}->
                                        true
                        end
                end,Local_Locks).


%  Data structure: lock_requests - [{dcid,[{lock,timestamp}]}]
%  Used to memorize which DC requested which lock


%% Locks : list of locks to be added to lock_requests
%% DcId : dcid that requested the locks
%% Timestamp : timestamp of the request
%% Lock_Requests : orddict managing all remote lock requests
%% Adds these locks with the DcId to the lock_request list
-spec requested([key()],dcid(),erlang:timestamp(),[{dcid(),[{key(),erlang:timestamp()}]}]) -> [{dcid(),[{key(),erlang:timestamp()}]}].
requested(Locks, DcId, Timestamp, Lock_Requests) ->
        case orddict:find(DcId,Lock_Requests) of
                {ok,Corresponding_Lock_Requests} ->
                        Updated_Lock_Reuqests=lists:foldl(fun(Elem,AccIn)-> orddict:store(Elem,Timestamp,AccIn) end,
                Corresponding_Lock_Requests,Locks),
                        orddict:store(DcId,Updated_Lock_Reuqests,Lock_Requests);
                error ->
                        New_Orddict = orddict:new(),
                        New_Lock_Request_Orddict = lists:foldl(fun(Elem,AccIn)-> orddict:store(Elem,Timestamp,AccIn) end,New_Orddict,Locks),
                        orddict:store(DcId,New_Lock_Request_Orddict,Lock_Requests)
        end.

%% TODO currently not used
%% Removes {lock,dcid,_} from the list of locks to send
sent(Lock,DcId,Lock_Requests) ->
    case orddict:find(DcId,Lock_Requests)of
                {ok, Corresponding_Lock_Request} ->
                        New_Corresponding_Lock_Request=orddict:erase(Lock,Corresponding_Lock_Request),
                    orddict:store(DcId,New_Corresponding_Lock_Request,Lock_Requests);
                error->
                        Lock_Requests
        end.
    %Removes {lock,dcid,_} from lock_requests.



%% Lock_Requests : orddict managing all remote lock requests
%% Timeout : timout value in ms
%% Removes all lock requests that are older than the specified timeout value from the list and returns that list
-spec clear_old_lock_requests([{dcid(),[{key(),erlang:timestamp()}]}],non_neg_integer()) -> [{dcid(),[{key(),erlang:timestamp()}]}].
clear_old_lock_requests(Lock_Requests,Timeout)->
    Current_Time = erlang:timestamp(),
    orddict:fold(
        fun(DCID,Lock_Timestamp_List,AccIn)->
            Filtered_Orddict = orddict:filter(fun(_Lock_2,Timestamp_2) -> timer:now_diff(Current_Time,Timestamp_2)< Timeout end,Lock_Timestamp_List),
            case orddict:size(Filtered_Orddict)==0 of
                                false ->
                                    orddict:store(DCID,Filtered_Orddict,AccIn);
                                true ->
                                        AccIn
                        end
        end,
    orddict:new(),Lock_Requests).




% Data sturcture: dets_ref : [{lock,{{send,dcid,[{to,amount}]},{received,dcid,[{from,amount}]}},last_modified}]
% Used to persistently store which DC currently owns which locks

%% Lock : lock to send to another dc
%% To : dc where the locks should be send to
%% Returns updated Dets_ref table and sends a message to the specified DC
%% Returns Dets_ref if the lock was not owned by this dc
%% Allways send a messge to the other DC, even if the lock may currently not be send or is not owned, to make up for lost messages
%% Updates the {send,dcid,[{to,amount}]} entry of dets_ref of the specified lock
-spec send_lock(key(),dcid()) -> ok.
send_lock(Lock,To)->
        case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,{{send,DCID1,Send_List},{received,DCID2,Received_List}},Snapshot}] ->
                    Total_Send = lists:foldl(fun({_To,Amount},Acc) -> Acc+Amount end,0,Send_List),
                    Total_Received = lists:foldl(fun({_From,Amount},Acc) -> Acc+Amount end,0,Received_List),
                    Has_Lock = Total_Send < Total_Received,
                    DCID = dc_meta_data_utilities:get_my_dc_id(),
                    case Has_Lock of
                            true ->
                                    case lists:keyfind(To,1,Send_List) of
                                            false ->
                                                    New_Send_List = [{To,1}|Send_List],
                                                    remote_send_lock(Lock, 1,Snapshot, DCID, To, 0), % TODO Key value ? (currently 0)
                                                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}},Snapshot});

                                            {To,Old_Amount} ->
                                                    New_Send_List = lists:keyreplace(To,1,Send_List,{To,Old_Amount+1}),
                                                    remote_send_lock(Lock, Old_Amount+1, Snapshot, DCID, To, 0), % TODO Key value ? (currently 0)
                                                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}},Snapshot})
                                    end;

                            false ->
                                    case lists:keyfind(To,1,Send_List) of
                                            false ->
                                                    New_Send_List = [{To,0}|Send_List],
                                                    % Send the current information about the lock (just to update the tables)
                                                    remote_send_lock(Lock, 0, Snapshot, DCID, To, 0), % TODO Key value ? (currently 0)
                                                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}},Snapshot});

                                            {To,Old_Amount} ->
                                                    % Send the current information about the lock (just to update the tables)
                                                    remote_send_lock(Lock, Old_Amount, Snapshot, DCID, To, 0), % TODO Key value ? (currently 0)
                                                    ok
                                    end
                    end;
                [] ->
                    %{ok,Now} = get_snapshot_time(),
                    Now = dict:from_list([]),
                    case am_i_leader() of
                        true ->
                            DCID = dc_meta_data_utilities:get_my_dc_id(),
                            remote_send_lock(Lock, 1, Now, DCID, To, 0), % TODO Key value ? (currently 0)
                            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[{To,1}]},{received,DCID,[{DCID,1}]}},Now});

                        false ->
                            DCID = dc_meta_data_utilities:get_my_dc_id(),
                            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[{To,0}]},{received,DCID,[]}},Now})
                    end;

                {error,_Reason} ->
                    %{ok,Now} = get_snapshot_time(),
                    Now = dict:from_list([]),
                    DCID = dc_meta_data_utilities:get_my_dc_id(),
                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[{To,0}]},{received,DCID,[]}},Now})
                    % sending the the total amount of locks again is not required here, since a lock was never send.
        end.



%% Lock : lock received by another dc
%% From : dc which send the lock
%% Amount : number of times this dc received a lock from the other dc(From)
%% Last_Changed : snapshot when the lock was released the last time
%% Updates the {received,dcid,[{from,amount}]} entry of dets_ref of the specified lock
-spec received_lock(key(),dcid(),non_neg_integer(),snapshot_time()) -> ok.
received_lock(Lock,From,Amount,Last_Changed)->
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,{{send,DCID1,Send_List},{received,DCID2,Received_List}},Old_Snapshot}] ->
            Snapshot = vectorclock:max([Old_Snapshot,Last_Changed]),
            case lists:keyfind(From,1,Received_List) of
                false ->
                    New_Received_List = [{From,Amount}|Received_List],
                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,Send_List},{received,DCID2,New_Received_List}},Snapshot});
                {From,Old_Amount} ->
                    case Old_Amount < Amount of
                        true ->
                            New_Received_List = lists:keyreplace(From,1,Received_List,{From,Amount}),
                            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,Send_List},{received,DCID2,New_Received_List}},Snapshot});
                        false -> ok
                    end
            end;
        []->
            MYDCID = dc_meta_data_utilities:get_my_dc_id(),
            dets:insert(?DETS_FILE_NAME,{Lock,{{send,MYDCID,[]},{received,MYDCID,[{From,Amount}]}},Last_Changed});
        {error,_Reason} ->
            DCID = dc_meta_data_utilities:get_my_dc_id(),
            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[]},{received,DCID,[{From,Amount}]}},Last_Changed})
    end.


%% Lock : lock to be checked
%% Returns true if the lock is currently owned by this DC
%% Returns false if it is not owned by this DC
-spec check_lock(key()) -> boolean().
check_lock(Lock) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,{{send,_DCID1,Send_List},{received,_DCID2,Received_List}},_}] ->
                    Total_Send = lists:foldl(fun({_To,Amount},Acc) -> Acc+Amount end,0,Send_List),
                    Total_Received = lists:foldl(fun({_From,Amount},Acc) -> Acc+Amount end,0,Received_List),
                    _Has_Lock = Total_Send < Total_Received;
                [] ->
                        %{ok,Now} = get_snapshot_time(),
                        Now = dict:from_list([]),
                        case am_i_leader() of
                                true ->
                                    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
                                    dets:insert(?DETS_FILE_NAME,{Lock,{{send,MyDCId,[]},{received,MyDCId,[{MyDCId,1}]}},Now}),
                                    true;
                                false ->
                                    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
                                        dets:insert(?DETS_FILE_NAME,{Lock,{{send,MyDCId,[]},{received,MyDCId,[]}},Now}),
                                    false
                        end;
                {error,Reason} ->
                        {error,Reason}
        end.

%% Returns true if this DC is the leader, else false is returned
%% The leader may create locks
%% Uses the ordering of orddict to decide the leader (the first key)
-spec am_i_leader() -> boolean().
am_i_leader() ->
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    AllDCIds = lists:foldl(fun(#descriptor{dcid=Id}, IdsList) ->
                                [Id | IdsList]
                             end, [], OtherDCDescriptors),
    Ordd = orddict:new(),
    OrddAllDCIDs = lists:foldl(fun(Id, DCIDs) -> orddict:store(Id,0,DCIDs) end, Ordd, AllDCIds),
    case OrddAllDCIDs of
        []->
            true;
        _ ->
        {Key,_Value} = hd(OrddAllDCIDs),
        Key== MyDCId
    end.

%% Locks : list of locks
%% Returns a list of the snapshot times the specified locks were released the last time
-spec get_last_modified([key()]) -> [snapshot_time()].
get_last_modified(Locks)->
    %{ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    lists:foldl(
        fun(Lock,AccIn) ->
            case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{_Lock,_,Snapshot}] ->
                    _New_AccIn = [Snapshot | AccIn];
                [] ->
                    AccIn
            end
        end,
        [],Locks).

%% Locks : list of locks
%% updates the last_changed value of the specified locks to the max of a current snapshot and the old last_changed value
-spec update_last_changed([key()]) -> ok.
update_last_changed(Locks) ->
    %{ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    lists:foldl(
        fun(Lock,AccIn) ->
            case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Current_Lock,Transfer_History,Old_Snapshot}] ->
                    {ok,Now} = get_snapshot_time(),
                    New_Snapshot = vectorclock:max([Now,Old_Snapshot]),
                    dets:insert(?DETS_FILE_NAME,{Current_Lock,Transfer_History,New_Snapshot}),
                    ok;
                [] ->
                    AccIn
            end
        end,
        ok,Locks).

%% Returns the current snapshot time of this dc
-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
    Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    SnapshotTime = vectorclock:set_clock_of_dc(DcId, Now, VecSnapshotTime),
    {ok, SnapshotTime}.
% ===================================================================
% Handling remote requests
% ===================================================================

%% Returns an ordered list of all other DCs
-spec other_dcs_list() -> [dcid()].
other_dcs_list() ->
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    OtherDCIds = lists:foldl(fun(#descriptor{dcid=Id}, IdsList) ->
                                     case Id == MyDCId of
                                         true -> IdsList;
                                         false -> [Id | IdsList]
                                     end
                             end, [], OtherDCDescriptors),
        lists:sort(OtherDCIds).


%% MyDCId : DCID of this DC
%% Key : ? TODO
%% Locks : list of locks that are to be requested
%% sends a message to all other DCs requesting the Locks.
-spec remote_lock_request(dcid(),term(),[key()])-> ok.
remote_lock_request(MyDCId, Key, Locks) ->
    {LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    Other_DCs_List = other_dcs_list(),
    spawn(fun()->   %TODO
    lists:foldl(
        fun(RemoteId,AccIn) ->
            BinaryMsg = term_to_binary({request_locks,
            {remote_lock_request, {Locks, MyDCId}}, LocalPartition, MyDCId, RemoteId}),
            [inter_dc_query:perform_request(?LOCK_MGR_REQUEST, {RemoteId, LocalPartition},
                BinaryMsg, fun lock_mgr:request_response/2) | AccIn]
        end
        ,[],Other_DCs_List)
    end), %TODO
    ok.


%% Lock : Lock to send to another DC
%% Amount : Total number of times the lock was send to that DC from this DC
%% Last_Changed : Snapshot of the last time this lock was released
%% MyDCId : DCID of this DC
%% RemoteId : DCID of the DC the lock is send to
%% Key : ? TODO
%% sends a message to the speciefed other DC containing the lock information
-spec remote_send_lock(key(),non_neg_integer(),snapshot_time(),dcid(),dcid(),key())-> ok.
remote_send_lock(Lock,Amount, Last_Changed,MyDCId, RemoteId, Key) ->
    %lager:info("remote_send_lock(~w,~w,Last_Changed,~w,~w,~w)~n",[Lock,Amount,MyDCId, RemoteId, Key]),
    {LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    BinaryMsg = term_to_binary({send_locks,
        {remote_send_lock, {Lock,Amount, Last_Changed,MyDCId, RemoteId}}, LocalPartition, MyDCId, RemoteId}),
    spawn(fun()->   %TODO
    inter_dc_query:perform_request(?LOCK_MGR_SEND, {RemoteId, LocalPartition},
        BinaryMsg, fun lock_mgr:request_response/2)
    end), %TODO
    ok.

%% Request response - do nothing. TODO  what is this function meant to do ?
request_response(_BinaryRep, _RequestCacheEntry) -> ok.
% ===================================================================
% Callbacks
% ===================================================================

%% Releases all locks currently owned by the specified transaction.
handle_cast({release_locks,TxId}, #state{local_locks=Local_Locks}=State) ->
        %lager:info("handle_cast({release_lock,~w},state)~n",[TxId]),
        New_Local_Locks = release_locks(TxId,Local_Locks),
        {noreply, State#state{local_locks=New_Local_Locks}};

%% Takes a Lock, amount(number of times this lock was send to this DC by From), the senders DCID and the DCID of this DC
%% Stores in dets_ref how often the sender send the Lock to this DC
handle_cast({remote_send_lock, {Lock,Amount,Snapshot,From,MyDCID1}}, State) ->
        %lager:info("handle_cast({remote_send_lock,~w,~w,~w,~w},state)~n",[Lock,Amount,From,MyDCID1]),
        MyDCID2 = dc_meta_data_utilities:get_my_dc_id(),
        case MyDCID1 == MyDCID2 of
                true ->
                        received_lock(Lock,From,Amount, Snapshot),
                        {noreply, State};
                false -> {noreply, State}
        end;

%% Takes a list of Locks and a Sender as input
%% Adds {dcid,[{lock,timestamp}]} to lock_requests to remember which DC requested which Locks
%% Adds a timestamp to filter too old requests
handle_cast({remote_lock_request, {Locks, Sender}}, #state{lock_requests=Lock_Requests}=State) ->
    %lager:info("handle_cast({remote_lock_request,~w,~w},from,state)~n",[Locks,Sender]),
    Timestamp = erlang:timestamp(),
    New_Lock_Requests = requested(Locks, Sender, Timestamp, Lock_Requests),
        {noreply, State#state{lock_requests=New_Lock_Requests}}.


%% For testing
handle_call({stateinfo}, _From, #state{}=State) ->
        {reply, State , State};
%% For testing
handle_call({lockinfo}, _From, #state{local_locks= Lock}=State) ->
        {reply, Lock, State};
%% For testing
handle_call({dets_info}, _From, State) ->
        {reply, dets:match(?DETS_FILE_NAME,'$1'),State};

%% Tries to aquire the specified locks for the transaction.
%% If all locks are currently owned by this DC and not used by another transaction then {ok,snapshot_time()} is returned.
%% If at least one lock is not owned by this DC then {missing_locks, Missing_Locks} is returned and it automatically requests the missing locks from other DCs.
%% If at al the requested lock are currently in use by other transactions of this dc {locks_in_use,Transactions_Using_The_Locks} is returned.
handle_call({get_locks,TxId,Locks}, _From, #state{local_locks=Local_Locks}=State) ->
    %lager:info("handle_call({get_locks,~w,~w},from,state)~n",[TxId,Locks]),
    case using(Locks, TxId, Local_Locks) of
        {missing_locks, Missing_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started missing_locks-- ~n",[TxId,Locks]),
            New_Local_Locks2=required_remote(Locks,Missing_Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished missing_locks-- ~n",[TxId,Locks]),
            {reply, {missing_locks, Missing_Locks} , State#state{local_locks=New_Local_Locks2}};
        {locks_in_use, Transactions_Using_The_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started locks_in_use-- ~n",[TxId,Locks]),
            New_Local_Locks3=required(Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished locks_in_use-- ~n",[TxId,Locks]),
            {reply, {locks_in_use, Transactions_Using_The_Locks} , State#state{local_locks=New_Local_Locks3}};
        New_Lokal_Locks1 ->
            % get the list of snapshots the locks were released the last time
            Snapshot_Times = get_last_modified(Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished ok-- ~n",[TxId,Locks]),
            {reply, {ok,Snapshot_Times}, State#state{local_locks=New_Lokal_Locks1}}
    end.


%% Periodically transfers locks requested by other DCs to them, if they are currently not used
handle_info(transfer_periodic, #state{lock_requests=Old_Lock_Requests,local_locks= Local_Locks, transfer_timer=OldTimer}=State) ->
    erlang:cancel_timer(OldTimer),
    Clean_Lock_Requests = clear_old_lock_requests(Old_Lock_Requests, ?LOCK_REQUEST_TIMEOUT),
    Clear_Local_Locks = remove_old_required_locks(Local_Locks,?LOCK_REQUIRED_TIMEOUT),
    %lager:info("handle_info({transfer_periodic},clear_local_locks=~w,clear_lock_requests =~w ~n",[Clear_Local_Locks,Clean_Lock_Requests]),
    % goes through all lock reuests. If the request is NOT in local_locks then the lock is send to the requesting DC
    Updated_Lock_Requests = orddict:fold(
        fun(DCID,Lock_Timestamp_List,Changed_Lock_Requests)->
            Updated_Key_Value_List = orddict:fold(
                fun(Lock, Timestamp,New_Key_Value_List) ->
                In_Use = orddict:fold(
                    fun(_TxId,Value2,AccIn) ->
                        case Value2 of
                            {using,Lock_List} -> lists:member(Lock,Lock_List) or AccIn;
                            _ -> false or AccIn
                        end
                    end,
                false,Clear_Local_Locks),
                case In_Use of
                    false ->
                        % lock is currently NOT used and therefore may be send to another DC
                        send_lock(Lock,DCID),
                        New_Key_Value_List;
                    true ->
                        orddict:store(Lock,Timestamp,New_Key_Value_List)

                end
            end,orddict:new(),Lock_Timestamp_List),
            case Updated_Key_Value_List of
                [] ->
                    Changed_Lock_Requests;
                _ ->
                    orddict:store(DCID,Updated_Key_Value_List,Changed_Lock_Requests)
            end
        end, orddict:new(),Clean_Lock_Requests),
    NewTimer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
    New_Lock_Requests_Value = case ?REDUCED_INTER_DC_COMMUNICATION of
        true -> Updated_Lock_Requests;
        false -> Clean_Lock_Requests
    end,
    {noreply, State#state{transfer_timer= NewTimer,local_locks=Clear_Local_Locks,lock_requests=New_Lock_Requests_Value}}.


terminate(_Reason, _State) ->
    dets:close(?DETS_FILE_NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
