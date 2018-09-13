


% This module is supporting the strongly consistent transaction offered by antidote.
% One instance of this module is running on every DC, which are created on startup of Antidote.
% lock_mgr_es persistently stores which lock is owned by its DC, which lock are requested/currently
% in use by transactions started on this DC and which locks are requested by other lock_mgr_es(and
% their respective DC).
% The functionality exported by this module are get_locks/2 and release_locks/1 which are used
% for acquiring and releasing the acquired locks.
% -----------
% Differences to lock_mgr
% This module allows a client/other process to get locks as "shared" or "exclusive".
% A lock that is acquired as exclusive behaves the same as a locks acquired with lock_mgr.
% When a lock is acquired as exclusive no other transaction may use that lock at that time
% (not as shared nor as exclusive)
% Multiple transactoins may use the same lock as shared at the same time across multiple DCs
% as long as no transaction has the exclusive lock.
% -----------
% Interactions with other modules:
% clocksi_interactive_coord - This module manages transactions (also those using locks) and
%   therefore uses get_locks/2 and release_locks/1 to implement strongly consistent transactions
% inter_dc_query - This module is used for the inter DC communication of the lock_mgr_es modules
%   Namely to manage the transfer of locks between the multiple lock_mgr_es.
% dets - The dets module is used for the persistent storage of lock ownership
% dc_meta_data_utilities -This module is mainly used to get the dc_id of this and the other DCs
% -------
% Additional information:
% Used leader election strategy -   The leader is chosen based on the order of dc_ids
% Deadlock prevention strategy -    Deadlocks are prevented by prioritizing DCs according to the
%                                   order of dc_ids.
%                                   Exclusive lock requests are prioritized above shared lock requests.


-module(lock_mgr_es).
-behaviour(gen_server).

% interface functions
-export([start_link/0,
        get_locks/3,
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
        request_response_es/2,
        send_locks_remote_es/1,
        request_locks_remote_es/1
        ]).

% functions not used
-export([
         sent/3
         ]).

% for testing
-export([stateinfo/0,
        local_locks_info/0,
        dets_info/0,
        am_i_leader/0,
         update_send_history2/2,
         update_send_history/2,
         get_last_modified/1
]).
% TODO to remove
-export([remote_lock_request_es/4
]).


-include("lock_mgr_es.hrl").
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%TODO
%% Data Type: state
%% local_locks: list of currently requeired and used locks  [{txid,{required,[locks()],[locks()],timestamp}}|{txid,{using,[locks()],[locks()]}}]
%% lock_requests: list of locks requested by other DCs  [{dcid,[{lock,amount,timestamp}]}]
%% transfer_timer: timer to shedule periodic lock transfers
%% dets_ref: dets table storing received and send lock parts [{lock,[{from,[{to,amount}]}],last_modified}]
-record(state, {local_locks,lock_requests, transfer_timer,dets_ref}).
-define(LOG_UTIL, log_utilities).
%-define(DETS_FILE_NAME, "lock_mgr_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id()))++ "_" ++lists:concat(tuple_to_list(element(2,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_FILE_NAME, filename:join(app_helper:get_env(riak_core, platform_data_dir),"lock_mgr_es_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_SETTINGS, [{access, read_write},{auto_save, 180000},{estimated_no_objects, 256},{file, ?DETS_FILE_NAME},
                        {min_no_slots, 256},{keypos, 1},{ram_file, false},{repair, true},{type, set}]).
-define(DC_UTIL, dc_utilities).
-define(DC_META_UTIL, dc_meta_data_utilities).
% ===================================================================
% Public API
% ===================================================================
%TODO
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
%TODO
init([]) ->
    lager:info("Started Lock manager ES at node ~p", [node()]),
    Timer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY_ES, self(), transfer_periodic),
    {ok, Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    {ok, #state{local_locks = orddict:new(),lock_requests=orddict:new(), transfer_timer=Timer,dets_ref=Ref }}.
%TODO
%% Shared_Locks : list of shared locks that are to be requested
%% Exclusive_Locks : list of exclusive locks that are to be requested
%% TxId : transaction id for which the locks should be reserved
%% Tries to reserve the locks for the specified transaction
%% Returns {ok,[snapshot_time()]} if all locks were available. The snapshot times correspond to the latest time the locks were released
%% Returns {missing_locks, {[key()],[key()]}} if any requested keys are not currently owned by this dc
%% Returns {locks_in_use, [txid()]} if all keys are owned by this dc but are in use by other transacitons
-spec get_locks([key()],[key()],txid()) -> {ok,[snapshot_time()]} | {missing_locks, [key()]} | {locks_in_use, [txid()]}.
get_locks(Shared_Locks,Exclusive_Locks,TxId) ->
    gen_server:call(?MODULE, {get_locks, TxId,Shared_Locks,Exclusive_Locks}).
%% TxId : transaction id for which all locks are to be released
%% Releases all locks requested for the specified transaction and updates the corresponding last_changed timestamps
-spec release_locks(txid()) -> ok.
release_locks(TxId) ->
    gen_server:cast(?MODULE,{release_locks,TxId}).

% ===================================================================
% Private API
% ===================================================================
%TODO
%% callback function used for inter dc communication
%% Handles incomming lock send to this dc by another dc
send_locks_remote_es({remote_send_lock_history, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_send_lock_history, TransferOp}).
%TODO
%% callback function used for inter dc communication
%% Handles incomming remote lock requests comming from other dcs
request_locks_remote_es({remote_lock_request_es, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_lock_request_es, TransferOp}).

% ===================================================================
% For Testing
% ===================================================================
%TODO
stateinfo() ->
    gen_server:call(?MODULE,{stateinfo}).
local_locks_info() ->
    gen_server:call(?MODULE,{lockinfo}).
dets_info() ->
    gen_server:call(?MODULE,{dets_info}).

% ===================================================================
% Private Functions
% ===================================================================
%                                                  shared   exclusive                          shared    exclusive
%  Data structure: local_locks - [{txid,{required,[locks()],[locks()],timestamp}}|{txid,{using,[locks()],[locks()]}}]
%  Used to store which transactions on this DC requires shared/exclusive locks or uses them
%TODO
%% Locks : locks reuqired for the txid
%% TxId : transaction that requeires the specified locks
%% Timestamp : timestamp of the request
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated local_locks list
%% Adds {TxId,{required,Locks,timestamp}} to local_locks
-spec required([key()],[key()],txid(),erlang:timestamp(),[{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}]) -> [{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}].
required(Shared_Locks,Exclusive_Locks,TxId,Timestamp,Local_Locks) ->
    _New_Local_Locks=orddict:store(TxId,{required,Shared_Locks,Exclusive_Locks,Timestamp},Local_Locks).

%% Locks : locks reuqired for the txid
%% TxId : transaction that requeires the specified locks
%% Timestamp : timestamp of the request
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated local_locks list
%% Adds {TxId,{required,Locks,timestamp}} to local_locks
%% Sends remote_lock_request_es(MyDCID,0,Shared_Locks,Exclusive_Locks) messages to all other DCs.
-spec required_remote([key()],[key()],{[key()],[key()]},txid(),erlang:timestamp(),[{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}]) -> [{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}].
required_remote(Shared_Locks,Exclusive_Locks,{Missing_Shared_Locks,Missing_Exclusive_Locks},TxId,Timestamp,Local_Locks) ->
    DCID = dc_meta_data_utilities:get_my_dc_id(),
    remote_lock_request_es(DCID,0,Missing_Shared_Locks,Missing_Exclusive_Locks),
    _New_Local_Locks=orddict:store(TxId,{required,Shared_Locks,Exclusive_Locks,Timestamp},Local_Locks).


%% Shared_Locks : Shared locks used by the txid
%% Exclusive_Locks : Exclusive locks used by the txid
%% TxId : transaction that uses the locks
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns the updated lokal_locks list if all specified locks are owned by this DC and not in use by another transaction.
%% Returns {missing_locks,Missing_Locks} if at least one lock is not owned by this DC or is currently used by another transaction.
%% Returns {locks_in_use,[{txid(),[locks]}]} if all locks are owned by this DC but are used by another Transaction
%% Updates the local_locks list if the locks were available by adding {TxId,{using,Locks}} to local_locks
-spec using([key()],[key()],txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}] | {atom(),[key()]} | {atom(),[{txid(),[key()]}]}.
using(Shared_Locks,Exclusive_Locks,TxId,Local_Locks) ->
        Missing_Locks_Shared = lists:foldl(fun(Lock,AccIn)->
                        case check_lock_shared(Lock) of
                                true -> AccIn;
                                false -> [Lock|AccIn]
                        end
                end,
                [],Shared_Locks),
        Missing_Locks_Exclusive = lists:foldl(fun(Lock,AccIn)->
                        case check_lock_exclusive(Lock) of
                                true -> AccIn;
                                false -> [Lock|AccIn]
                        end
                end,
                [],Exclusive_Locks),
        case {Missing_Locks_Shared,Missing_Locks_Exclusive} of
                {[],[]} ->
                    case {check_if_used_for_exclusive_locks(Exclusive_Locks++Shared_Locks,Local_Locks),check_if_used_for_shared_locks(Exclusive_Locks, Local_Locks)} of
                        {[],[]} ->
                            _New_Local_Locks = orddict:store(TxId,{using,Shared_Locks,Exclusive_Locks},Local_Locks);
                        Used_As_Exclusive_Locks_List ->
                            {locks_in_use, Used_As_Exclusive_Locks_List}
                    end;
                Missing_Locks_Lists ->
                    {missing_locks,Missing_Locks_Lists}
        end.

%% Locks : Locks to check
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns [{txid(),[locks]}] for all transactions that currently use the exclusive locks specified by Locks (only the intersection is returned)
-spec check_if_used_for_exclusive_locks([key()],[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),[key()]}].
check_if_used_for_exclusive_locks(Locks,Local_Locks) ->
    _Used_Locks = lists:foldl(fun(Elem,AccIn) ->
        case Elem of
            {_TxId,{required,_Shared_Locks_required,_Exclusive_Locks_required,_Timestamp}} ->
                AccIn;
            {TxId_other,{using,_Shared_Locks_in_use,Exclusive_Locks_in_use}} ->
                Used_by_other_TxId = lists:foldl(fun(Elem2,AccIn2) ->
                        case lists:member(Elem2,Exclusive_Locks_in_use) of
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
%% Locks : Locks to check
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Returns [{txid(),[locks]}] for all transactions that currently use the shared locks specified by Locks (only the intersection is returned)
-spec check_if_used_for_shared_locks([key()],[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),[key()]}].
check_if_used_for_shared_locks(Locks,Local_Locks) ->
    _Used_Locks = lists:foldl(fun(Elem,AccIn) ->
        case Elem of
            {_TxId,{required,_Shared_Locks_required,_Exclusive_Locks_required,_Timestamp}} ->
                AccIn;
            {TxId_other,{using,Shared_Locks_in_use,_Exclusive_Locks_in_use}} ->
                Used_by_other_TxId = lists:foldl(fun(Elem2,AccIn2) ->
                        case lists:member(Elem2,Shared_Locks_in_use) of
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


% Compiliation flag that determines if transactions should wait for the updates made using shared locks or not
-ifdef(WAIT_FOR_SHARED_LOCKS1).

%% TxId : transaction whose locks are to be released
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Updates dets_ref by updating the last_changed entry of all locks used by the specified TxId (for exclusive and shared locks)
%% Releases ownership and lock requests of all locks of the specified TxId
%% Returns the updated lokal_locks list
-spec release_locks(txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
release_locks(TxId,Local_Locks) ->
    case orddict:find(TxId,Local_Locks) of
        {ok,{using,Shared_Locks,Exclusive_Locks}} ->
            All_Locks = Shared_Locks ++ Exclusive_Locks,
            update_last_changed(All_Locks);
        error ->
            %lager:error("release_locks(~w,~w) failed since the locks were not used~",[TxId,Local_Locks]),
            ok
    end,
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).

%% Shared_Locks : Shared locks requested via get locks
%% Exclusive_Locks : Exclusive locks requested via get locks
%% Returns all requested locks if WAIT_FOR_SHARED_LOCKS is defined in lock_mgr_es.hrl
%% Returns only the requested exclusive locks if WAIT_FOR_SHARED_LOCKS is NOT defined in lock_mgr_es.hrl
-spec what_locks_to_wait_for([key()],[key()]) -> [key()].
what_locks_to_wait_for(Shared_Locks,Exclusive_Locks)->
    _All_Locks = Shared_Locks ++ Exclusive_Locks.

-else.
-ifdef(WAIT_FOR_SHARED_LOCKS2).

%% TxId : transaction whose locks are to be released
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Updates dets_ref by updating the last_changed entry of all locks used by the specified TxId (only for exclusive locks)
%% Releases ownership and lock requests of all locks of the specified TxId
%% Returns the updated lokal_locks list
-spec release_locks(txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
release_locks(TxId,Local_Locks) ->
    case orddict:find(TxId,Local_Locks) of
        {ok,{using,Shared_Locks,Exclusive_Locks}} ->
            All_Locks = Shared_Locks ++ Exclusive_Locks,
            update_last_changed(All_Locks);
        error ->
            %lager:error("release_locks(~w,~w) failed since the locks were not used~",[TxId,Local_Locks]),
            ok
    end,
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).

%% Shared_Locks : Shared locks requested via get locks
%% Exclusive_Locks : Exclusive locks requested via get locks
%% Returns all requested locks if WAIT_FOR_SHARED_LOCKS is defined in lock_mgr_es.hrl
%% Returns only the requested exclusive locks if WAIT_FOR_SHARED_LOCKS is NOT defined in lock_mgr_es.hrl
-spec what_locks_to_wait_for([key()],[key()]) -> [key()].
what_locks_to_wait_for(_Shared_Locks,Exclusive_Locks)->
    Exclusive_Locks.

-else.
%-ifdef(WAIT_FOR_SHARED_LOCKS3).

%% TxId : transaction whose locks are to be released
%% Local_Locks : orddict managing all transaction requesting and using locks
%% Updates dets_ref by updating the last_changed entry of all locks used by the specified TxId (only for exclusive locks)
%% Releases ownership and lock requests of all locks of the specified TxId
%% Returns the updated lokal_locks list
-spec release_locks(txid(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
release_locks(TxId,Local_Locks) ->
    case orddict:find(TxId,Local_Locks) of
        {ok,{using,_Shared_Locks,Exclusive_Locks}} ->
            case Exclusive_Locks of
                [] -> ok;
                _ -> update_last_changed(Exclusive_Locks)
            end;
        error ->
            %lager:error("release_locks(~w,~w) failed since the locks were not used~",[TxId,Local_Locks]),
            ok
    end,
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).

%% Shared_Locks : Shared locks requested via get locks
%% Exclusive_Locks : Exclusive locks requested via get locks
%% Returns all requested locks if WAIT_FOR_SHARED_LOCKS is defined in lock_mgr_es.hrl
%% Returns only the requested exclusive locks if WAIT_FOR_SHARED_LOCKS is NOT defined in lock_mgr_es.hrl
-spec what_locks_to_wait_for([key()],[key()]) -> [key()].
what_locks_to_wait_for(_Shared_Locks,Exclusive_Locks)->
    Exclusive_Locks.

-endif.
-endif.

%% Local_Locks : orddict managing all transaction requesting and using locks
%% Timeout : timeout value in ms
%% Removes lock requests that are older than the specified timeout value form local_locks
%% Returns the updated lokal_locks list
-spec remove_old_required_locks([{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}],non_neg_integer()) -> [{txid(),{atom(),[key()],[key()],erlang:timestamp()}|{atom(),[key()],[key()]}}].
remove_old_required_locks(Local_Locks,Timeout) ->
    Current_Time = erlang:timestamp(),
    _New_Local_Locks=orddict:filter(
                fun(_Key,Value) ->
                        case Value of
                                {required,_Shared_Lock_List,_Exclusive_Lock_List,Timestamp} ->
                                         timer:now_diff(Current_Time,Timestamp)< Timeout;
                                {using,_Shared_Lock_List,_Exclusive_Lock_List}->
                                        true
                        end
                end,Local_Locks).

%  Data structure: lock_requests - [{dcid,[{lock,amount,timestamp}]}]  - amount: int() | all
%  Used to memorize which DC requested how many part of a lock

%TODO
%% Locks : list of {lock,amount} tuple to be added to lock_requests
%% DcId : dcid that requested the locks
%% Timestamp : timestamp of the request
%% Lock_Requests : orddict managing all remote lock requests
%% Adds these lock requests with the DcId to the lock_request list
-spec requested([{key(),non_neg_integer() | all}],dcid(),erlang:timestamp(),[{dcid(),[{key(),non_neg_integer() | all,erlang:timestamp()}]}]) -> [{dcid(),[{key(),erlang:timestamp()}]}].
requested(Locks, DcId, Timestamp, Lock_Requests) ->
        case orddict:find(DcId,Lock_Requests) of
                {ok,Corresponding_Lock_Requests} ->
                        Updated_Lock_Reuqests=lists:foldl(fun({Lock,Amount},AccIn)-> lists:keystore(Lock,1,AccIn,{Lock,Amount,Timestamp})
                                                          end,Corresponding_Lock_Requests,Locks),
                        orddict:store(DcId,Updated_Lock_Reuqests,Lock_Requests);
                error ->
                        New_Lock_Request_List = lists:foldl(fun({Lock,Amount},AccIn)-> lists:keystore(Lock,1,AccIn,{Lock,Amount,Timestamp})
                                                            end,[],Locks),
                        orddict:store(DcId,New_Lock_Request_List,Lock_Requests)
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
-spec clear_old_lock_requests([{dcid(),[{key(),non_neg_integer()| all,erlang:timestamp()}]}],non_neg_integer()) -> [{dcid(),[{key(),non_neg_integer() | all, erlang:timestamp()}]}].
clear_old_lock_requests(Lock_Requests,Timeout)->
    Current_Time = erlang:timestamp(),
    orddict:fold(
        fun(DCID,Lock_Amount_Timestamp_List,AccIn)->
            Filtered_List = lists:filter(fun({_Lock,_Amount,Timestamp_2}) -> timer:now_diff(Current_Time,Timestamp_2)< Timeout end,Lock_Amount_Timestamp_List),
            case length(Filtered_List)==0 of
                                false ->
                                    orddict:store(DCID,Filtered_List,AccIn);
                                true ->
                                        AccIn
                        end
        end,
    orddict:new(),Lock_Requests).




% Data sturcture: dets_ref : [{lock,[{from,[{to,amount}]}],last_modified}]
% Used to persistently store which DC currently owns how many parts of a lock

%% Lock : Lock for which to create an entry
%% Creates a tuple according to dets_ref that contains a data structure containing the amount of lock parts send from every
%% DC to every DC.
-spec create_dets_ref_lock_entry(key()) -> {key(),[{dcid(),[{dcid(),non_neg_integer()}]}],snapshot_time()}.
create_dets_ref_lock_entry(Lock) ->
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    OtherDCDescriptors = dc_meta_data_utilities:get_dc_descriptors(),
    AllDCIds = lists:foldl(fun(#descriptor{dcid=Id}, IdsList) ->
                                [Id | IdsList]
                             end, [], OtherDCDescriptors),
    AllDCIds2 =
        case lists:member(MyDCId,AllDCIds) of
            true -> AllDCIds;
            false -> [MyDCId|AllDCIds]
        end,
    %{ok,Now} = get_snapshot_time(),
    Now = dict:from_list([]),
    _New_Dets_Ref_Table_Entry =
        {Lock,lists:foldl(fun(DCId_From,AccIn1)->
            [{DCId_From,lists:foldl(fun(DCId_To,AccIn2)->
                [{DCId_To,0}|AccIn2]
            end,[],AllDCIds2)}|AccIn1]
        end,[],AllDCIds2),Now}.

%% Returns the amount of lock parts a lock should be initialized with
-spec lock_part_amount() -> non_neg_integer().
lock_part_amount() ->
    Other_DC_IDs = [dc_meta_data_utilities:get_my_dc_id() | other_dcs_list()],
    round(length(Other_DC_IDs)*?LOCK_PART_TO_DC_FACTOR) + ?LOCK_PART_TO_DC_OFFSET.


%% Lock : lock to send to another dc
%% Amount : amount of lock parts to send
%% To : dc where the locks should be send to
%% Allways send a messge to the other DC, even if the lock may currently not be send or is not owned, to make up for lost messages
%% Updates the {send,dcid,[{to,amount}]} entry of dets_ref of the specified lock
-spec send_lock(key(),non_neg_integer() | all ,dcid()) -> ok.
send_lock(Lock,Amount,To)->
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,_Lock_Send_History,Snapshot}]->
            Owned_Lock_Parts = count_lock_parts(Lock, MyDCId),
            case Amount of
                all ->
                    update_dets_ref_send_entry(Lock, MyDCId, To, Owned_Lock_Parts),
                    New_Lock_Send_History = get_send_history_of(Lock),
                    remote_send_lock_history(Lock,New_Lock_Send_History,Snapshot,MyDCId,To,0); %TODO Needs to be implemented accordingly
                _ ->
                    Amount_Needed = Amount - count_lock_parts(Lock,To),
                    Amount_To_Send = case (Owned_Lock_Parts > Amount_Needed)of
                                         true when (Amount_Needed > 0) -> Amount_Needed;
                                         false -> Owned_Lock_Parts - 1;
                                         _ -> 0
                                     end,
                    case ((Amount_To_Send > 0) and is_integer(Amount_To_Send)) of
                        true ->
                            update_dets_ref_send_entry(Lock, MyDCId, To, Amount_To_Send),
                            New_Lock_Send_History = get_send_history_of(Lock),
                            remote_send_lock_history(Lock,New_Lock_Send_History,Snapshot,MyDCId,To,0); % TODO Send the whole {lock,send_history,snaphot} tuple
                        false -> ok
                    end
            end;
        [] ->
            case am_i_leader() of
                true ->
                    New_Lock_Parts = lock_part_amount(),
                    Amount_To_Send = case Amount of
                                         all -> New_Lock_Parts;
                                         _ -> case New_Lock_Parts > Amount of
                                                  true -> Amount;
                                                  false -> New_Lock_Parts - 1
                                              end
                                     end,
                    {Lock,Send_History,_Snapshot} = create_dets_ref_lock_entry(Lock),
                    %TODO Check if dict:from_list([]) works as intended. The receiving DC should not have to wait for a
                    % snapshot without introducing any other unwanted interactions.
                    dets:insert(?DETS_FILE_NAME,{Lock,Send_History,dict:from_list([])}),
                    update_dets_ref_send_entry(Lock, MyDCId, MyDCId, New_Lock_Parts),
                    update_dets_ref_send_entry(Lock, MyDCId, To, Amount_To_Send),
                    New_Lock_Send_History = get_send_history_of(Lock),
                    remote_send_lock_history(Lock,New_Lock_Send_History,dict:from_list([]),MyDCId,To,0),
                    ok;
                false ->
                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                    ok
            end;
        {error,Reason} ->
            {error,Reason}
    end.

-spec get_send_history_of(key()) -> {key(),[{dcid(),[{dcid(),non_neg_integer() | all}]}],snapshot_time()}.
get_send_history_of(Lock) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,Send_History,_Snapshot}] ->
            Send_History;
        {error,Reason} ->
            {error,Reason};
        _ ->
            {error,entry_not_found}
    end.

%% Lock : Lock which lock parts are send
%% From : DC_ID which send lock parts
%% To : DC_ID which receives lock parts
%% Additional_Amount : Additional amount of lock parts send from From to To.
%% Updates the corresponding dets_ref entry with the new amount, if it is bigger than the old one
-spec update_dets_ref_send_entry(key(),dcid(),dcid(),non_neg_integer())-> ok.
update_dets_ref_send_entry(Lock,From,To,Additional_Amount) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,Send_History,Snapshot}] ->
                    case lists:keyfind(From,1,Send_History) of
                        false ->
                            New_Send_History = lists:keystore(From,1,Send_History,{From,[{To,Additional_Amount}]}),
                            dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                        {From,To_Amount_List} ->
                            case lists:keyfind(To,1,To_Amount_List) of
                                false ->
                                    New_To_Amount_List = lists:keystore(To,1,To_Amount_List,{To,Additional_Amount}),
                                    New_Send_History = lists:keyreplace(From,1,Send_History,{From,New_To_Amount_List}),
                                    dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                                {To,Old_Amount}->
                                    case 0 < Additional_Amount of
                                        true ->
                                            New_To_Amount_List = lists:keyreplace(To,1,To_Amount_List,{To,Old_Amount+Additional_Amount}),
                                            New_Send_History = lists:keyreplace(From,1,Send_History,{From,New_To_Amount_List}),
                                            dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                                        false -> ok
                                    end
                            end
                    end;
                []->
                    %{ok,Now} = get_snapshot_time(),
                    dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{To,Additional_Amount}]}],dict:from_list([])});
                {error,_Reason} ->
                    %{ok,Now} = get_snapshot_time(),
                    dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{To,Additional_Amount}]}],dict:from_list([])})
    end.
%% Old_Send_History : Send history of a specific lock to be updated
%% New_Send_History : Send history of the same lock to update the other one
%% Updates every value in Old_Send_History by allways choosing the max value of both send histories.
%% (How many lock parts were send in totoal from one DC to another DC)
-spec update_send_history([{dcid(),[{dcid(),non_neg_integer() | all}]}],[{dcid(),[{dcid(),non_neg_integer() | all}]}]) ->[{dcid(),[{dcid(),non_neg_integer() | all}]}].
update_send_history(Old_Send_History,New_Send_History) ->
    % Update all entries of Old_Send_History and memorize those DCs, for which Old_Send_History has no entry
    {Updated_Send_History,To_Add_From} = lists:foldl(fun({From1,To_Amount_List1},{Acc1,Acc2})->
            case lists:keyfind(From1,1,New_Send_History) of
                {From1,To_Amount_List2} ->
                    % Update all entries of Old_Send_History's To_Amount_List and memorize those DCs, for which Old_Send_History's To_Amount_List has no entry
                    {New_To_Amount_List1,To_Add_List} = lists:foldl(fun({To1,Amount1},{New_To_Amount_List2,To_Add})->
                            case lists:keyfind(To1,1,To_Amount_List2) of
                                {To1,Amount2} ->
                                    New_Amount = case Amount1 > Amount2 of
                                        true -> Amount1;
                                        false -> Amount2
                                    end,
                                    {[{To1,New_Amount}|New_To_Amount_List2],[To1|Acc2]};
                                false ->
                                    {[{To1,Amount1}|New_To_Amount_List2],To_Add}
                            end
                        end,{[],[]},To_Amount_List1),
                    % Add all entries that were missing in Old_Send_History's To_Amount_List but present in New_Send_History's To_Amount_List
                    Added_New_To_Amount_List1 = lists:foldl(fun({To2,Amount3},Acc)->
                            case lists:member(To2,To_Add_List) of
                                true -> Acc;
                                false -> [{To2,Amount3}|Acc]
                            end
                        end,New_To_Amount_List1,To_Amount_List2),
                    {[{From1,Added_New_To_Amount_List1}|Acc1],[From1|Acc2]};
                false ->
                    {[{From1,To_Amount_List1}|Acc1],Acc2}
                end
        end,{[],[]},Old_Send_History),
    % Add all entries that were missing in Old_Send_History but present in New_Send_History
    lists:foldl(fun({From2,To_Amount_List3},Acc3)->
            case lists:member(From2,To_Add_From) of
                true -> Acc3;
                false -> [{From2,To_Amount_List3}|Acc3]
            end
        end,Updated_Send_History,New_Send_History).

%% Old_Send_History : Send history of a specific lock to be updated
%% New_Send_History : Send history of the same lock to update the other one
%% Updates every value in Old_Send_History by allways choosing the max value of both send histories.
%% (How many lock parts were send in totoal from one DC to another DC)
-spec update_send_history2([{dcid(),[{dcid(),non_neg_integer() | all}]}],[{dcid(),[{dcid(),non_neg_integer() | all}]}]) ->[{dcid(),[{dcid(),non_neg_integer() | all}]}].
update_send_history2(Old_Send_History,New_Send_History) ->
    Sorted_Old_Send_History = lists:keysort(1,Old_Send_History),
    Sorted_New_Send_History = lists:keysort(1,New_Send_History),
    orddict:merge(fun(_Key,Value1,Value2)->
                          Sorted_Value1 = lists:keysort(1,Value1),
                          Sorted_Value2 = lists:keysort(1,Value2),
                          orddict:merge(fun(_Key2,Value3,Value4)->
                                case Value3 > Value4 of
                                    true -> Value3;
                                    false -> Value4
                                end
                            end,Sorted_Value1,Sorted_Value2)
                    end,Sorted_Old_Send_History,Sorted_New_Send_History).
%TODO
%% Lock : lock received by another dc
%% From : dc which send the lock
%% New_Send_History : History of lock parts send form each DC to each DC
%% Last_Changed : snapshot when the lock was released the last time
%% Updates the {Lock,Send_History,Snapshot} entry of dets_ref of the specified lock
-spec received_lock(key(),dcid(),[{dcid(),[{dcid(),non_neg_integer() | all}]}],snapshot_time()) -> ok.
received_lock(Lock,_From,New_Send_History,Last_Changed)->  %TODO Change so that it updates the send history of the respective lock
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,Send_History,Old_Snapshot}] ->
            Snapshot = vectorclock:max([Old_Snapshot,Last_Changed]),
            Updated_Send_History = update_send_history2(Send_History, New_Send_History), %TODO Decide if update_send_history2 or update_send_history is better (also check if both are correct)
            dets:insert(?DETS_FILE_NAME,{Lock,Updated_Send_History,Snapshot});
        []->
            dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Last_Changed});
        {error,Reason} ->
            {error,Reason}
    end.
%TODO
%% Lock : lock to be checked
%% DC_ID : DC_ID of the DC which lock parts are to be counted
%% Retruns the amount of lock parts of the specific lock currently owned the specified DC
-spec count_lock_parts(key(),dcid()) -> non_neg_integer().
count_lock_parts(Lock,DC_ID) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,Lock_Information,_}] ->
                    Total_Send = case lists:keyfind(DC_ID,1,Lock_Information) of
                        false ->
                            error; %TODO New DC Added to the DDB
                         {DC_ID,Send_To_Information}->
                            lists:foldl(fun({To,Amount},Acc) ->
                                                case To of
                                                    DC_ID -> Acc;
                                                    _ ->Acc+Amount
                                                end
                                        end,0,Send_To_Information)
                    end,
                    Total_Received = lists:foldl(fun({_From,To_List},Acc) ->
                                                Acc+case lists:keyfind(DC_ID,1,To_List) of
                                                        false ->
                                                            error; %TODO New DC added to the DDB
                                                        {DC_ID,Amount}->
                                                            Amount
                                                end end,0,Lock_Information),
                    _Has_Lock_Parts = Total_Received - Total_Send;
                [] ->
                        MyDCId = dc_meta_data_utilities:get_my_dc_id(),
                        case am_i_leader() of
                                true ->
                                    New_Lock_Parts = lock_part_amount(),
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    update_dets_ref_send_entry(Lock, MyDCId, MyDCId, New_Lock_Parts),
                                    case MyDCId == DC_ID of
                                        true -> New_Lock_Parts;
                                        false -> 0
                                    end;
                                false ->
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    0
                        end;
                {error,Reason} ->
                        {error,Reason}
        end.
%TODO
%% Lock : lock to be checked
%% Returns true if at least one lock part is currently owned by this DC
%% Returns false if none is owned by this DC
-spec check_lock_shared(key()) -> boolean().
check_lock_shared(Lock) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,Lock_Information,_}] ->
                    Total_Send = case lists:keyfind(MyDCId,1,Lock_Information) of
                        false ->
                            error; %TODO New DC Added to the DDB
                         {MyDCId,Send_To_Information}->
                            lists:foldl(fun({To,Amount},Acc) ->
                                                case To of
                                                    MyDCId -> Acc;
                                                    _ ->Acc+Amount
                                                end
                                        end,0,Send_To_Information)
                    end,
                    Total_Received = lists:foldl(fun({_From,To_List},Acc) ->
                                                Acc+case lists:keyfind(MyDCId,1,To_List) of
                                                        false ->
                                                            error; %TODO New DC added to the DDB
                                                        {MyDCId,Amount}->
                                                            Amount
                                                end end,0,Lock_Information),
                    _Has_Lock = Total_Send < Total_Received;
                [] ->
                        case am_i_leader() of
                                true ->
                                    New_Lock_Parts = lock_part_amount(),
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    update_dets_ref_send_entry(Lock, MyDCId, MyDCId, New_Lock_Parts),
                                    true;
                                false ->
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    false
                        end;
                {error,Reason} ->
                        {error,Reason}
        end.
%TODO
%% Lock : lock to be checked
%% Returns true if all lock parts are currently owned by this DC
%% Returns false if not all are owned by this DC
-spec check_lock_exclusive(key()) -> boolean().
check_lock_exclusive(Lock) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    MyDCId = dc_meta_data_utilities:get_my_dc_id(),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
                [{Lock,Lock_Information,_}] ->
                    Total_Send = case lists:keyfind(MyDCId,1,Lock_Information) of
                        false ->
                            error; %TODO New DC Added to the DDB
                         {MyDCId,Send_To_Information}->
                            lists:foldl(fun({To,Amount},Acc) ->
                                                case To of
                                                    MyDCId -> Acc;
                                                    _ ->Acc+Amount
                                                end
                                        end,0,Send_To_Information)
                    end,
                    Total_Received = lists:foldl(fun({_From,To_List},Acc) ->
                                                Acc+case lists:keyfind(MyDCId,1,To_List) of
                                                        false ->
                                                            error; %TODO New DC added to the DDB
                                                        {MyDCId,Amount}->
                                                            Amount
                                                end end,0,Lock_Information),
                    Total_Generated = lists:foldl(fun({From,To_List},Acc) ->
                                                  Acc+  case lists:keyfind(From,1,To_List) of
                                                            false ->
                                                                error; %TODO New DC added to the DDB
                                                            {From,Amount}->
                                                                Amount
                                                        end
                                                  end,0,Lock_Information),
                    _Has_Lock = (Total_Generated == (Total_Received - Total_Send)) and (Total_Generated > 0);
                [] ->
                        case am_i_leader() of
                                true ->
                                    New_Lock_Parts = lock_part_amount(),
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    update_dets_ref_send_entry(Lock, MyDCId, MyDCId, New_Lock_Parts),
                                    true;
                                false ->
                                    New_Dets_Ref_Entry = create_dets_ref_lock_entry(Lock),
                                    dets:insert(?DETS_FILE_NAME,New_Dets_Ref_Entry),
                                    false
                        end;
                {error,Reason} ->
                        {error,Reason}
        end.
%TODO
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
%% Returns a list of the snapshot times the specified locks were released the last time as exclusive locks

-spec get_last_modified([key()]) -> [snapshot_time()].
get_last_modified([])->
    [dict:from_list([])|[]];
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
%TODO
%% Exclusive_Locks : list of exclusive locks
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
%TODO
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

%TODO
%% MyDCId : DCID of this DC
%% Key : ? TODO
%% Locks : list of locks that are to be requested
%% sends a message to all other DCs requesting the Locks.
-spec remote_lock_request_es(dcid(),term(),[key()],[key()])-> ok.
remote_lock_request_es(MyDCId, Key, Shared_Locks,Exclusive_Locks) ->
    {LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    Other_DCs_List = other_dcs_list(),
    Locks = [{Key1,1} || Key1 <- Shared_Locks] ++ [{Key2,all} || Key2 <- Exclusive_Locks],
    spawn(fun()->   %TODO
    lists:foldl(
        fun(RemoteId,AccIn) ->
            BinaryMsg = term_to_binary({request_locks_es,
            {remote_lock_request_es, {Locks, MyDCId}}, LocalPartition, MyDCId, RemoteId}),
            [inter_dc_query:perform_request(?LOCK_MGR_ES_REQUEST, {RemoteId, LocalPartition},
                BinaryMsg, fun lock_mgr_es:request_response_es/2) | AccIn]
        end
        ,[],Other_DCs_List)
    end), %TODO
    ok.

%TODO
%% Lock : Lock to send to another DC
%% Send_History : History of lock parts send from each DC to each DC
%% Last_Changed : Snapshot of the last time this lock was released
%% MyDCId : DCID of this DC
%% RemoteId : DCID of the DC the lock is send to
%% Key : ? TODO
%% sends a message to the speciefed other DC containing the lock information
-spec remote_send_lock_history(key(),[{dcid(),[{dcid(),non_neg_integer() | all}]}],snapshot_time(),dcid(),dcid(),key())-> ok.
remote_send_lock_history(Lock,Send_History, Last_Changed,MyDCId, RemoteId, Key) ->
    %lager:info("remote_send_lock_history : ~w,~w,~w},state)~n",[Lock,RemoteId,MyDCId]),
    {LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    BinaryMsg = term_to_binary({send_locks_es,
        {remote_send_lock_history, {Lock,Send_History, Last_Changed,MyDCId, RemoteId}}, LocalPartition, MyDCId, RemoteId}),
    spawn(fun()->   %TODO
    inter_dc_query:perform_request(?LOCK_MGR_ES_SEND, {RemoteId, LocalPartition},
        BinaryMsg, fun lock_mgr_es:request_response_es/2)
    end), %TODO
    ok.

%% Request response - do nothing. TODO  what is this function meant to do ?
request_response_es(_BinaryRep, _RequestCacheEntry) -> ok.
% ===================================================================
% Callbacks
% ===================================================================

%% Releases all locks currently owned by the specified transaction.
handle_cast({release_locks,TxId}, #state{local_locks=Local_Locks}=State) ->
        %lager:info("handle_cast({release_lock,~w},state)~n",[TxId]),
        New_Local_Locks = release_locks(TxId,Local_Locks),
        {noreply, State#state{local_locks=New_Local_Locks}};
%TODO
%% Takes a Lock, amount(number of times this lock was send to this DC by From), the senders DCID and the DCID of this DC
%% Stores in dets_ref how often the sender send the Lock to this DC
handle_cast({remote_send_lock_history, {Lock,Send_History,Snapshot,From,MyDCID1}}, State) ->
        %lager:info("handle_cast({remote_send_lock_history,~w,~w,~w,~w,~w},state)~n",[Lock,Send_History,Snapshot,From,MyDCID1]),
        MyDCID2 = dc_meta_data_utilities:get_my_dc_id(),
        case MyDCID1 == MyDCID2 of
                true ->
                        received_lock(Lock,From,Send_History, Snapshot),
                        {noreply, State};
                false -> {noreply, State}
        end;
%TODO
%% Takes a list of Locks and a Sender as input
%% Adds {dcid,[{lock,timestamp}]} to lock_requests to remember which DC requested which Locks
%% Adds a timestamp to filter too old requests
handle_cast({remote_lock_request_es, {Locks, Sender}}, #state{lock_requests=Lock_Requests}=State) ->
    %lager:info("handle_cast({remote_lock_request_es,~w,~w},from,state)~n",[Locks,Sender]),
    Timestamp = erlang:timestamp(),
    New_Lock_Requests = requested(Locks, Sender, Timestamp, Lock_Requests),
        {noreply, State#state{lock_requests=New_Lock_Requests}}.

%TODO
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
handle_call({get_locks,TxId,Shared_Locks,Exclusive_Locks}, _From, #state{local_locks=Local_Locks}=State) ->
    %lager:info("handle_call({get_locks_es,~w,~w,~w},from,state)~n",[TxId,Shared_Locks,Exclusive_Locks]),
    New_Shared_Locks = Shared_Locks--Exclusive_Locks,
    case using(Shared_Locks,Exclusive_Locks, TxId, Local_Locks) of
        {missing_locks, Missing_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started missing_locks-- ~n",[TxId,Locks]),
            New_Local_Locks2=required_remote(New_Shared_Locks,Exclusive_Locks,Missing_Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished missing_locks-- ~n",[TxId,Locks]),
            {reply, {missing_locks, Missing_Locks} , State#state{local_locks=New_Local_Locks2}};
        {locks_in_use, Transactions_Using_The_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started locks_in_use-- ~n",[TxId,Locks]),
            New_Local_Locks3=required(New_Shared_Locks,Exclusive_Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished locks_in_use-- ~n",[TxId,Locks]),
            {reply, {locks_in_use, Transactions_Using_The_Locks} , State#state{local_locks=New_Local_Locks3}};
        New_Lokal_Locks1 ->
            % get the list of snapshots the locks were released the last time
            Snapshot_Times = get_last_modified(what_locks_to_wait_for(New_Shared_Locks,Exclusive_Locks)),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished ok-- ~n",[TxId,Locks]),
            {reply, {ok,Snapshot_Times}, State#state{local_locks=New_Lokal_Locks1}}
    end.


%% Periodically transfers locks requested by other DCs to them, if they are currently not used
handle_info(transfer_periodic, #state{lock_requests=Old_Lock_Requests,local_locks= Local_Locks, transfer_timer=OldTimer}=State) ->
    erlang:cancel_timer(OldTimer),
    Clean_Lock_Requests = clear_old_lock_requests(Old_Lock_Requests, ?LOCK_REQUEST_TIMEOUT_ES),
    Clear_Local_Locks = remove_old_required_locks(Local_Locks,?LOCK_REQUIRED_TIMEOUT_ES),
    %lager:info("handle_info({transfer_periodic},clear_local_locks=~w,clear_lock_requests =~w ~n",[Clear_Local_Locks,Clean_Lock_Requests]),
    % goes through all lock reuests. If the request is NOT in local_locks then the lock is send to the requesting DC
    New_Lock_Requests = orddict:fold(
        fun(DCID,Lock_Amount_Timestamp_List,Changed_Lock_Requests)->
            Updated_Lock_Amount_Timestamp_List = lists:foldl(
                fun({Lock,Amount, Timestamp},New_Lock_Amount_Timestamp_List) ->
                {In_Use,Required} = orddict:fold(
                    fun(_TxId,Value2,{AccIn,AccInRequired}) ->
                        case Value2 of
                            {using,Shared_Lock_List,Exclusive_Lock_List} ->
                                case lists:member(Lock,Exclusive_Lock_List) of
                                    true -> {all,AccInRequired};
                                    false ->
                                        case lists:member(Lock,Shared_Lock_List) of
                                            true ->
                                                case AccIn of
                                                    all -> {all,AccInRequired};
                                                    _ -> {1,AccInRequired}
                                                end;
                                            false ->
                                                {AccIn,AccInRequired}
                                        end
                                    end;
                            {required,Shared_Lock_List2,Exclusive_Lock_List2,_Timestamp2} ->
                                case lists:member(Lock,Exclusive_Lock_List2) of
                                    true -> {AccIn,all};
                                    false ->
                                        case lists:member(Lock,Shared_Lock_List2) of
                                            true ->
                                                case AccInRequired of
                                                    all -> {AccIn,all};
                                                    _ -> {AccIn,1}
                                                end;
                                            false ->
                                                {AccIn,AccInRequired}
                                        end
                                    end;
                            _ -> {AccIn,AccInRequired}
                        end
                    end,
                {0,0},Clear_Local_Locks),
                case {In_Use,Required} of
                    {all,_} ->
                        [{Lock,Amount, Timestamp} | New_Lock_Amount_Timestamp_List];
                    {1,all} when (Amount /= all)->
                        MyDCId = dc_meta_data_utilities:get_my_dc_id(),
                        case MyDCId < DCID of
                            true -> [{Lock,Amount, Timestamp} | New_Lock_Amount_Timestamp_List];
                            false-> send_lock(Lock,Amount + ?ADDITIONAL_LOCK_PARTS_SEND,DCID),
                                    New_Lock_Amount_Timestamp_List
                        end;
                    {1,1} when (Amount /= all) ->
                        send_lock(Lock,Amount + ?ADDITIONAL_LOCK_PARTS_SEND,DCID),
                        New_Lock_Amount_Timestamp_List;
                    {1,0} when (Amount /= all) ->
                        send_lock(Lock,Amount + ?ADDITIONAL_LOCK_PARTS_SEND,DCID),
                        New_Lock_Amount_Timestamp_List;
                    {1,_} when (Amount == all)->
                        [{Lock,Amount, Timestamp} | New_Lock_Amount_Timestamp_List];
                    {0,all} ->
                        MyDCId = dc_meta_data_utilities:get_my_dc_id(),
                        case MyDCId < DCID of
                            true -> [{Lock,Amount, Timestamp} | New_Lock_Amount_Timestamp_List];
                            false-> send_lock(Lock,Amount,DCID),
                                    New_Lock_Amount_Timestamp_List
                        end;
                    {0,1} when (Amount /= all) ->
                        send_lock(Lock,Amount + ?ADDITIONAL_LOCK_PARTS_SEND,DCID),
                        New_Lock_Amount_Timestamp_List;
                    {0,1} when (Amount == all) ->   %Since exclusive lock requests are prioritized above shared lock requests
                        send_lock(Lock,Amount,DCID),
                        New_Lock_Amount_Timestamp_List;
                    {0,0} when (Amount /= all)->
                        send_lock(Lock,Amount + ?ADDITIONAL_LOCK_PARTS_SEND,DCID),
                        New_Lock_Amount_Timestamp_List;
                    {0,0} when (Amount == all)->
                        send_lock(Lock,Amount,DCID),
                        New_Lock_Amount_Timestamp_List;
                    _ ->
                        [{Lock,Amount, Timestamp} | New_Lock_Amount_Timestamp_List]
                end
            end,[],Lock_Amount_Timestamp_List),
        case Updated_Lock_Amount_Timestamp_List of
            [] ->
                Changed_Lock_Requests;
            _ ->
                [{DCID,Updated_Lock_Amount_Timestamp_List}|Changed_Lock_Requests]
        end
        end,[],Clean_Lock_Requests),
    NewTimer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY_ES, self(), transfer_periodic),
    New_Lock_Requests_Value = case ?REDUCED_INTER_DC_COMMUNICATION_ES of
        true -> New_Lock_Requests;
        false -> Clean_Lock_Requests
    end,
    {noreply, State#state{transfer_timer= NewTimer,local_locks=Clear_Local_Locks,lock_requests=New_Lock_Requests_Value}}.


terminate(_Reason, _State) ->
    dets:close(?DETS_FILE_NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
