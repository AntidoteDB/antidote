


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
% Used leader election strategy - The leader is chosen based on the order of dc_ids
% Deadlock prevention strategy - Deadlocks are prevented by prioritizing DCs according to the
%   order of dc_ids.



-module(lock_mgr_es).
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

-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%TODO
%% Data Type: state
%% local_locks: list of currently requeired and used locks  [{txid,{required,[locks()],timestamp}}|{txid,{using,[locks()]}} ]
%% lock_requests: list of locks requested by other DCs  [{dcid,[{lock,timestamp}]}]
%% transfer_timer: timer to shedule periodic lock transfers
%% dets_ref: dets table storing received and send locks [{lock,{{send,dcid,[{to,amount}]},{received,dcid,[{from,amount}]}}}]
-record(state, {local_locks,lock_requests, transfer_timer,dets_ref}).
-define(LOG_UTIL, log_utilities).
-define(DATA_TYPE, antidote_crdt_counter_b).
-define(LOCK_REQUEST_TIMEOUT, 1000000).     % Locks requested by other DCs ( in microseconds -- 1 000 000 equals 1 second)
-define(LOCK_REQUIRED_TIMEOUT,500000).     % Locks requested by transactions of this DC (in microseconds -- 1 000 000 equals 1 second)
-define(LOCK_TRANSFER_FREQUENCY,1000).
%-define(DETS_FILE_NAME, "lock_mgr_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id()))++ "_" ++lists:concat(tuple_to_list(element(2,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_FILE_NAME, filename:join(app_helper:get_env(riak_core, platform_data_dir),"lock_mgr_es_persistant_storage_"++ atom_to_list(element(1,dc_meta_data_utilities:get_my_dc_id())))).
-define(DETS_SETTINGS, [{access, read_write},{auto_save, 180000},{estimated_no_objects, 256},{file, ?DETS_FILE_NAME},
                        {min_no_slots, 256},{keypos, 1},{ram_file, false},{repair, true},{type, set}]).
-define(DC_UTIL, dc_utilities).
-define(DC_META_UTIL, dc_meta_data_utilities).
-define(LOCK_PART_TO_DC_FACTOR,2). % float() #Lock_Parts = round(#DCs*LOCK_PART_TO_DC_FACTOR) + LOCK_PART_TO_DC_OFFSET
-define(LOCK_PART_TO_DC_OFFSET,0). % non_neg_integer()
% ===================================================================
% Public API
% ===================================================================
%TODO
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
%TODO
init([]) ->
    lager:info("Started Lock manager at node ~p", [node()]),
    Timer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
    {ok, Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    {ok, #state{local_locks = orddict:new(),lock_requests=orddict:new(), transfer_timer=Timer,dets_ref=Ref }}.
%TODO
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
%TODO
%% callback function used for inter dc communication
%% Handles incomming lock send to this dc by another dc
send_locks_remote({remote_send_lock, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_send_lock, TransferOp}).
%TODO
%% callback function used for inter dc communication
%% Handles incomming remote lock requests comming from other dcs
request_locks_remote({remote_lock_request, TransferOp}) ->
    gen_server:cast(?MODULE, {remote_lock_request, TransferOp}).

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
%% Sends remote_lock_request(Locks,0,dcid) messages to all other DCs.
-spec required([key()],[key()],txid(),erlang:timestamp(),[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}].
required(Shared_Locks,Exclusive_Locks,TxId,Timestamp,Local_Locks) ->
    DCID = dc_meta_data_utilities:get_my_dc_id(),
    remote_lock_request(DCID, 0, Shared_Locks),  % TODO
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
                [],Shared_Locks),
        case {Missing_Locks_Exclusive,Missing_Locks_Shared} of
                {[],[]} -> 
                    case exclusive_locks_used_by_other_tx(Exclusive_Locks,Local_Locks) of
                        [] ->
                            _New_Local_Locks = orddict:store(TxId,{using,Shared_Locks,Exclusive_Locks},Local_Locks);
                        Used_Exclusive_Locks_List -> 
                            {locks_in_use, Used_Exclusive_Locks_List}
                    end;
                Missing_Locks_Lists -> 
                    {missing_locks,Missing_Locks_Lists}
        end.

%% Exclusive_Locks : Exclusive that are to be checked if they are used by a transaction (of this dc)
%% Local_Locks : orddict managing all transaction requesting and using locks 
%% Returns [{txid(),[locks]}] for all transactions that currently use the exclusive locks specified by Exclusive_Locks (only the intersection is returned)
-spec exclusive_locks_used_by_other_tx([key()],[{txid(),{atom(),[key()],erlang:timestamp()}|{atom(),[key()]}}]) -> [{txid(),[key()]}].
exclusive_locks_used_by_other_tx(Exclusive_Locks,Local_Locks) ->
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
                    [],Exclusive_Locks),
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
        {ok,{using,_Shared_Locks,Exclusive_Locks}} ->
            case Exclusive_Locks of
                [] -> ok;
                _ -> update_last_changed(Exclusive_Locks)
            end;
        error ->
            lager:error("release_locks(~w,~w)~n",[TxId,Local_Locks]),
            ok
    end,
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).

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
            false -> MyDCId
        end,
    {ok,Now} = get_snapshot_time(),
    _New_Dets_Ref_Table_Entry =
        {Lock,lists:foldl(fun(DCId_From,AccIn1)->
            [{DCId_From,lists:fold(fun(DCId_To,AccIn2)->
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
                    remote_send_lock(Lock,New_Lock_Send_History,Snapshot,MyDCId,To,0); %TODO Needs to be implemented accordingly
                _ ->
                    Amount_To_Send = case Owned_Lock_Parts > Amount of
                                         true -> Amount;
                                         false -> Owned_Lock_Parts - 1
                                     end,
                    
                    update_dets_ref_send_entry(Lock, MyDCId, To, Amount_To_Send)
                    New_Lock_Send_History = get_send_history_of(Lock),
                    remote_send_lock(Lock,New_Lock_Send_History,Snapshot,MyDCId,To,0) % TODO Send the whole {lock,send_history,snaphot} tuple
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
                    {Lock,Send_History,Snapshot} = create_dets_ref_lock_entry(Lock),
                    dets:insert(?DETS_FILE_NAME,{Lock,Send_History,Snapshot}),
                    update_dets_ref_send_entry(Lock, MyDCId, MyDCId, New_Lock_Parts),
                    update_dets_ref_send_entry(Lock, MyDCId, To, Amount_To_Send),
                    New_Lock_Send_History = get_send_history_of(Lock),
                    remote_send_lock(Lock,New_Lock_Send_History,Snapshot,MyDCId,To,0);
                    ok;
                false ->
                    ok
            end;
        {error,_Reason} ->
            ok
    end.

-spec get_send_history_of(key()) -> {key(),[{dcid(),[{dcid(),non_negative_integer() | all}]}],snapshot_time()}
get_send_history_of(Lock) ->
    {ok, _Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,Send_History,Snapshot}] ->
            Send_History;
        {error,_Reason} ->
            {error,_Reason}
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
                    {ok,Now} = get_snapshot_time(),
                    dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{To,Additional_Amount}]}],Now});
                {error,_Reason} ->
                    {ok,Now} = get_snapshot_time(),
                    dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{To,Additional_Amount}]}],Now})
    end.
        

%TODO
%% Lock : lock received by another dc
%% From : dc which send the lock
%% Amount : number of lock parts this dc received from the other dc(From)
%% Last_Changed : snapshot when the lock was released the last time
%% Updates the {Lock,[{From,[{To,Amount}]}],Snapshot} entry of dets_ref of the specified lock
-spec received_lock(key(),dcid(),non_neg_integer(),snapshot_time()) -> ok.
received_lock(Lock,From,Amount,Last_Changed)->
    case dets:lookup(?DETS_FILE_NAME,Lock) of
        [{Lock,Send_History,Old_Snapshot}] ->
            Snapshot = vectorclock:max([Old_Snapshot,Last_Changed]),
            MyDCId = dc_meta_data_utilities:get_my_dc_id(),
            case lists:keyfind(From,1,Send_History) of
                false ->
                    New_Send_History = lists:keystore(From,1,Send_History,{From,[{MyDCId,Amount}]}),
                    dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                {From,To_Amount_List} ->
                    case lists:keyfind(MyDCId,1,To_Amount_List) of
                        false ->
                            New_To_Amount_List = lists:keystore(MyDCId,1,To_Amount_List,{MyDCId,Amount}),
                            New_Send_History = lists:keyreplace(From,1,Send_History,{From,New_To_Amount_List}),
                            dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                        {MyDCId,Old_Amount}->
                            case Old_Amount < Amount of
                                true ->
                                    New_To_Amount_List = lists:keyreplace(MyDCId,1,To_Amount_List,{MyDCId,Amount}),
                                    New_Send_History = lists:keyreplace(From,1,Send_History,{From,New_To_Amount_List}),
                                    dets:insert(?DETS_FILE_NAME,{Lock,New_Send_History,Snapshot});
                                false -> ok
                            end
                    end
            end;
        []->
            MYDCID = dc_meta_data_utilities:get_my_dc_id(),
            dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{MYDCID,Amount}]}],Last_Changed});
        {error,_Reason} ->
            MYDCID = dc_meta_data_utilities:get_my_dc_id(),
            dets:insert(?DETS_FILE_NAME,{Lock,[{From,[{MYDCID,Amount}]}],Last_Changed})
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
                        {ok,Now} = get_snapshot_time(),
                        %TODO
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
                        {ok,Now} = get_snapshot_time(),
                        %TODO
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
                    _Has_Lock = Total_Generated == (Total_Send - Total_Received);
                [] ->
                        {ok,Now} = get_snapshot_time(),
                        %TODO
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
update_last_changed(Exclusive_Locks) ->
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
        ok,Exclusive_Locks).

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

%TODO
%% Lock : Lock to send to another DC
%% Amount : Total number of times the lock was send to that DC from this DC
%% Last_Changed : Snapshot of the last time this lock was released
%% MyDCId : DCID of this DC
%% RemoteId : DCID of the DC the lock is send to
%% Key : ? TODO
%% sends a message to the speciefed other DC containing the lock information
-spec remote_send_lock(key(),non_neg_integer(),snapshot_time(),dcid(),dcid(),key())-> ok.
remote_send_lock(Lock,Amount, Last_Changed,MyDCId, RemoteId, Key) ->
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
        lager:info("handle_cast({release_lock,~w},state)~n",[TxId]),
        New_Local_Locks = release_locks(TxId,Local_Locks),
        {noreply, State#state{local_locks=New_Local_Locks}};
%TODO
%% Takes a Lock, amount(number of times this lock was send to this DC by From), the senders DCID and the DCID of this DC
%% Stores in dets_ref how often the sender send the Lock to this DC
handle_cast({remote_send_lock, {Lock,Amount,Snapshot,From,MyDCID1}}, State) ->
        lager:info("handle_cast({remote_send_lock,~w,~w,~w,~w,~w},state)~n",[Lock,Amount,Snapshot,From,MyDCID1]),
        MyDCID2 = dc_meta_data_utilities:get_my_dc_id(),
        case MyDCID1 == MyDCID2 of
                true ->
                        received_lock(Lock,From,Amount, Snapshot),
                        {noreply, State};
                false -> {noreply, State}
        end;
%TODO
%% Takes a list of Locks and a Sender as input
%% Adds {dcid,[{lock,timestamp}]} to lock_requests to remember which DC requested which Locks
%% Adds a timestamp to filter too old requests
handle_cast({remote_lock_request, {Locks, Sender}}, #state{lock_requests=Lock_Requests}=State) ->
    lager:info("handle_cast({remote_lock_request,~w,~w},from,state)~n",[Locks,Sender]),
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
    lager:info("handle_call({get_locks,~w,~w,,~w},from,state)~n",[TxId,Shared_Locks,Exclusive_Locks]),
    New_Shared_Locks = Shared_Locks--Exclusive_Locks,
    case using(Shared_Locks,Exclusive_Locks, TxId, Local_Locks) of
        {missing_locks, Missing_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started missing_locks-- ~n",[TxId,Locks]),
            New_Local_Locks2=required(Shared_Locks,Exclusive_Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished missing_locks-- ~n",[TxId,Locks]),
            {reply, {missing_locks, Missing_Locks} , State#state{local_locks=New_Local_Locks2}};
        {locks_in_use, Transactions_Using_The_Locks} ->
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Started locks_in_use-- ~n",[TxId,Locks]),
            New_Local_Locks3=required(Shared_Locks,Exclusive_Locks, TxId, erlang:timestamp(), Local_Locks),
            %lager:info("handle_call({get_locks,~w,~w},from,state) --Finished locks_in_use-- ~n",[TxId,Locks]),
            {reply, {locks_in_use, Transactions_Using_The_Locks} , State#state{local_locks=New_Local_Locks3}};
        New_Lokal_Locks1 ->
            % get the list of snapshots the locks were released the last time
            Snapshot_Times = get_last_modified(Exclusive_Locks++New_Shared_Locks),
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
    orddict:fold(
        fun(DCID,Lock_Amount_Timestamp_List,ok)->
            lists:foldl(
                fun({Lock,Amount, _Timestamp},ok) ->
                In_Use = orddict:fold(
                    fun(_TxId,Value2,AccIn) ->
                        case Value2 of
                            {using,Shared_Lock_List,Exclusive_Lock_List} -> 
                                case lists:member(Lock,Exclusive_Lock_List) of
                                    true -> all;
                                    false ->
                                        case lists:member(Lock,Shared_Lock_List) of
                                            true -> 
                                                case AccIn of
                                                    all -> all;
                                                    _ -> 1
                                                end;
                                            false ->
                                                case AccIn of
                                                    all -> all;
                                                    1 -> 1;
                                                    0 -> 0
                                                end
                                        end
                                    end;
                            _ -> AccIn
                        end
                    end,
                0,Clear_Local_Locks),
                case In_Use of
                    all ->
                        ok;
                    _ ->
                        send_lock(Lock,Amount,DCID)
                end
            end,ok,Lock_Amount_Timestamp_List)
        end, ok,Clean_Lock_Requests),
    NewTimer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
    {noreply, State#state{transfer_timer= NewTimer,local_locks=Clear_Local_Locks}}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.