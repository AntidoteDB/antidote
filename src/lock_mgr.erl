-module(lock_mgr).
-behaviour(gen_server).

-export([start_link/0,
		 get_locks/2,
		 release_locks/1,
		 remote_lock_request/2
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

% for testing
-compile(export_all).
-export([stateinfo/0,
		 local_locks_info/0,
		 setup/0,
		 test1/0,
		 test2/0,
		 test3/0
		 ]).




%-include("antidote.hrl").
%-include("inter_dc_repl.hrl").


-record(state, {req_queue,local_locks,lock_requests, last_transfers, transfer_timer,dets_ref}).
-define(LOG_UTIL, log_utilities).
-define(DATA_TYPE, antidote_crdt_counter_b).
-define(LOCK_REQUEST_TIMEOUT, 1000000).  % Locks requested by other DCs ( in microseconds -- 1 000 000 equals 1 second)
-define(LOCK_REQUIRED_TIMEOUT,1000000). % Locks requested by transactions of this DC (in microseconds -- 1 000 000 equals 1 second)
-define(LOCK_TRANSFER_FREQUENCY,15000).
-define(DETS_FILE_NAME, lock_mgr_persistant_storage).
-define(DETS_SETTINGS, [{access, read_write},{auto_save, 180000},{estimated_no_objects, 256},{file, ?DETS_FILE_NAME},
                        {min_no_slots, 256},{keypos, 1},{ram_file, false},{repair, true},{type, set}]).
% ===================================================================
% Public API
% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %lager:info("Started Lock manager at node ~p", [node()]),
    Timer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
	{ok, Ref}= dets:open_file(?DETS_FILE_NAME,?DETS_SETTINGS),
    {ok, #state{req_queue=orddict:new(),local_locks = orddict:new(),lock_requests=orddict:new(), transfer_timer=Timer, last_transfers=orddict:new(),dets_ref=Ref }}.



get_locks(Locks,TxId) ->
    gen_server:call(?MODULE, {get_locks, TxId,Locks}).

release_locks(TxId) ->
	gen_server:cast(?MODULE,{release_locks,TxId}).

remote_lock_request(DcId,Locks)->
	gen_server:cast(?MODULE,{lock_request,Locks,DcId}).

stateinfo() ->
	gen_server:call(?MODULE,{stateinfo}).

local_locks_info() ->
	gen_server:call(?MODULE,{lockinfo}).

dets_info() ->
	gen_server:call(?MODULE,{dets_info}).
% ===================================================================
% Private Functions
% ===================================================================

 
%  Data structure: local_locks - [{txid,{required,[locks()],timestamp}}|{txid,{using,[locks()]}} ]

%% Takes a list of locks, a transaction id, a timestamp, and the local_locks list as input
%% Returns the updated local_locks list
%% Adds {required,[locks()],txid,timestamp} to local_locks
%% Sends lock_request([locks()],dcid) messages to all other DCs.
required(Locks,TxId,Timestamp,Local_Locks) ->
	%remote_lock_request(MyDCId, RemoteId, Key, Locks), TODO
	New_Local_Locks=orddict:store(TxId,{required,Locks,Timestamp},Local_Locks).
    % TODO
    %Adds {required,[locks()],txid,timestamp} to local_locks
    %Sends lock_request([locks()],dcid) messages to all other DCs.



%% Takes a list of locks, a transaction id, the local_locks and the dets_ref table reference as input
%% Returns the updated lokal_locks list if all specified locks are owned by this DC and not in use by another transaction.
%% Returns {missing_locks,Missing_Locks} if at least one lock is not owned by this DC or is currently used by another transaction.
%% Updates the local_locks list if the locks were available  
using(Locks,TxId,Local_Locks,Dets_Ref) ->
	Missing_Locks = lists:foldl(fun(Lock,AccIn)-> 
			case check_lock(Lock, Dets_Ref) of
				true -> AccIn;
				false -> [Lock|AccIn]
	 		end
		end,
		[],Locks),
	case Missing_Locks of
		[] -> _New_Local_Locks = orddict:store(TxId,{using,Locks},Local_Locks);
		Missing_Locks_List -> {missing_locks,Missing_Locks_List}
	end.
    % Adds {using,[locks()],txid} to local_locks if possible

%% Takes a transaction id and the local_locks
%% Releases ownership and lock requests of all locks of the specified TxId
%% Returns the updated lokal_locks list
release_locks(TxId,Local_Locks) ->
    _New_Local_Locks=orddict:filter(fun(Key,_Value) -> Key=/=TxId end,Local_Locks).


%% Takes the local_locks and a timeout as input
%% Removes lock requests that are older than the specified timeout value form local_locks
%% Returns the updated lokal_locks list
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


%% Takes a list of locks, a DcId and a timestamp as input
%% Adds these locks with the DcId to the lock_request list
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
    %Adds {lock,dcid,timestamp} to lock_requests for every lock in [locks()]


%% Takes a lock and DcId as input
%% Removes {lock,dcid,_} from the list of locks to send
sent(Lock,DcId,Lock_Requests) ->
    case orddict:find(DcId,Lock_Requests)of
		{ok, Corresponding_Lock_Request} ->
			New_Corresponding_Lock_Request=orddict:erease(Lock,Corresponding_Lock_Request),
		    orddict:store(DcId,New_Corresponding_Lock_Request,Lock_Requests);
		error->
			Lock_Requests
	end.
    %Removes {lock,dcid,_} from lock_requests.



%% Takes the Lock_Requests list and a Timeout value as input
%% Removes all lock requests that are older than the specified timeout value from the list and returns that list
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

% Data sturcture: dets_ref : {[lock,{{send,dcid,[{to,amount}]},{received,dcid,[{from,amount}]}}]}




%% Takes the lock to send, the DC to send it to and the dets_ref table reference
%% Returns updated Dets_ref table and sends a message to the specified DC
%% Returns Dets_ref if the lock was not owned by this dc
%% still sends the information to the target DC to make up for lost messages.
%% Updates the {send,dcid,[{to,amount}]} entry of dets_ref of the specified lock
send_lock(Lock,To,Dets_ref)->
	case dets:lookup(?DETS_FILE_NAME,Lock) of
		{Lock,{{send,DCID1,Send_List},{received,DCID2,Received_List}}} ->
		    Total_Send = lists:foldl(fun({_To,Amount},Acc) -> Acc+Amount end,0,Send_List),
	        Total_Received = lists:foldl(fun({_From,Amount},Acc) -> Acc+Amount end,0,Received_List),
	        Has_Lock = Total_Send < Total_Received,
			DCID = erlang:timestamp(),
            % DCID = dc_meta_data_utilities:get_my_dc_id(), TODO - use when integrated.
			case Has_Lock of
				true ->
					case lists:keyfind(To,1,Send_List) of
						false -> 
							New_Send_List = [{To,1}|Send_List],
							% TODO actually send the lock to the other DC
							remote_send_lock(Lock, 1, DCID, To, 0), % TODO Key value ? (currently 0)
							dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}}});
							
						{To,Old_Amount} -> 
							New_Send_List = lists:keyreplace(To,1,Send_List,{To,Old_Amount+1}),
							% TODO actually send the lock to the other DC
							remote_send_lock(Lock, Old_Amount+1, DCID, To, 0), % TODO Key value ? (currently 0)
			   				dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}}})
					end;

				false -> 
					case lists:keyfind(To,1,Send_List) of
						false -> 
							New_Send_List = [{To,0}|Send_List],
							% TODO actually send the lock to the other DC (in this case the total amount of allready send locks)
							remote_send_lock(Lock, 0, DCID, To, 0), % TODO Key value ? (currently 0)
							dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,New_Send_List},{received,DCID2,Received_List}}});
							
						{To,Old_Amount} -> 
							% TODO actually send the lock to the other DC (in this case the total amount of allready send locks)
							remote_send_lock(Lock, Old_Amount, DCID, To, 0), % TODO Key value ? (currently 0)
			   				Dets_ref
					end
			end;


		{error,_Reason} ->
			DCID = erlang:timestamp(),
            % DCID = dc_meta_data_utilities:get_my_dc_id(), TODO - use when integrated.
			dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[{To,0}]},{received,DCID,[]}}})
			% sending the the total amount of locks again is not required here, since a lock was never send.
	end.



%% Takes the lock received, the DC send from, the accumulation of send locks and the dets_ref table reference
%% Amount - Total number of times another DC(From) send the lock to this DC 
%% Returns updated dets_ref table.
%% Updates the {received,dcid,[{from,amount}]} entry of dets_ref of the specified lock
received_lock(Lock,From,Amount,Dets_ref)->
	case dets:lookup(?DETS_FILE_NAME,Lock) of
        {Lock,{{send,DCID1,Send_List},{received,DCID2,Received_List}}} ->
            case lists:keyfind(From,1,Received_List) of
                false -> New_Received_List = [{From,Amount}|Received_List],
						 dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,Send_List},{received,DCID2,New_Received_List}}});
                {From,Old_Amount} ->
                    case Old_Amount < Amount of
                        true ->
                            New_Received_List = lists:keyreplace(From,1,Received_List,{From,Amount}),
                            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID1,Send_List},{received,DCID2,New_Received_List}}});
                        false -> Dets_ref
                    end
	        end;
        {error,_Reason} -> 
            DCID = erlang:timestamp(),
            % DCID = dc_meta_data_utilities:get_my_dc_id(), TODO - use when integrated.
            dets:insert(?DETS_FILE_NAME,{Lock,{{send,DCID,[]},{received,DCID,[{From,Amount}]}}})
    end.


%% Takes a Lock and the dets_ref table reference as input
%% Returns true if the lock is currently owned by this DC
%% Returns false if it is not owned by this DC
check_lock(Lock,Dets_Ref) ->
    case dets:lookup(?DETS_FILE_NAME,Lock) of
		[{Lock,{{send,DCID1,Send_List},{received,DCID2,Received_List}}}] ->
		    Total_Send = lists:foldl(fun({_To,Amount},Acc) -> Acc+Amount end,0,Send_List),
	        Total_Received = lists:foldl(fun({_From,Amount},Acc) -> Acc+Amount end,0,Received_List),
	        Has_Lock = Total_Send < Total_Received;
		[] -> false;
		{error,Reason} ->
			{error,Reason}
	end.
	
% ===================================================================
% Handling remote requests
% ===================================================================	

%% Returns an ordered list of all other DCs
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
%% Takes TODO as input
%% sends a message to all other DCs requesting the Locks.
remote_lock_request(MyDCId, Key, Locks) ->
    {LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    BinaryMsg = term_to_binary({request_permissions,
                                {remote_lock_request, {Locks, MyDCId}}, LocalPartition, MyDCId, RemoteId}),
    inter_dc_query:perform_request(?LOCK_MGR_REQUEST, {RemoteId, LocalPartition},
                                   BinaryMsg, fun lock_mgr:request_response/2).
%TODO
%% Takes TODO as input
%% sends a message to the speciefed other DC containing the lock information
remote_send_lock(Lock,Amount,MyDCId, RemoteId, Key) ->
	{LocalPartition, _} = ?LOG_UTIL:get_key_partition(Key),
    BinaryMsg = term_to_binary({request_permissions,
                                {remote_send_lock, {Lock,Amount,MyDCId, RemoteId}}, LocalPartition, MyDCId, RemoteId}),
    inter_dc_query:perform_request(?LOCK_MGR_REQUEST, {RemoteId, LocalPartition},
                                   BinaryMsg, fun lock_mgr:request_response/2).


%% Request response - do nothing. TODO  what is this function meant to do ?
request_response(_BinaryRep, _RequestCacheEntry) -> ok.
% ===================================================================
% Callbacks
% ===================================================================

%% Adds the specified locks to the lock_requests list under the specified DcId
handle_cast({lock_request,Locks,DcId}, #state{lock_requests=Lock_Requests}=State) ->
    New_Lock_Requests = requested(Locks, DcId, erlang:timestamp(), Lock_Requests),
	{noreply, State#state{lock_requests=New_Lock_Requests}};

%% Releases all locks currently owned by the specified transaction.
handle_cast({release_locks,TxId}, #state{local_locks=Local_Locks}=State) ->
    New_Local_Locks = release_locks(TxId,Local_Locks),
	{noreply, State#state{local_locks=New_Local_Locks}};


%%DEPRECATED
%% Adds the send lock information to the dets_ref table 
handle_cast({sent_lock,Lock,From,Amount}, #state{dets_ref=Dets_Ref}=State) ->
	New_Dets_Ref = received_lock(Lock,From,Amount,Dets_Ref),
	{noreply, State#state{dets_ref=New_Dets_Ref}};


%% Takes a Lock, amount(number of times this lock was send to this DC by From), the senders DCID and the DCID of this DC
%% Stores in dets_ref how often the sender send the Lock to this DC
handle_cast({remote_send_lock, {Lock,Amount,From,MyDCID1}}, #state{dets_ref=Dets_Ref}=State) ->
	MyDCID2 = dc_meta_data_utilities:get_my_dc_id(),
	case MyDCID1 == MyDCID2 of
		true -> 
			New_Dets_Ref = received_lock(Lock,From,Amount,Dets_Ref),
			{noreply, State#state{dets_ref=New_Dets_Ref}};
		false -> {noreply, State}
	end;

%% Takes a list of Locks and a Sender as input
%% Adds {dcid,[{lock,timestamp}]} to lock_requests to remember which DC requested which Locks 
%% Adds a timestamp to filter too old requests
handle_cast({remote_lock_request, {Locks, Sender}}, #state{lock_requests=Lock_Requests}=State) ->
	Timestamp = erlang:timestamp(),
    New_Lock_Requests = requested(Locks, Sender, Timestamp, Lock_Requests),
	{noreply, State#state{lock_requests=New_Lock_Requests}}.





%% For testing
handle_call({stateinfo}, _From, #state{}=State) ->
	{reply, State , State};
handle_call({lockinfo}, _From, #state{local_locks= Lock}=State) ->
	{reply, Lock, State};
handle_call({dets_info}, _From, #state{dets_ref= Dets_Ref}=State) ->
	{reply, dets:match(Dets_Ref,'_'),State};

%% Tries to aquire the specified locks for the transaction.
%% If all locks are currently owned by this DC and not used by another transaction then ok is returned.
%% If at least one lock is not owned by this DC then {error, Reason} is returned and it automatically requests the missing locks from other DCs.
handle_call({get_locks,TxId,Locks}, _From, #state{local_locks=Local_Locks,dets_ref= Dets_Ref}=State) ->
    case using(Locks, TxId, Local_Locks,Dets_Ref) of
		{missing_locks, Missing_Locks} ->
            New_Local_Locks2=required(Locks, TxId, erlang:timestamp(), Local_Locks),
			{reply, {missing_locks, Missing_Locks} , State#state{local_locks=New_Local_Locks2}};
		
		New_Lokal_Locks1 ->
			{reply, ok, State#state{local_locks=New_Lokal_Locks1}}
        
    end.









%% Periodically transfers locks requested by other DCs to them, if they are currently not used
handle_info(transfer_periodic, #state{lock_requests=Old_Lock_Requests,local_locks= Local_Locks, transfer_timer=OldTimer, dets_ref = Dets_Ref}=State) ->
    erlang:cancel_timer(OldTimer),
    Clean_Lock_Requests = clear_old_lock_requests(Old_Lock_Requests, ?LOCK_REQUEST_TIMEOUT),
	Clear_Local_Locks = remove_old_required_locks(Local_Locks,?LOCK_REQUIRED_TIMEOUT),
    % goes through all lock reuests. If the request is NOT in local_locks then the lock is send to the requesting DC
    New_Dets_Ref = orddict:fold(
	    fun(DCID,Lock_Timestamp_List,Dets_Ref_1)-> 
			orddict:fold(
			  
	        fun(Lock, _Timestamp,Dets_Ref_2) ->
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
						% TODO also remove the corresponding lock request from the lock request list.
						New_Dets_Ref = send_lock(Lock,DCID,Dets_Ref_2);
	                true -> 
						% Lock is currently used and therefore may not be send to another DC
						Dets_Ref_2
				end
            end,Dets_Ref_1,Lock_Timestamp_List) 
		end, Dets_Ref,Clean_Lock_Requests),
    NewTimer=erlang:send_after(?LOCK_TRANSFER_FREQUENCY, self(), transfer_periodic),
    {noreply, State#state{dets_ref=New_Dets_Ref, transfer_timer= NewTimer,local_locks=Clear_Local_Locks}}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




% ===================================================================
% Unit Tests
% ===================================================================
setup() ->
	start_link().
	


test1() ->
	start_link(),
	get_locks([a,b,c],1).

test2() ->
	start_link(),
	get_locks([a,b,c],1),
	release_locks(1).

test3() -> 
	start_link(),
	get_locks([a,b,c],1),
	remote_lock_request(5, [d,e,f]).





