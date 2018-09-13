% Parameters the lock_mgr_es can be modified with.
-define(LOCK_REQUEST_TIMEOUT_ES, 500000).     % Locks requested by other DCs ( in microseconds -- 1 000 000 equals 1 second)
                                            % Defines how long a DC stores a remote lock request to send the lock to that remote DC when it has the lock and can send it
-define(LOCK_REQUIRED_TIMEOUT_ES,900000).     % Locks requested by transactions of this DC (in microseconds -- 1 000 000 equals 1 second)
                                            % Defines how long a DC stores a lock request of a transaction started on that DC to get the requested locks from other DCs
-define(LOCK_TRANSFER_FREQUENCY_ES,10).       % Sets how long lock_mgr_es waits between each time it sends requested locks to other DCs (in milliseconds -- 1 000 equals 1second)
-define(LOCK_PART_TO_DC_FACTOR,2).          % float() #Lock_Parts = round(#DCs*LOCK_PART_TO_DC_FACTOR) + LOCK_PART_TO_DC_OFFSET
                                            % Defines with how many lock parts in relation to the number of DCs a new es_lock is initiated with
-define(LOCK_PART_TO_DC_OFFSET,0).          % non_neg_integer()
                                            % Defines with how many lock parts in addition to those of "LOCK_PART_TO_DC_FACTOR" a new es_lock is intitiated with
-define(ADDITIONAL_LOCK_PARTS_SEND,1).          % Defines how many lock parts are send to a requesting dc in addition to the requested amount (To distribute them faster across the system)
                                            % Compilation Flag: at most one of (WAIT_FOR_SHARED_LOCKS1,WAIT_FOR_SHARED_LOCKS2) may be defined 
%-define(WAIT_FOR_SHARED_LOCKS1,1).          %                    1 - When acquiring an exclusive lock, the transaction will wait for data updates of all shared and exclusive lock commits
                                            %                        For shared locks it will also wait for all commits using these as exclusive locks, and commits using these as shared locks if commited on the same DC
-define(WAIT_FOR_SHARED_LOCKS2,1).          %                    2 - For every exclusive lock requested, the transaction will wait for data updates of all shared and exclusive lock commits
                                            %                        For every shared lock requested, the transaction will only wait for data updates of commits using that lock as an excluisve one
%Default if neither WAIT_FOR_SHARED_LOCKS1  %                    3 - For every exclusive lock requested, the transaction will wait for data updates of all shared and exclusive lock commits
% nor WAIT_FOR_SHARED_LOCKS2 are defined    %                        It will not wait because of any requested shared lock. Though the timestamps of their commits will still be stored and
                                            %                        used the next time that lock is used as an exlusive one.
-define(REDUCED_INTER_DC_COMMUNICATION_ES,true).% true - Reduces the communication of lock_mgr_es with each other by answering to a lock request at most once.
                                            % false - Improves performance of lock_mgr_es if bandwidth/inter_dc_communication is not the bottleneck
% Parameters the clocksi_interactive_coord - lock_mgr_es interaction can be modified with.
-define(How_LONG_TO_WAIT_FOR_LOCKS_ES,3).   % Defines how often a transaction should retry to get the locks before it is aborted
-define(GET_LOCKS_INTERVAL_ES,50).         % Defines how long a transaction waits between retrying to get the locks
-define(GET_LOCKS_FINAL_TRY_OPTION_ES,true).   % Decides if the clocksi_interactive_coord waits for GET_LOCKS_FINAL_TRY_WAIT after a lock could not be aquired.
                                            % This is usefull if GET_LOCKS_INTERVAL_ES is set very low in comparison to the time needed by DCs to communicate.
-define(GET_LOCKS_FINAL_TRY_WAIT_ES,800).    % Defines how long it waits untill it does the last attempt to get the locks.