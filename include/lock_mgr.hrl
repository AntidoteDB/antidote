% Parameters the lock_mgr can be modified with.
-define(LOCK_REQUEST_TIMEOUT, 300000).     % Locks requested by other DCs ( in microseconds -- 1 000 000 equals 1 second)
                                            % Defines how long a DC stores a remote lock request to send the lock to that remote DC when it has the lock and can send it
-define(LOCK_REQUIRED_TIMEOUT,400000).     % Locks requested by transactions of this DC (in microseconds -- 1 000 000 equals 1 second)
                                            % Defines how long a DC stores a lock request of a transaction started on that DC to get the requested locks from other DCs
-define(LOCK_TRANSFER_FREQUENCY,10).       % Sets how long lock_mgr waits between each time it sends requested locks to other DCs (in milliseconds -- 1 000 equals 1second)
-define(REDUCED_INTER_DC_COMMUNICATION,true).% true - Reduces the communication of lock_mgr_es with each other by answering to a lock request at most once.
                                            % false - Improves performance of lock_mgr_es if bandwidth/inter_dc_communication is not the bottleneck

% Parameters the clocksi_interactive_coord - lock_mgr_es interaction can be modified with.
-define(How_LONG_TO_WAIT_FOR_LOCKS,3).   % Defines how often a transaction should retry to get the locks before it is aborted
-define(GET_LOCKS_INTERVAL,50).         % Defines how long a transaction waits between retrying to get the locks
-define(GET_LOCKS_FINAL_TRY_OPTION,true).    % Decides if the clocksi_interactive_coord waits for GET_LOCKS_FINAL_TRY_WAIT after a lock could not be aquired.
                                            % This is usefull if GET_LOCKS_INTERVAL_ES is set very low in comparison to the time needed by DCs to communicate.
-define(GET_LOCKS_FINAL_TRY_WAIT,300).       % Defines how long it waits untill it does the last attempt to get the locks.