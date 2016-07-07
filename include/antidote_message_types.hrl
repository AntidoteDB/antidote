%% The version of messages being used by this version of antidote
%% for interdc messages (in binary), and the size of the version
%% number in bytes 
-define(MESSAGE_VERSION,<<0,1>>).
-define(VERSION_BYTES,2).

%% The following are binary codes defining the message
%% types for inter dc communication
-define(CHECK_UP_MSG,1).
-define(LOG_READ_MSG,2).
-define(LOG_RESP_MSG,3).
-define(OK_MSG,4).
-define(ERROR_MSG,5).
