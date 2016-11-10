%% The version of messages being used by this version of antidote
%% for interdc messages (in binary), and the size of the version
%% number in bytes
-define(MESSAGE_VERSION,<<0,1>>).
-define(VERSION_BYTES,2).
-define(VERSION_BITS,16).

%% The following are binary codes defining the message
%% types for inter dc communication
-define(CHECK_UP_MSG,1).
-define(LOG_READ_MSG,2).
-define(OK_MSG,4).
-define(ERROR_MSG,5).
-define(BCOUNTER_REQUEST,6).

%% The number of bytes a parition id is in a message
-define(PARTITION_BYTE_LENGTH, 20).
%% the number of bytes a message id is
-define(REQUEST_ID_BYTE_LENGTH, 2).
-define(REQUEST_ID_BIT_LENGTH, 16).

%% Needed for dialyzer, must be the size of the request id bits plus the version bits
-define(MESSAGE_HEADER_BIT_LENGTH, 32).

-type inter_dc_message_type() :: ?CHECK_UP_MSG | ?LOG_READ_MSG | ?OK_MSG | ?ERROR_MSG | ?BCOUNTER_REQUEST.
