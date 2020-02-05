%% redirects log messages to ct:log

-module(ct_redirect_handler).

%% API
-export([log/2]).

log(LogEvent, _Config) ->
    CtMaster = application:get_env(antidote, ct_master, undefined),
    #{msg := Message} = LogEvent,
    case Message of
        {Msg, Format} -> _ = rpc:call(CtMaster, ct, log, [Msg, Format]);
        _ -> _ = rpc:call(CtMaster, ct, log, ["~p", [Message]])
    end.
