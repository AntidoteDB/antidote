-module(floppy_pb_set).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/floppy_pb.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #fpbsetupdatereq{} ->
            {ok, Msg, {"floppy.updt", <<>>}};
        #fpbgetsetreq{} ->
            {ok, Msg, {"floppy.get", <<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbsetupdatereq{key=Key, adds=AddsBin, rems=RemsBin}, State) ->
    lists:foreach(fun(X) ->
                          Elem = erlang:binary_to_term(X),
                          floppy:append(Key, riak_dt_orset, {{add, Elem}, node()})
                  end,AddsBin),
    lists:foreach(fun(X) ->
                          Elem = erlang:binary_to_term(X),
                          floppy:append(Key, riak_dt_orset, {{remove, Elem}, node()})
                  end,RemsBin),
    {reply, #fpboperationresp{success = true}, State};

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbgetsetreq{key=Key}, State) ->
    {ok, Result} = floppy:read(Key, riak_dt_orset),
    {reply, #fpbgetsetresp{value = erlang:term_to_binary(Result)}, State}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.
