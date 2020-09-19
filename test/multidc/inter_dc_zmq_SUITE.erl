%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(inter_dc_zmq_SUITE).

%%%% common_test callbacks
-export([
%%    init_per_suite/1,
%%    end_per_suite/1,
%%    init_per_testcase/2,
%%    end_per_testcase/2,
    all/0
]).
%%
%%%% tests
%%-export([
%%    pub_alone/1,
%%    pub_two_sub/1,
%%    pub_one_sub_two_topics/1,
%%    pub_sub_stop/1,
%%    pub_one_sub_two_topics_partitions/1,
%%
%%    router_multiple_dealer/1,
%%    router_dealer_use/1,
%%
%%    how_to_close_chumak_sockets/1
%%]).
%%
%%-include_lib("common_test/include/ct.hrl").
%%-include_lib("eunit/include/eunit.hrl").
%%
%%
%%init_per_suite(InitialConfig) ->
%%    ok = application:ensure_started(chumak),
%%    InitialConfig.
%%
%%end_per_suite(Config) ->
%%    application:stop(chumak),
%%    Config.
%%
%%init_per_testcase(Case, Config) ->
%%    ct:print("[ RUN ] ~p", [Case]),
%%    Config.
%%
%%end_per_testcase(Name, _) ->
%%    ct:print("[ OK ] ~p", [Name]),
%%    ok.
%%
all() -> [
%%%%    how_to_close_chumak_sockets,
%%%%
%%%%    pub_alone,
%%%%    pub_two_sub,
%%%%    pub_one_sub_two_topics,
%%%%    pub_sub_stop,
%%%%    pub_one_sub_two_topics_partitions,
%%%%
%%%%    router_multiple_dealer,
%%%%    router_dealer_use
].
%%
%%how_to_close_chumak_sockets(_) ->
%%    %% this test case produces spam if running with other SUITEs at the same time
%%%%    DontDie = spawn_link(
%%%%        fun() ->
%%%%            process_flag(trap_exit, true), % socket will crash on test case exit, trap exit only for clean console
%%%%            application:set_env(antidote, pubsub_port, 1555),
%%%%            {ok, 15555} = application:get_env(antidote, pubsub_port),
%%%%            {ok, Pid} = inter_dc_pub:start_link(),
%%%%
%%%%            %% publish without interruption
%%%%            spawn_link(fun L() ->
%%%%                %% try catch only for test case
%%%%                %% it does not really matter if the publish call crashes the caller,
%%%%                %% can only happen when the application shuts down
%%%%                try
%%%%                    gen_server:call(Pid, {publish, <<"hello">>}),
%%%%                    timer:sleep(50),
%%%%                    L()
%%%%                catch
%%%%                    _:_:_ -> ok
%%%%                end
%%%%                       end),
%%%%            receive
%%%%                finish_test -> gen_server:stop(Pid)
%%%%            end
%%%%        end
%%%%    ),
%%%%
%%%%    true = is_process_alive(DontDie),
%%%%    %% simple socket and close
%%%%    spawn_link(fun() ->
%%%%        {ok, SocketPid} = chumak:socket(sub),
%%%%        inter_dc_utils:close_socket(SocketPid),
%%%%        false = is_process_alive(SocketPid)
%%%%               end),
%%%%
%%%%    %% simple socket connect and close
%%%%    spawn_link(fun() ->
%%%%        {ok, SocketPid} = chumak:socket(sub),
%%%%        {ok, _} = chumak:connect(SocketPid, tcp, "localhost", 15555),
%%%%        inter_dc_utils:close_socket(SocketPid),
%%%%        false = is_process_alive(SocketPid)
%%%%               end),
%%%%
%%%%
%%%%
%%%%    true = is_process_alive(DontDie),
%%%%    DontDie ! finish_test,
%%%%    timer:sleep(100),
%%%%    false = is_process_alive(DontDie),
%%    ok.
%%
%%pub_alone(_Config) ->
%%    application:set_env(antidote, pubsub_port, 15554),
%%    {ok, 15554} = application:get_env(antidote, pubsub_port),
%%    {ok, Pid} = inter_dc_pub:start_link(),
%%
%%    Topic = " ",
%%    Message = <<" ", "ping">>,
%%    Self = self(),
%%
%%    % start local subscriber
%%    spawn_link(fun() ->
%%        {ok, SubSocket} = chumak:socket(sub),
%%        chumak:subscribe(SubSocket, Topic),
%%        {ok, _SocketPid} = chumak:connect(SubSocket, tcp, "localhost", 15554),
%%        %% wait until socket connected properly
%%        timer:sleep(100),
%%        Self ! step,
%%        {ok, Message} = chumak:recv(SubSocket),
%%        ok = inter_dc_utils:close_socket(SubSocket),
%%        Self ! finish
%%               end),
%%
%%    receive step -> ok end,
%%
%%    % internal API, broadcast allows only for transactions
%%    ok = gen_server:call(Pid, {publish, Message}),
%%
%%    receive finish -> ok after 300 -> throw(subscriber_timeout) end,
%%    gen_server:stop(Pid),
%%    ok.
%%
%%pub_two_sub(_Config) ->
%%    application:set_env(antidote, pubsub_port, 15553),
%%    {ok, 15553} = application:get_env(antidote, pubsub_port),
%%    {ok, Pid} = inter_dc_pub:start_link(),
%%
%%    Message = <<"01", "ping">>,
%%    Message2 = <<"02", "ping">>,
%%    Self = self(),
%%
%%    % start local subscriber
%%    spawn_link(fun() ->
%%        {ok, SubSocket} = chumak:socket(sub),
%%        chumak:subscribe(SubSocket, "01"),
%%        {ok, _SocketPid} = chumak:connect(SubSocket, tcp, "localhost", 15553),
%%        {ok, <<"01", "ping">>} = chumak:recv(SubSocket),
%%        ok = inter_dc_utils:close_socket(SubSocket),
%%        Self ! finish
%%               end),
%%
%%    % start local subscriber 2
%%    spawn_link(fun() ->
%%        {ok, SubSocket} = chumak:socket(sub),
%%        chumak:subscribe(SubSocket, "02"),
%%        {ok, _SocketPid} = chumak:connect(SubSocket, tcp, "localhost", 15553),
%%        {ok, <<"02", "ping">>} = chumak:recv(SubSocket),
%%        ok = inter_dc_utils:close_socket(SubSocket),
%%        Self ! finish
%%               end),
%%
%%    timer:sleep(50),
%%
%%    % internal API, broadcast allows only for transactions
%%    ok = gen_server:call(Pid, {publish, Message}),
%%    ok = gen_server:call(Pid, {publish, Message2}),
%%
%%    receive finish -> ok after 100 -> throw(subscriber_timeout) end,
%%    gen_server:stop(Pid),
%%    ok.
%%
%%pub_one_sub_two_topics(_Config) ->
%%    application:set_env(antidote, pubsub_port, 15552),
%%    {ok, 15552} = application:get_env(antidote, pubsub_port),
%%    {ok, Pid} = inter_dc_pub:start_link(),
%%
%%    Message = <<"01", "msg">>,
%%    Message2 = <<"02", "msg">>,
%%    Message3 = <<"03", "msg">>,
%%    Self = self(),
%%
%%    % start local subscriber
%%    spawn_link(fun() ->
%%        {ok, SubSocket} = chumak:socket(sub),
%%        chumak:subscribe(SubSocket, "01"),
%%        chumak:subscribe(SubSocket, "02"),
%%        {ok, _SocketPid} = chumak:connect(SubSocket, tcp, "localhost", 15552),
%%        {ok, _} = chumak:recv(SubSocket),
%%        {ok, _} = chumak:recv(SubSocket),
%%
%%        ok = inter_dc_utils:close_socket(SubSocket),
%%        Self ! finish
%%               end),
%%
%%    timer:sleep(50),
%%
%%    % internal API, broadcast allows only for transactions
%%    ok = gen_server:call(Pid, {publish, Message}),
%%    ok = gen_server:call(Pid, {publish, Message2}),
%%    ok = gen_server:call(Pid, {publish, Message3}),
%%
%%    receive finish -> ok after 100 -> throw(subscriber_timeout) end,
%%    gen_server:stop(Pid),
%%    ok.
%%
%%pub_sub_stop(_Config) ->
%%    application:set_env(antidote, pubsub_port, 15555),
%%    {ok, 15555} = application:get_env(antidote, pubsub_port),
%%    {ok, Pid} = inter_dc_pub:start_link(),
%%
%%    Message = <<"01", "ping">>,
%%    Self = self(),
%%
%%    % start local subscriber
%%    _Loop = spawn(fun() ->
%%        {ok, SubSocket} = chumak:socket(sub),
%%        chumak:subscribe(SubSocket, "01"),
%%        {ok, SubPid} = chumak:connect(SubSocket, tcp, "localhost", 15555),
%%        Self ! {sock, SubSocket, SubPid},
%%        {ok, M} = chumak:recv(SubSocket),
%%        ct:log("Received but should not receive something! ~p", [M]),
%%        0 = M
%%                  end),
%%
%%    {Subscriber, _SubscriberPid} = receive {sock, Sub, SubPid} -> {Sub, SubPid} end,
%%
%%    ct:log("Stopping subscriber"),
%%
%%    inter_dc_utils:close_socket(Subscriber),
%%
%%    timer:sleep(50),
%%
%%    % internal API, broadcast allows only for transactions
%%    ok = gen_server:call(Pid, {publish, Message}),
%%
%%    gen_server:stop(Pid),
%%    timer:sleep(50),
%%    ok.
%%
%%pub_one_sub_two_topics_partitions(_Config) ->
%%    application:set_env(antidote, pubsub_port, 15551),
%%    {ok, 15551} = application:get_env(antidote, pubsub_port),
%%
%%    %% start the antidote publisher module
%%    {ok, Pid} = inter_dc_pub:start_link(),
%%
%%
%%    %% four topics corresponding to four partitions
%%    P1 = 0,
%%    P2 = 365375409332725729550921208179070754913983135744,
%%    P3 = 730750818665451459101842416358141509827966271488,
%%    P4 = 1096126227998177188652763624537212264741949407232,
%%
%%    %% Binary representation of the topics, format required by publisher
%%    P1Bin = inter_dc_txn:partition_to_bin(P1),
%%    P2Bin = inter_dc_txn:partition_to_bin(P2),
%%    P3Bin = inter_dc_txn:partition_to_bin(P3),
%%    P4Bin = inter_dc_txn:partition_to_bin(P4),
%%
%%    %% payload with topic prefix
%%    Msg = erlang:term_to_binary(helo),
%%    M0 = <<P1Bin/binary, Msg/binary>>,
%%    M1 = <<P1Bin/binary, Msg/binary>>,
%%    M2 = <<P2Bin/binary, Msg/binary>>,
%%    M3 = <<P3Bin/binary, Msg/binary>>,
%%    M4 = <<P4Bin/binary, Msg/binary>>,
%%
%%    Self = self(),
%%
%%    % start local subscriber
%%    spawn(fun() ->
%%
%%        %% create two sockets
%%        {ok, SubSocket} = chumak:socket(sub),
%%        {ok, SubSocket2} = chumak:socket(sub),
%%
%%
%%        %% test message with first socket only
%%        chumak:subscribe(SubSocket, <<>>),
%%        {ok, _SocketPid} = chumak:connect(SubSocket, tcp, "localhost", 15551),
%%        {ok, MM1} = chumak:recv(SubSocket),
%%        ct:log("Received test message ~p", [MM1]),
%%
%%        %% unsubscribe partition
%%        chumak:cancel(SubSocket, <<>>),
%%
%%        %% subscribe to different partitions with Sock 1 & 2
%%        ct:log("Subscribing to ~p", [P1Bin]),
%%        chumak:subscribe(SubSocket2, P1Bin),
%%
%%        ct:log("Subscribing to ~p", [P3Bin]),
%%        chumak:subscribe(SubSocket2, P3Bin),
%%
%%        %% connect socket 2
%%        {ok, _} = chumak:connect(SubSocket2, tcp, "localhost", 15551),
%%        timer:sleep(50),
%%        Self ! more,
%%
%%        %% receive two messages with socket 2
%%        {ok, _MMM1} = chumak:recv(SubSocket2),
%%        {ok, _MMM2} = chumak:recv(SubSocket2),
%%
%%        %% subscriber_timeout when uncomment
%%%%        {ok, _} = chumak:recv(SubSocket2),
%%%%        logger:warning("2"),
%%
%%        ok = inter_dc_utils:close_socket(SubSocket),
%%        Self ! finish
%%          end),
%%
%%    timer:sleep(50),
%%
%%    % internal API, broadcast allows only for transactions
%%    ok = gen_server:call(Pid, {publish, M0}),
%%
%%    receive more -> ok after 100 -> throw(subscriber_timeout) end,
%%
%%    [ ok = gen_server:call(Pid, {publish, M}) || M <- [M1, M2, M3, M4]],
%%
%%    receive finish -> ok after 200 -> throw(subscriber_timeout) end,
%%    gen_server:stop(Pid),
%%    ok.
%%
%%router_multiple_dealer(_Config) ->
%%    Self = self(),
%%    % start local router
%%    _RouterPid = spawn(fun() ->
%%        {ok, RouterSocket} = chumak:socket(router),
%%        {ok, _BindPid} = chumak:bind(RouterSocket, tcp, "0.0.0.0", 15556),
%%        %% give socket some time to bind
%%        timer:sleep(20),
%%
%%        CaseCheck = fun() ->
%%            {ok, [Id, <<"ping">>]} = chumak:recv_multipart(RouterSocket),
%%            chumak:send_multipart(RouterSocket, [Id, <<"pong">>])
%%                    end,
%%
%%        Self ! step,
%%        CaseCheck(),
%%        CaseCheck(),
%%        inter_dc_utils:close_socket(RouterSocket),
%%        false = is_process_alive(RouterSocket),
%%        Self ! finish_test
%%        end),
%%
%%    receive step -> ok end,
%%
%%    WorkerLoop = fun(Socket, Id, _Parent) ->
%%        spawn_link(fun() ->
%%            chumak:send_multipart(Socket, [<<"ping">>]),
%%            {ok, [<<"pong">>]} = chumak:recv_multipart(Socket),
%%            ct:log("~p Message received from router", [Id])
%%                   end
%%        )
%%                 end,
%%
%%    StartWorker = fun () ->
%%        Parent = self(),
%%        spawn_link(
%%            fun() ->
%%                Id = atom_to_list(node()) ++ pid_to_list(self()),
%%                {ok, Socket} = chumak:socket(dealer, Id),
%%                {ok, _PeerPid} = chumak:connect(Socket, tcp, "localhost", 15556),
%%                WorkerLoop(Socket, Id, Parent)
%%            end
%%        ) end,
%%
%%
%%    StartWorker(),
%%    StartWorker(),
%%    receive finish_test -> ok end,
%%    ok.
%%
%%router_dealer_use(_Config) ->
%%    Self = self(),
%%    % start local router
%%    RouterPid = spawn_link(fun() ->
%%        process_flag(trap_exit, true), % socket will crash on test case exit, trap exit only for clean console
%%        {ok, RouterSocket} = chumak:socket(router),
%%        {ok, _BindPid} = chumak:bind(RouterSocket, tcp, "0.0.0.0", 15557),
%%
%%        ct:log("Spawned router, waiting"),
%%        CaseCheck = fun L(Counter) ->
%%            {ok, [Id, Msg]} = chumak:recv_multipart(RouterSocket),
%%            ct:log("Got Message from ~p", [Id]),
%%            BinInt = integer_to_binary(Counter),
%%            chumak:send_multipart(RouterSocket, [Id, <<BinInt/binary, Msg/binary>>]),
%%            L(Counter + 1)
%%                    end,
%%        spawn_link(fun() -> CaseCheck(0) end),
%%        Self ! step,
%%        receive finish_test -> ok end,
%%        inter_dc_utils:close_socket(RouterSocket)
%%          end),
%%
%%    receive step -> ok end,
%%
%%    WorkerLoop = fun(Socket, Parent) ->
%%        spawn_link(fun() ->
%%            ct:log("Sending ping"),
%%            chumak:send_multipart(Socket, [list_to_binary("hello")]),
%%            {ok, [<<"0hello">>]} = chumak:recv_multipart(Socket),
%%
%%            ct:log("Received"),
%%
%%            %% two sends in succession is ok
%%            chumak:send_multipart(Socket, [list_to_binary("hello")]),
%%            chumak:send_multipart(Socket, [list_to_binary("world")]),
%%
%%            %% two receive in succession is ok
%%            {ok, [<<"1hello">>]} = chumak:recv_multipart(Socket),
%%            {ok, [<<"2world">>]} = chumak:recv_multipart(Socket),
%%            ct:log("Received"),
%%
%%            Parent ! finish
%%                   end
%%        )
%%                 end,
%%
%%    %% request identity is strictly required by chumak
%%    {ok, Socket} = chumak:socket(dealer, "A"),
%%    {ok, _PeerPid} = chumak:connect(Socket, tcp, "localhost", 15557),
%%    WorkerLoop(Socket, self()),
%%
%%    receive finish -> ok end,
%%    RouterPid ! finish_test,
%%    inter_dc_utils:close_socket(Socket),
%%    ok.
