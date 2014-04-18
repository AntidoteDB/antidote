-module(floppy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case floppy_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, floppy_vnode}]),
            ok = riak_core_node_watcher:service_up(floppy, self()),

            ok = riak_core:register([{vnode_module, logging_vnode}]),
            ok = riak_core_node_watcher:service_up(logging, self()),

            ok = riak_core:register([{vnode_module, clocksi_vnode}]),
            ok = riak_core_node_watcher:service_up(clocksi, self()),

	    ok = riak_core:register([{vnode_module, floppy_rep_vnode}]),
            ok = riak_core_node_watcher:service_up(replication, self()),

	    ok = riak_core:register([{vnode_module, inter_dc_repl_vnode}]),
            ok = riak_core_node_watcher:service_up(interdcreplication, self()),

            ok = riak_core_ring_events:add_guarded_handler(floppy_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(floppy_node_event_handler, []),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
