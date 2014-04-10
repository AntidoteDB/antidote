-module(floppy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { floppy_vnode_master,
                  {riak_core_vnode_master, start_link, [floppy_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    LoggingMaster = { logging_vnode_master,
                  {riak_core_vnode_master, start_link, [logging_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    RepMaster = { floppy_rep_vnode_master,
                  {riak_core_vnode_master, start_link, [floppy_rep_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRepMaster = { inter_dc_repl_vnode_master,
                  {riak_core_vnode_master, start_link, [inter_dc_repl_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    
    CoordSup =  { floppy_coord_sup,
                  {floppy_coord_sup, start_link, []},
                  permanent, 5000, supervisor, [floppy_coord_sup]},

    ProxySup =  { proxy_sup,
                  {proxy_sup, start_link, []},
                  permanent, 5000, supervisor, [proxy_sup]},

    RepSup = {   floppy_rep_sup,
                  {floppy_rep_sup, start_link, []},
                  permanent, 5000, supervisor, [floppy_rep_sup]},

    { ok,
        { {one_for_one, 5, 10},
          [VMaster, LoggingMaster, RepMaster, InterDcRepMaster, CoordSup, ProxySup, RepSup]}}.
