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
    LoggingMaster = { logging_vnode_master,
                  {riak_core_vnode_master, start_link, [logging_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    RepMaster = { floppy_rep_vnode_master,
                  {riak_core_vnode_master, start_link, [floppy_rep_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    ClockSIMaster = { clockSI_vnode_master,
                  {riak_core_vnode_master, start_link, [clockSI_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    InterDcRepMaster = { inter_dc_repl_vnode_master,
                  {riak_core_vnode_master, start_link, [inter_dc_repl_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    
    ClockSITxCoordSup =  { clockSI_tx_coord_sup,
                  {clockSI_tx_coord_sup, start_link, []},
                  permanent, 5000, supervisor, [clockSI_tx_coord_sup]},
    
    ClockSIiTxCoordSup =  { clockSI_interactive_tx_coord_sup,
                  {clockSI_interactive_tx_coord_sup, start_link, []},
                  permanent, 5000, supervisor, [clockSI_interactive_tx_coord_sup]},

    ClockSIDSGenMaster = { clockSI_downstream_generator_vnode_master,
                           {riak_core_vnode_master,  start_link, [clockSI_downstream_generator_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    VectorClockMaster = {vectoclock_vnode_master,
                         {riak_core_vnode_master,  start_link, [vectorclock_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},

    MaterializerMaster = {materializer_vnode_master,
                         {riak_core_vnode_master,  start_link, [materializer_vnode]},
                         permanent, 5000, worker, [riak_core_vnode_master]},
    
    CoordSup =  { floppy_coord_sup,
                  {floppy_coord_sup, start_link, []},
                  permanent, 5000, supervisor, [floppy_coord_sup]},

    RepSup = {   floppy_rep_sup,
                  {floppy_rep_sup, start_link, []},
                  permanent, 5000, supervisor, [floppy_rep_sup]},

    InterDcRecvr = { inter_dc_recvr, 
		     {inter_dc_recvr, start_link, []},
		     permanent, 5000, worker, [inter_dc_recvr]},

    { ok,
        { {one_for_one, 5, 10},
          [LoggingMaster, RepMaster, ClockSIMaster, ClockSITxCoordSup, ClockSIiTxCoordSup, InterDcRepMaster, CoordSup, RepSup, InterDcRecvr,
          ClockSIDSGenMaster,
          VectorClockMaster,
          MaterializerMaster]}}.
