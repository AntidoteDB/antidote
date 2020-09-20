# Antidote CRDT library
[![Build Status](https://travis-ci.org/AntidoteDB/antidote_crdt.svg?branch=master)](https://travis-ci.org/AntidoteDB/antidote_crdt)
[![Coverage Status](https://coveralls.io/repos/github/AntidoteDB/antidote_crdt/badge.svg?branch=master)](https://coveralls.io/github/AntidoteDB/antidote_crdt?branch=master)

Operation based CRDT implementations to use with Antidote.

# API

The `antidote_crdt` module provides the API below.
For a more detailed description of the different data types, the [Antidote Documentation](https://antidotedb.gitbook.io/documentation/api/datatypes) provides more information.

```erlang
% The CRDTs supported by Antidote:
-type typ() ::
      antidote_crdt_counter_pn      % PN-Counter aka Positive Negative Counter
    | antidote_crdt_counter_b       % Bounded Counter
    | antidote_crdt_counter_fat     % Fat Counter
    | antidote_crdt_flag_ew         % Enable Wins Flag aka EW-Flag
    | antidote_crdt_flag_dw         % Disable Wins Flag DW-Flag
    | antidote_crdt_set_go          % Grow Only Set aka G-Set
    | antidote_crdt_set_aw          % Add Wins Set aka AW-Set
    | antidote_crdt_set_rw          % Remove Wins Set aka RW-Set
    | antidote_crdt_register_lww    % Last Writer Wins Register aka LWW-Reg
    | antidote_crdt_register_mv     % MultiValue Register aka MV-Reg
    | antidote_crdt_map_go          % Grow Only Map aka G-Map
    | antidote_crdt_map_rr.         % Recursive Resets Map aka RR-Map

% The State of a CRDT:
-type crdt() :: term().
% The downstream effect, which has to be applied at each replica
-type effect() :: term().
% The update operation, consisting of operation name and parameters
% (e.g. {increment, 1} to increment a counter by one)
-type update() :: {atom(), term()}.
% Result of reading a CRDT (state without meta data)
-type value() :: term().
```

```erlang
% Check if the given type is supported by Antidote
-spec is_type(typ()) -> boolean().

% Returns the initial CRDT state for the given Type
-spec new(typ()) -> crdt().

% Reads the value from a CRDT state
-spec value(typ(), crdt()) -> any().

% Computes the downstream effect for a given update operation and current state.
% This has to be called once at the source replica.
% The effect must then be applied on all replicas using the update function.
% For some update operation it is not necessary to provide the current state
% and the atom 'ignore' can be passed instead (see function require_state_downstream).
-spec downstream(typ(), update(), crdt() | ignore) -> {ok, effect()} | {error, reason()}.

% Updates the state of a CRDT by applying a downstream effect calculated
% using the downstream function.
% For most types the update function must be called in causal order:
% if Eff2 was calculated on a state where Eff1 was already replied,
% then Eff1 has to be applied before Eff2 on all replicas.
-spec update(typ(), effect(), crdt()) -> {ok, crdt()}.

% Checks whether the current state is required by the downstream function
% for a specific type and update operation
-spec require_state_downstream(typ(), update()) -> boolean().

% Checks whether the given update operation is valid for the given type
-spec is_operation(typ(), update()) -> boolean().
```

# Development

Use the following `make` targets to build and test the CRDT library:


    # compile
    make compile
    # check types:
    make dialyzer
    # check code style:
    make lint


## EUnit and PropEr tests

To run the property based tests in the test directory install the [rebar3 PropEr plugin](https://www.rebar3.org/docs/using-available-plugins#proper) by adding the following line to `~/.config/rebar3/rebar.config`:

    {plugins, [{rebar3_proper, "0.9.0"}]}.

Alternatively, running the command

    sed -i -- 's/%%PROPER//g' rebar.config

in the main folder of this repository will change the rebar3 config so that tests (eunit + proper) will run. For more information check the comments in the file rebar.config.

Then execute the tests with:

    # run unit tests (eunit):
    make test
    # run proper tests:
    make proper

For more control, you can run PropEr manually and specify parameters like the tested module, or the number of generated tests:

    rebar3 proper -n 1000 -m prop_crdt_orset
