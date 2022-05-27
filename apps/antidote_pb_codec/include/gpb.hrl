-ifndef(gpb_hrl).
-define(gpb_hrl, true).

-type gpb_scalar() ::
    int32
    | int64
    | uint32
    | uint64
    | sint32
    | sint64
    | fixed32
    | fixed64
    | sfixed32
    | sfixed64
    | bool
    | float
    | double
    | string
    | bytes.

% "any scalar type except floating point types and bytes"
-type gpb_map_key() ::
    int32
    | int64
    | uint32
    | uint64
    | sint32
    | sint64
    | fixed32
    | fixed64
    | sfixed32
    | sfixed64
    | bool
    | string.

%% It is not possible to have maps in maps directly,
%% so this type is any gpb_field_type() except {map,K,V}.
-type gpb_map_value() ::
    gpb_scalar()
    | {enum, atom()}
    | {msg, atom()}.

%% Erlang type  Comment
-type gpb_field_type() ::
    % integer()    variable-length encoded
    int32
    | int64
    % integer()    variable-length encoded
    | uint32
    | uint64
    % integer()    variable-length zig-zag encoded
    | sint32
    | sint64
    % integer()    always 4 | 8 bytes on wire
    | fixed32
    | fixed64
    % integer()    always 4 | 8 bytes on wire
    | sfixed32
    | sfixed64
    % true | false
    | bool
    % float()
    | float
    | double
    % string()     UTF-8 encoded
    | string
    % | binary()   iff option `strings_as_binaries'

    % binary()
    | bytes
    % atom()       the enum literal is the atom
    | {enum, atom()}
    % record()     the message name is record name
    | {msg, atom()}
    % | map()      iff option `maps'

    % record()     name is <msg name>_<field name>
    | {group, atom()}
    % | map()      iff option `maps'

    % [{K,V}] | map()
    | {map, gpb_map_key(), gpb_map_value()}.

%% An intermediary type temporarily used internally within gpb during parsing,
%% neither returned from gpb, nor accepted as input go gpb.
-type gpb_internal_intermediary_ref() ::
    {ref, term()}
    | {msg, list()}
    | {group, list()}
    | {enum, list()}.

-type gpb_internal_intermediary_map_ref() ::
    {map, gpb_map_key(), gpb_map_value() | gpb_internal_intermediary_ref()}.

%% The following two definitions (`gpb_field' and `gpb_rpc') are to
%% avoid clashes with other code, since the `field' and `rpc' are
%% really too general names, they should have been prefixed.
%%
%% Unfortunately, they are already part of the API, so they can't
%% be changed without breaking backwards compatibility.
%% (They appear as parameters or return values for functions in `gpb'
%% in generated code.)
%%
%% In case a clash, it is possible to redefine the name locally.
%% The recommendation is to redefine them with prefix, ie to `gpb_field'
%% and `gpb_rpc', since this is what they will change to in some future.
%%
-ifdef(gpb_field_record_name).
-define(gpb_field, ?gpb_field_record_name).
-else.
%% odd definition is due to backwards compatibility
-define(gpb_field, field).
-endif.

-ifdef(gpb_rpc_record_name).
-define(gpb_rpc, ?gpb_rpc_record_name).
-else.
%% odd definition is due to backwards compatibility
-define(gpb_rpc, rpc).
-endif.

% NB: record name is (currently) `field' (not `gpb_field')!
-record(?gpb_field, {
    name ::
        atom()
        % temporarily in some phases
        | undefined,
    fnum ::
        integer()
        % temporarily in some phases
        | undefined,
    % field number in the record
    rnum ::
        pos_integer()
        % temporarily, during parsing
        | undefined,
    type ::
        gpb_field_type()
        | gpb_internal_intermediary_ref()
        | gpb_internal_intermediary_map_ref()
        % temporarily in some phases
        | undefined,
    occurrence ::
        'required'
        | 'optional'
        | 'repeated'
        % temporarily in some phases
        | undefined,
    opts = [] :: [term()]
}).

-record(gpb_oneof, {
    name ::
        atom()
        % temporarily in some phases
        | undefined,
    % field number in the record
    rnum ::
        pos_integer()
        % temporarily, during parsing
        | undefined,
    % all fields have the same rnum
    fields ::
        [#?gpb_field{}]
        % temporarily in some phases
        | undefined
}).

% NB: record name is (currently) `rpc' (not `gpb_rpc')!
-record(?gpb_rpc, {
    name ::
        atom()
        % temporarily in some phases
        | undefined,
    input,
    output,
    input_stream ::
        boolean()
        % temporarily in some phases
        | undefined,
    output_stream ::
        boolean()
        % temporarily in some phases
        | undefined,
    opts ::
        [term()]
        % temporarily in some phases
        | undefined
}).

-endif.
