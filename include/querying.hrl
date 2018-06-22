%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc An Antidote library to support some definitions that concern
%%%      the querying structure of the database.
%%%
%%% @end
%%%-------------------------------------------------------------------

%% Table metadata definitions
-define(TABLE(Name, Policy, Cols, FKeys, Indexes), {Name, Policy, Cols, FKeys, Indexes}).
-define(ATTRIBUTE(Column, Type, Value), {{Column, Type}, Value}).
-define(FK(FkName, FkType, RefTableName, RefColName, DeleteRule), {{FkName, FkType}, {RefTableName, RefColName}, DeleteRule}).
-define(TABLE_DT, antidote_crdt_map_go).
-define(TABLE_NAME_DT, antidote_crdt_register_lww).
-define(COLUMNS, {names}).
-define(PK_COLUMN, {pk}).
-define(TABLE_METADATA_KEY, '#tables').
-define(TABLE_METADATA_DT, antidote_crdt_map_go).
-define(AQL_METADATA_BUCKET, aql_metadata).
-define(SHADOW_COL_DT, antidote_crdt_map_go).
-define(SHADOW_COL_ENTRY_DT, antidote_crdt_register_mv).
-define(STATE_COL, '#st').
-define(STATE_COL_DT, antidote_crdt_register_mv).

%% Data types definitions
-define(AQL_INTEGER, integer).
-define(CRDT_INTEGER, antidote_crdt_register_lww).

-define(AQL_VARCHAR, varchar).
-define(CRDT_VARCHAR, antidote_crdt_register_lww).

-define(AQL_BOOLEAN, boolean).
-define(CRDT_BOOLEAN, antidote_crdt_flag_ew).

-define(AQL_COUNTER_INT, counter_int).
-define(CRDT_BCOUNTER_INT, antidote_crdt_counter_b).
-define(CRDT_COUNTER_INT, antidote_crdt_counter_pn).

%% Indexes definitions
-define(INDEX(IndexName, TableName, Attributes), {IndexName, TableName, Attributes}).
-define(is_index(Index), is_tuple(Index) andalso tuple_size(Index) =:= 3).
-define(PINDEX_PREFIX, "#_").
-define(SINDEX_PREFIX, "#2i_").
-define(PINDEX_DT, antidote_crdt_set_go).
-define(SINDEX_DT, antidote_crdt_index_go).
-define(is_index_upd(Update), is_tuple(Update) andalso tuple_size(Update) =:= 3).
-define(RECORD_UPD_TYPE, record).
-define(TABLE_UPD_TYPE, table).
-define(METADATA_UPD_TYPE, metadata).
-define(OTHER_UPD_TYPE, other).

%% Query definitions
-define(CONDITION(Column, Comparator, Value), {Column, Comparator, Value}).
-define(WILDCARD, {wildcard, ignore}).
-define(OBJECT_UPDATE(Key, Type, Bucket, Op, Param), {{Key, Type, Bucket}, Op, Param}).

%% Types
-type filter() :: [filter_content()].

-type filter_content() :: table_filter() | projection_filter() | conditions_filter().

-type table_name() :: atom() | list().
-type table_filter() :: {tables, [table_name()]}.

-type column_name() :: atom() | list().
-type projection_filter() :: {projection, [column_name()]}.

-type comparison() :: atom().
-type value() :: term().
-type condition() :: {column_name(), comparison(), value()}.

-type conditions_filter() :: {conditions, [condition()]}.

%% Export
-export_type([filter/0]).
