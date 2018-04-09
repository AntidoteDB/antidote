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
-define(FK(FkName, FkType, RefTableName, RefColName), {{FkName, FkType}, {RefTableName, RefColName}}).
-define(TABLE_DT, antidote_crdt_gmap).
-define(TABLE_NAME_DT, antidote_crdt_lwwreg).
-define(COLUMNS, {names}).
-define(PK_COLUMN, {pk}).
-define(TABLE_METADATA_KEY, '#tables').
-define(TABLE_METADATA_DT, antidote_crdt_gmap).
-define(AQL_METADATA_BUCKET, aql_metadata).
-define(SHADOW_COL_DT, antidote_crdt_gmap).
-define(SHADOW_COL_ENTRY_DT, antidote_crdt_mvreg).
-define(STATE_COL, '#st').
-define(STATE_COL_DT, antidote_crdt_mvreg).

%% Data types definitions
-define(AQL_INTEGER, integer).
-define(CRDT_INTEGER, antidote_crdt_integer).

-define(AQL_VARCHAR, varchar).
-define(CRDT_VARCHAR, antidote_crdt_lwwreg).

-define(AQL_BOOLEAN, boolean).
-define(CRDT_BOOLEAN, antidote_crdt_flag_ew).

-define(AQL_COUNTER_INT, counter_int).
-define(CRDT_BCOUNTER_INT, antidote_crdt_bcounter).
-define(CRDT_COUNTER_INT, antidote_crdt_counter).

%% Indexes definitions
-define(INDEX(IndexName, TableName, Attributes), {IndexName, TableName, Attributes}).
-define(is_index(Index), is_tuple(Index) andalso tuple_size(Index) =:= 3).
-define(PINDEX_PREFIX, "#_").
-define(SINDEX_PREFIX, "#2i_").
-define(PINDEX_DT, antidote_crdt_gset).
-define(SINDEX_DT, antidote_crdt_orset).
-define(INDEX_UPDATE(TableName, IndexName, EntryKey, EntryValue), {TableName, IndexName, {EntryKey, EntryValue}}).
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
