-type crdt() :: term().
-type update() :: {atom(), term()}.
-type effect() :: term().
-type value() ::  term().
-type reason() :: term().

-export_type([ crdt/0,
               update/0,
               effect/0,
               value/0
             ]).
