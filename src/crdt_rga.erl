%% @doc
%%
%% An operation-based Replicated Growable Array CRDT.
%%
%% As the data structure is operation-based, to issue an operation, one should
%% first call `generate_downstream/3', to get the downstream version of the
%% operation, and then call `update/2'.
%%
%% It provides two operations: insert, which adds an element to the RGA, and delete,
%% which removes an element from the RGA.
%%
%% This implementation is based on the paper cited below.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end
-module(crdt_rga).
