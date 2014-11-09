%% -*- coding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% crdt_pncounter: A convergent, replicated, state based PN counter
%%
%%
%% -------------------------------------------------------------------

%% @doc
%% A PN-Counter CRDT. A PN-Counter is essentially two G-Counters: one for increments and
%% one for decrements. The value of the counter is the difference between the value of the
%% Positive G-Counter and the value of the Negative G-Counter.
%%
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. [http://hal.upmc.fr/inria-00555588/]
%%
%% @end

-module(crdt_pncounter).

-export([new/0, new/1, value/1, value/2, update/2]).
-export([parent_clock/2, update/3, equal/2]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([pncounter/0, pncounter_op/0]).

-opaque pncounter()  :: {Inc::non_neg_integer(), Dec::non_neg_integer()}.
-type pncounter_op() :: riak_dt_gcounter:gcounter_op() | decrement_op().
-type decrement_op() :: decrement | {decrement, pos_integer()}.
-type pncounter_q()  :: positive | negative.

%% @doc Create a new, empty `pncounter()'
-spec new() -> pncounter().
new() ->
    {0,0}.

%% @doc Create a `pncounter()' with an initial `Value' for `Actor'.
-spec new(integer()) -> pncounter().
new(Value) when Value > 0 ->
    update({increment, Value}, new());
new(Value) when Value < 0 ->
    update({decrement, Value * -1}, new());
new(_Zero) ->
    new().

%% @doc no-op
-spec parent_clock(riak_dt_vclock:vclock(), pncounter()) ->
                          pncounter().
parent_clock(_Clock, Cntr) ->
    Cntr.

%% @doc The single, total value of a `pncounter()'
-spec value(pncounter()) -> integer().
value(PNCnt) ->
    {Inc, Dec} = PNCnt,
    Inc-Dec.

%% @doc query the parts of a `pncounter()'
%% valid queries are `positive' or `negative'.
-spec value(pncounter_q(), pncounter()) -> integer().
value(positive, PNCnt) ->
    {Inc, _Dec} = PNCnt,
    Inc;
value(negative, PNCnt) ->
    {_Inc, Dec} = PNCnt,
    Dec.

%% @doc Update a `pncounter()'. The first argument is either the atom
%% `increment' or `decrement' or the two tuples `{increment, pos_integer()}' or
%% `{decrement, pos_integer()}'. In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% `Actor' is any term, and the 3rd argument is the `pncounter()' to update.
%%
%% returns the updated `pncounter()'
-spec update(pncounter_op(), pncounter()) -> {ok, pncounter()}.
update(increment, PNCnt) ->
    update({increment, 1}, PNCnt);
update(decrement, PNCnt) ->
    update({decrement, 1}, PNCnt);
update({_IncrDecr, 0}, PNCnt) ->
    {ok, PNCnt};
update({increment, By}, PNCnt) when is_integer(By), By > 0 ->
    {ok, increment_by(By, PNCnt)};
update({increment, By}, PNCnt) when is_integer(By), By < 0 ->
    update({decrement, -By}, PNCnt);
update({decrement, By}, PNCnt) when is_integer(By), By > 0 ->
    {ok, decrement_by(By, PNCnt)}.

update(Op, Cntr, _Ctx) ->
    update(Op, Cntr).

-spec equal(pncounter(), pncounter()) -> boolean().
equal({Inc1, Dec1}, {Inc2, Dec2}) ->
    case Inc1 of
        Inc2 ->
            case Dec1 of
                Dec2 ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.


-include("../deps/riak_dt/include/riak_dt_tags.hrl").
-define(TAG, ?DT_PNCOUNTER_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

% Priv
-spec increment_by(pos_integer(), pncounter()) -> pncounter().
increment_by(Increment, PNCnt) ->
    {Inc, Dec} = PNCnt,
    {Inc+Increment, Dec}.

-spec decrement_by(pos_integer(), pncounter()) -> pncounter().
decrement_by(Decrement, PNCnt) ->
    {Inc, Dec} = PNCnt,
    {Inc, Dec+Decrement}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({0,0}, new()).

value_test() ->
    PNCnt1 = {4,0}, 
    PNCnt2 = {8,4},
    PNCnt3 = {4,4},
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(4, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

update_increment_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 1}, PNCnt0),
    {ok, PNCnt2} = update({increment, 2}, PNCnt1),
    {ok, PNCnt3} = update({increment, 1}, PNCnt2),
    ?assertEqual({4,0}, PNCnt3).

update_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, PNCnt0),
    ?assertEqual({7,0}, PNCnt1).

update_decrement_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 1}, PNCnt0),
    {ok, PNCnt2} = update({increment, 2}, PNCnt1),
    {ok, PNCnt3} = update({increment, 1}, PNCnt2),
    {ok, PNCnt4} = update({decrement, 1}, PNCnt3),
    ?assertEqual({4,1}, PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, PNCnt0),
    {ok, PNCnt2} = update({decrement, 5}, PNCnt1),
    ?assertEqual({7, 5}, PNCnt2).

equal_test() ->
    PNCnt1 = {4,2},
    PNCnt2 = {2,0},
    PNCnt3 = {2,0}, 
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt2, PNCnt3)).

-endif.
