% Generated file--see release:make()
% eqc_macros.hrl
-define(DELAY(X),fun()->X end).
-define(FORCE(X),(X)()).
-define(LET(X,E,E2),eqc_gen:bind(E,fun(X)->E2 end)).
-define(SIZED(S,G),eqc_gen:sized(fun(S)->G end)).
-define(SUCHTHAT(X,G,P),eqc_gen:suchthat(G,fun(X)->P end)).
-define(SUCHTHATMAYBE(X,G,P),eqc_gen:suchthatmaybe(G,fun(X)->P end)).
-define(SHRINK(G,Gs),eqc_gen:shrinkwith(G,?DELAY(Gs))).
-define(LETSHRINK(Es,Gs,E), eqc_gen:letshrink(Gs,fun(Es) -> E end)).
-define(LAZY(G),eqc_gen:lazy(?DELAY(G))).
-define(IMPLIES(Pre,Prop),eqc:implies(Pre,??Pre,?DELAY(Prop))).
-define(FORALL(X,Gen,Prop),eqc:forall(Gen,fun(X)->Prop end)).
-define(WHENFAIL(Action,Prop),eqc:whenfail(fun(_) -> Action end,?LAZY(Prop))).
-define(TRAPEXIT(E),eqc:trapexit(?DELAY(E))).
-define(TIMEOUT(Limit,Prop),eqc:timeout(Limit,?LAZY(Prop))).
-define(ALWAYS(N,P),eqc:always(N,?DELAY(P))).
-define(SOMETIMES(N,P),eqc:sometimes(N,?DELAY(P))).
% eqc_imports.hrl
-import(eqc_gen,
  [pick/1,pick/2,
	includeif/2,return/1,applygen/2,
	noshrink/1,shrinkings/1,shrinking_path/2,
        timeout/2,
	resize/2,
   	parameter/1, parameter/2, with_parameter/3, with_parameters/2,
        choose/2,
        shuffle/1,
	sample/1, sampleshrink/1,
	oneof/1, frequency/1,
	non_empty/1,
	elements/1, growingelements/1, list/1, shrink_list/1, vector/2,
	function0/1, function1/1, function2/1, function3/1, function4/1,
	bool/0, maybe/1, char/0, int/0, shrink_int/3, nat/0, largeint/0, 
        real/0, orderedlist/1,
	binary/0, binary/1, bitstring/0, bitstring/1,
        default/2, weighted_default/2,
	seal/1,open/1,peek/1,
        fault/2, fault_rate/3, more_faulty/2, less_faulty/2, no_faults/1,
        prop_shrinks_without_duplicates/1, shrink_without_duplicates/1,
        is_generator/1]).

-import(eqc_symbolic,
	[eval/1,eval/2,defined/1,well_defined/1,pretty_print/1,pretty_print/2]).

-import(eqc,[equals/2,
	     fails/1,
	     conjunction/1,
	     collect/2,collect/3,classify/3,aggregate/2,aggregate/3,measure/3,
	     %distribution/0, 
	     with_title/1, 
	     %print_distribution/1,
	     numtests/2,
	     on_output/2,
	     on_test/2,
	     quickcheck/1,
	     counterexample/0,counterexample/1,
	     current_counterexample/0,
	     module/1,
	     check/2,
	     recheck/1]).

-compile({parse_transform,eqc_warn}).
