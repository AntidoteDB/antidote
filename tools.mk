REBAR ?= $(shell pwd)/rebar3

compile-utils: compile
	for filename in "test/utils/*.erl" ; do \
		erlc -o test/utils $$filename ; \
	done

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

singledc: compile-utils rel
	rm -f test/singledc/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/singledc/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test/singledc -cover test/antidote.coverspec
endif

multidc: compile-utils rel
	rm -f test/multidc/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/multidc/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test/multidc -cover test/antidote.coverspec
endif

systests: singledc multidc

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
