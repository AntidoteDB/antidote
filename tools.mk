REBAR ?= $(shell pwd)/rebar3

compile-utils: compile
	for filename in "test/utils/*.erl" ; do \
		erlc -pa _build/default/lib/lager/ebin/ -o test/utils $$filename ; \
	done

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

systests: compile-utils rel
	rm -f test/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test -cover test/antidote.coverspec
endif

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
