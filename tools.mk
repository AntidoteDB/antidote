REBAR ?= $(shell pwd)/rebar3

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true

systests: rel
	rm -f test/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin -logdir logs -suite test/${SUITE}
else
	ct_run -pa ./_build/default/lib/*/ebin -logdir logs -dir test
endif

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
