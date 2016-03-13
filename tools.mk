REBAR ?= $(shell pwd)/rebar3

test:
	${REBAR} eunit skip_deps=true

all-tests: test
	mkdir -p logs
	ct_run -dir test  -pa ./_build/default/lib/*/ebin -logdir logs

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
