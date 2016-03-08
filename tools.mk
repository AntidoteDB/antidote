REBAR ?= $(shell pwd)/rebar3

test:
	${REBAR} eunit skip_deps=true

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
