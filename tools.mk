REBAR ?= $(shell pwd)/rebar3

test: compile
	${REBAR} eunit

docs:
	${REBAR} doc

xref:	compile
	${REBAR} xref

dialyzer:	compile
	${REBAR} dialyzer
