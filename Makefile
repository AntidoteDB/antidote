REBAR := rebar3

.PHONY: rel deps test

all: deps compile

compile: deps
	@${REBAR} compile

deps:
	@${REBAR} deps

clean:
	@${REBAR} clean

test: compile
	${REBAR} eunit

docs:
	${REBAR} edoc

xref:	compile
	${REBAR} xref

dialyzer:	compile
	${REBAR} dialyzer

lint:
	${REBAR} as lint lint