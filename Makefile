REBAR := ./rebar3

.PHONY: rel deps test

all: deps compile

compile: deps
	@${REBAR} compile

deps:
	@${REBAR} deps

clean:
	@${REBAR} clean

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
