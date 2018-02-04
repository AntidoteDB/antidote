REBAR := ./rebar3

.PHONY: rel deps test

all: deps compile

compile: deps
	@${REBAR} compile

deps:
	@${REBAR} deps

clean:
	@${REBAR} clean

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
