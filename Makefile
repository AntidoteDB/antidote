REBAR = $(shell pwd)/rebar3

.PHONY: test

all: compile

compile:
	${REBAR} compile

clean:
	${REBAR} clean

##
## Test targets
##

check: test xref dialyzer lint

test: eunit proper

lint:
	${REBAR} as lint lint

eunit:
	${REBAR} eunit

proper:
	${REBAR} proper

shell:
	${REBAR} shell --apps antidote_crdt

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
