REBAR = $(shell pwd)/rebar3
COVERPATH = ./_build/test/cover

.PHONY: compile clean lint test proper coverage shell docs xref dialyzer typer \

all: compile

compile:
	${REBAR} compile

clean:
	${REBAR} clean

lint:
	${REBAR} as lint lint

test:
	${REBAR} eunit

proper:
	${REBAR} proper -n 1000

coverage:
	cp _build/proper+test/cover/eunit.coverdata ${COVERPATH}/proper.coverdata ;\
	${REBAR} cover --verbose

shell:
	${REBAR} shell --apps antidote_crdt

docs:
	${REBAR} doc

xref:
	${REBAR} xref

dialyzer:
	${REBAR} dialyzer

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
