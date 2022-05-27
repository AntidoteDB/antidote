REBAR := rebar3

.PHONY: rel deps test

all: compile

compile:
	@${REBAR} compile

clean:
	@${REBAR} clean

distclean: clean
	$(REBAR) clean --all

test:
	mkdir -p logs
	${REBAR} eunit
	${REBAR} cover --verbose

docs:
	${REBAR} doc

xref: compile
	${REBAR} xref

dialyzer:
	${REBAR} dialyzer

# style checks
lint:
	${REBAR} as lint lint

check: distclean test dialyzer lint
