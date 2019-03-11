REBAR = rebar3

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean
	$(REBAR) clean --all

cleantests:
	rm -f test/*.beam
	rm -rf logs/

test:
	mkdir -p logs
	${REBAR} eunit
	${REBAR} cover

docs:
	${REBAR} doc

xref: compile
	${REBAR} xref

dialyzer:
	${REBAR} dialyzer

# style checks
lint:
	${REBAR} as lint lint

check: distclean cleantests test dialyzer lint
