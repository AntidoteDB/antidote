REBAR = $(shell pwd)/rebar3

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean relclean
	$(REBAR) clean --all

cleantests:
	rm -f test/*.beam
	rm -rf logs/

test:
	mkdir -p logs
	${REBAR} eunit skip_deps=true
	${REBAR} cover

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer

# style checks
lint:
	${REBAR} as lint lint

check: distclean cleantests test dialyzer lint
