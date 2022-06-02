ifdef CI
	REBAR=$(shell whereis rebar3 | awk '{print $$2}')
else
	REBAR=$(shell pwd)/rebar3
endif

COVERPATH = $(shell pwd)/_build/test/cover
.PHONY: rel test relgentlerain docker-build docker-run

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean relclean
	$(REBAR) clean --all

shell: rel
	export NODE_NAME=antidote@127.0.0.1 ; \
	export COOKIE=antidote ; \
	export ROOT_DIR_PREFIX=$$NODE_NAME/ ; \
	_build/default/rel/antidote/bin/antidote console ${ARGS}

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

reltest: rel
	test/release_test.sh

# style checks
lint:
	${REBAR} lint
	${REBAR} fmt --check

check: distclean test reltest dialyzer lint

relgentlerain: export TXN_PROTOCOL=gentlerain
relgentlerain: relclean rel

relnocert: export NO_CERTIFICATION=true
relnocert: relclean rel

stage :
	$(REBAR) release -d

test:
	${REBAR} eunit

proper:
	${REBAR} proper

coverage:
	${REBAR} cover --verbose

singledc:
ifdef SUITE
	${REBAR} ct --dir apps/antidote/test/singledc --suite ${SUITE}
else
	${REBAR} ct --dir apps/antidote/test/singledc --cover_export_name=singledc
endif

multidc: 
ifdef SUITE
	${REBAR} ct --dir apps/antidote/test/multidc --suite ${SUITE}
else
	${REBAR} ct --dir apps/antidote/test/multidc --cover_export_name=multidc

endif

systests: singledc multidc

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer

docker-build:
	tmpdir=`mktemp -d` ; \
	wget "https://raw.githubusercontent.com/AntidoteDB/docker-antidote/master/local-build/Dockerfile" -O "$$tmpdir/Dockerfile" ; \
	docker build -f $$tmpdir/Dockerfile -t antidotedb:local-build .

docker-run: docker-build
	docker run -d --name antidote -p "8087:8087" antidotedb:local-build

docker-clean:
ifneq ($(docker images -q antidotedb:local-build 2> /dev/null), "")
	docker image rm -f antidotedb:local-build
endif
