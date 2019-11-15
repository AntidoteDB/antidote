REBAR = $(shell pwd)/rebar3
COVERPATH = $(shell pwd)/_build/test/cover
.PHONY: rel test relgentlerain docker-build docker-run

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean relclean
	$(REBAR) clean --all

cleantests:
	rm -f test/utils/*.beam
	rm -f test/singledc/*.beam
	rm -f test/multidc/*.beam
	rm -rf logs/

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
	${REBAR} as lint lint

check: distclean cleantests test reltest dialyzer lint

relgentlerain: export TXN_PROTOCOL=gentlerain
relgentlerain: relclean cleantests rel

relnocert: export NO_CERTIFICATION=true
relnocert: relclean cleantests rel

stage :
	$(REBAR) release -d

compile-utils: compile
	for filename in "test/utils/*.erl" ; do \
		erlc -o test/utils $$filename ; \
	done

test:
	${REBAR} eunit skip_deps=true

coverage:
	# copy the coverdata files with a wildcard filter
	# won't work if there are multiple folders (multiple systests)
	cp logs/*/*singledc*/../all.coverdata ${COVERPATH}/singledc.coverdata ; \
	cp logs/*/*multidc*/../all.coverdata ${COVERPATH}/multidc.coverdata ; \
	${REBAR} cover --verbose

singledc: compile-utils rel
	rm -f test/singledc/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/singledc/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test/singledc -cover test/antidote.coverspec
endif

multidc: compile-utils rel
	rm -f test/multidc/*.beam
	mkdir -p logs
ifdef SUITE
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -suite test/multidc/${SUITE} -cover test/antidote.coverspec
else
	ct_run -pa ./_build/default/lib/*/ebin test/utils/ -logdir logs -dir test/multidc -cover test/antidote.coverspec
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
