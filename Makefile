REBAR = $(shell pwd)/rebar3
.PHONY: rel test relgentlerain

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

shell:
	$(REBAR) shell --name='antidote@127.0.0.1' --setcookie antidote --config config/sys-debug.config

# same as shell, but automatically reloads code when changed
# to install add `{plugins, [rebar3_auto]}.` to ~/.config/rebar3/rebar.config
# the tool requires inotifywait (sudo apt install inotify-tools)
# see https://github.com/vans163/rebar3_auto or http://blog.erlware.org/rebar3-auto-comile-and-load-plugin/
auto:
	$(REBAR) auto --name='antidote@127.0.0.1' --setcookie antidote --config config/sys-debug.config

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
	mkdir -p logs
	${REBAR} eunit skip_deps=true

coverage:
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
	docker build -f Dockerfiles/Dockerfile -t antidotedb/antidote Dockerfiles

docker-local:
	docker run --rm -v $(shell pwd):/code -w /code erlang:21 make rel
	docker build -f Dockerfiles/Dockerfile-local -t antidotedb/antidote:local .
