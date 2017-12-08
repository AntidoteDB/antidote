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
	rm -f test/*.beam
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

include tools.mk

# Tutorial targets.

tutorial:
	docker build -f Dockerfiles/antidote-tutorial -t cmeiklejohn/antidote-tutorial .
	docker run -t -i cmeiklejohn/antidote-tutorial

# Mesos targets.

foreground: rel
	./_build/default/rel/antidote/bin/env foreground

console: rel
	./_build/default/rel/antidote/bin/env console

mesos-docker-build:
	docker build -f Dockerfiles/antidote-mesos -t cmeiklejohn/antidote-mesos .

mesos-docker-run: mesos-docker-build
	docker run -t -i cmeiklejohn/antidote-mesos

mesos-docker-build-dev:
	docker build -f Dockerfiles/antidote-mesos-dev -t cmeiklejohn/antidote-mesos-dev .

mesos-docker-run-dev: mesos-docker-build-dev
	docker run -t -i cmeiklejohn/antidote-mesos-dev

docker-build:
	docker build -f Dockerfiles/Dockerfile -t antidotedb/antidote Dockerfiles

docker-local:
	docker run --rm -v $(shell pwd):/code --workdir /code erlang:19 make rel
	docker build -f Dockerfiles/Dockerfile-local -t antidotedb/antidote:local .
