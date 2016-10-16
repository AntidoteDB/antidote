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
	$(REBAR) shell

rel:
	$(REBAR) release

relclean:
	rm -rf _build/default/rel

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
	./_build/default/rel/antidote/bin/env

mesos-docker-build:
	docker build -f Dockerfiles/antidote-mesos -t cmeiklejohn/antidote-mesos .

mesos-docker-run: mesos-docker-build
	docker run -t -i cmeiklejohn/antidote-mesos
