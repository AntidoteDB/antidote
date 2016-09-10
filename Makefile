REBAR = $(shell pwd)/rebar3
.PHONY: rel test relgentlerain

all: compile #test compile-riak-test

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean relclean #cleanplt
	$(REBAR) clean --all

cleantests:
	rm -f test/*.beam
	rm -rf logs/

rel:
	$(REBAR) release --overlay_vars rel/vars.config

relclean:
	rm -rf _build/default/rel

relgentlerain: export TXN_PROTOCOL=gentlerain
relgentlerain: relclean cleantests rel

relnocert: export NO_CERTIFICATION=true
relnocert: relclean cleantests rel

stage :
	$(REBAR) release -d

include tools.mk

tutorial:
	docker build -f Dockerfiles/antidote-tutorial -t cmeiklejohn/antidote-tutorial .
	docker run -t -i cmeiklejohn/antidote-tutorial

