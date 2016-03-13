REBAR = $(shell pwd)/rebar3
.PHONY: rel test relgentlerain

all: compile #test compile-riak-test

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

distclean: clean devclean relclean cleanplt
	$(REBAR) clean --all

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