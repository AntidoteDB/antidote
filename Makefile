REBAR = $(shell pwd)/rebar
.PHONY: rel deps test relgentlerain

all: deps compile test compile-riak-test

compile: deps
	$(REBAR) compile

compile-riak-test: compile
	$(REBAR) skip_deps=true riak_test_compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean
	rm -rf riak_test/ebin

cleantests:
	rm -rf riak_test/ebin

distclean: clean devclean relclean cleanplt
	$(REBAR) delete-deps

rel: all
	$(REBAR) generate

relgentlerain: export TXN_PROTOCOL=gentlerain
relgentlerain: relclean cleantests rel

relnocert: export NO_CERTIFICATION = true
relnocert: relclean cleantests rel

relclean:
	rm -rf rel/antidote

stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/antidote/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/antidote/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/antidote/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/antidote/lib;)

currentdevrel: stagedevrel compile-riak-test
	riak_test/bin/antidote-current.sh

riak-test: currentdevrel
	$(foreach dep,$(wildcard riak_test/*.erl), ../riak_test/riak_test -v -c antidote -t $(dep);)

stage-riak-test: all
	$(foreach dep,$(wildcard riak_test/*.erl), ../riak_test/riak_test -v -c antidote -t $(dep);)

##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

.PHONY : stagedevrel devrel

DEVNODES ?= 3

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)
	  $(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)

devclean: clean
	rm -rf dev

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

include tools.mk

typer:
	typer --annotate -I ../ --plt $(PLT) -r src
