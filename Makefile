# Copyright 2012 Erlware, LLC. All Rights Reserved.
#
# This file is provided to you under the Apache License,
# Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain
# a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
 
ERLFLAGS= -pa $(CURDIR)/.eunit -pa $(CURDIR)/ebin -pa $(CURDIR)/deps/*/ebin
 
DEPS_PLT=$(CURDIR)/.deps_plt
DEPS=erts kernel stdlib
 
# =============================================================================
# Verify that the programs we need to run are installed on this system
# =============================================================================
ERL = $(shell which erl)
 
ifeq ($(ERL),)
$(error "Erlang not available on this system")
endif
 
# REBAR=$(shell which rebar)
# ifeq ($(REBAR),)
# $(error "Rebar not available on this system")
# endif

REBAR=./rebar
 
 
.PHONY: all compile doc clean test dialyzer typer shell distclean pdf \
  update-deps clean-common-test-data rebuild
 
all: deps compile test
 
# =============================================================================
# Rules to build the system
# =============================================================================
 
deps:
	$(REBAR) get-deps
	$(REBAR) compile
 
update-deps:
	$(REBAR) update-deps
	$(REBAR) compile
 
compile:
	$(REBAR) skip_deps=true compile
 
doc:
	$(REBAR) skip_deps=true doc
 
eunit: compile clean-common-test-data
	$(REBAR) skip_deps=true eunit
 
$(TEST_DEPS):
	@echo Running EUnit

test: compile eunit
  
PLT ?= $(HOME)/.combo_dialyzer_plt
LOCAL_PLT = .local_dialyzer_plt
DIALYZER_FLAGS ?= -Wunmatched_returns -Werror_handling -Wrace_conditions -Wunderspecs

${PLT}: compile
	@if [ -f $(PLT) ]; then \
		dialyzer --check_plt --plt $(PLT) && \
		dialyzer --add_to_plt --plt $(PLT) --output_plt $(PLT) ; test $$? -ne 1; \
	else \
		dialyzer --build_plt --output_plt $(PLT) ; test $$? -ne 1; \
	fi

${LOCAL_PLT}: compile
	@if [ -d deps ]; then \
		if [ -f $(LOCAL_PLT) ]; then \
			dialyzer --check_plt --plt $(LOCAL_PLT) deps/*/ebin  && \
			dialyzer --add_to_plt --plt $(LOCAL_PLT) --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		else \
			dialyzer --build_plt --output_plt $(LOCAL_PLT) deps/*/ebin ; test $$? -ne 1; \
		fi \
	fi

dialyzer: ${PLT} ${LOCAL_PLT}
	@echo "==> $(shell basename $(shell pwd)) (dialyzer)"
	@if [ -f $(LOCAL_PLT) ]; then \
		PLTS="$(PLT) $(LOCAL_PLT)"; \
	else \
		PLTS=$(PLT); \
	fi; \
	if [ -f dialyzer.ignore-warnings ]; then \
		if [ $$(grep -cvE '[^[:space:]]' dialyzer.ignore-warnings) -ne 0 ]; then \
			echo "ERROR: dialyzer.ignore-warnings contains a blank/empty line, this will match all messages!"; \
			exit 1; \
		fi; \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin > dialyzer_warnings ; \
		egrep -v "^[[:space:]]*(done|Checking|Proceeding|Compiling)" dialyzer_warnings | grep -F -f dialyzer.ignore-warnings -v > dialyzer_unhandled_warnings ; \
		cat dialyzer_unhandled_warnings ; \
		[ $$(cat dialyzer_unhandled_warnings | wc -l) -eq 0 ] ; \
	else \
		dialyzer $(DIALYZER_FLAGS) --plts $${PLTS} -c ebin; \
	fi

typer:
	typer --plt $(DEPS_PLT) -r ./src
 
shell: deps compile
# You often want *rebuilt* rebar tests to be available to the
# shell you have to call eunit (to get the tests
# rebuilt). However, eunit runs the tests, which probably
# fails (thats probably why You want them in the shell). This
# runs eunit but tells make to ignore the result.
	- @$(REBAR) skip_deps=true eunit
	@$(ERL) $(ERLFLAGS)
 
clean:
	- rm -rf $(CURDIR)/test/*.beam
	- rm -rf $(CURDIR)/logs
	- rm -rf $(CURDIR)/ebin
	$(REBAR) skip_deps=true clean
 
distclean: clean
	- rm -rf $(DEPS_PLT)
	- rm -rvf $(CURDIR)/deps
 
rebuild: distclean deps compile escript dialyzer test


