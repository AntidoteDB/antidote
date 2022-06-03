##
# Vax
#
# @file
# @version 0.1

.PHONY: deps
deps:
	mix deps.get
	mix deps.compile

compile: deps
	mix compile

.PHONY: test
test:
	mix test

clean:
	mix clean

distclean: clean
	rm -rf _build
	rm -rf deps

# end
