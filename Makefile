REBAR = rebar3


all: compile




compile:
	$(REBAR) compile

clean:
	rm -rf ebin/* test/*.beam logs log
	$(REBAR) clean

test:
	$(REBAR) eunit

dialyzer:
	$(REBAR) dialyzer

shell:
	$(REBAR) shell


_build/gpb:
	mkdir -p _build/
	(cd _build/ && git clone https://github.com/tomas-abrahamsson/gpb && cd gpb && git checkout 4.1.8)

_build/gpb/bin/protoc-erl: _build/gpb
	(cd _build/gpb && make)



generate_pb: proto/antidote.proto _build/gpb/bin/protoc-erl
	_build/gpb/bin/protoc-erl -I. proto/antidote.proto -o-hrl include -o-erl src -modsuffix _pb -strbin
	cp _build/gpb/include/gpb.hrl include/
