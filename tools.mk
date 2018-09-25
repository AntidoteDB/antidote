.PHONY: compile-no-deps test docs xref dialyzer \

compile-no-deps:
	${REBAR} compile

docs:
	${REBAR} doc

xref: compile
	${REBAR} xref

dialyzer: compile
	${REBAR} dialyzer
