.PHONY: compile-no-deps test docs xref dialyzer \
		
compile-no-deps:
	${REBAR} compile skip_deps=true

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer: compile
	${REBAR} dialyzer
