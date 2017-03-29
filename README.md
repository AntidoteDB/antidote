# Antidote CRDT library

CRDT implementations to use with Antidote.

# Development

Use the following `make` targets to build and test the CRDT library:


	# compile
	make compile
	# run unit tests:
	make test
	# check types:
	make dialyzer
	# check code style:
	make lint


## PropEr tests

To run the property based tests in the test directory install the [rebar3 PropEr plugin](https://www.rebar3.org/docs/using-available-plugins#proper) by adding the following line to `~/.config/rebar3/rebar.config`:

	{plugins, [rebar3_proper]}.

Then execute the tests with:

	make proper

For more control, you can run PropEr manually and specify parameters like the tested module or the number of generated tests:

	rebar3 proper -n 1000 -m prop_crdt_orset