all: compile

compile:
	./rebar compile

clean:
	./rebar clean
	rm -rf priv/*.so
	rm -rf c_src/*.o
	rm -rf .eunit/

check: compile test

test:
	# Tried this but does not seem to work.
	# https://stackoverflow.com/questions/14315207/how-to-start-lager-before-running-eunit-test-with-rebar
	# ERL_AFLAGS="-s lager"
	# ./rebar eunit skip_deps=true
	./rebar eunit skip_deps=true

.PHONY: test clean

