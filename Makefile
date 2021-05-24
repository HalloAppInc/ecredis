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
	./rebar eunit skip_deps=true

.PHONY: test clean

