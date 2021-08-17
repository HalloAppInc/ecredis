all: compile

compile:
	./rebar3 compile

clean:
	./rebar3 clean
	rm -rf log

check: compile test

test:
	./rebar3 eunit

.PHONY: test clean

