all:
	./rebar compile

clean:
	./rebar clean
	rm -rf priv/*.so
	rm -rf c_src/*.o
	rm -rf .eunit/

check:
	./rebar compile eunit
