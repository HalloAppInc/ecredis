-module(perf_tests).

-include_lib("eunit/include/eunit.hrl").

-export([perf1/1]).

while(0, _F) -> ok;
while(N, F) ->
    erlang:apply(F, [N]),
    while(N -1, F).

set_get(N) ->
    Key = "oof" ++ integer_to_list(N),
    Val = "bar" ++ integer_to_list(N),
    {ok, _Res1} = ecredis:q(ecredis_a, ["SET", Key, Val]),
    {ok, _Res2} = ecredis:q(ecredis_a, ["GET", Key]),
    ok.

perf1(Parent) ->
    Start = os:system_time(millisecond),
    while(20000, fun set_get/1),
    End = os:system_time(millisecond),
    Time = End - Start,
    Parent ! {finished, Time}.


wait_for(0) ->
    ok;
wait_for(M) ->
    receive
        {finished, T} ->
            io:format("finished ~p~n", [T]),
            wait_for(M - 1)
    end.


perf_test1(N) ->
    StartTime = os:system_time(microsecond),
    while(N,
        fun(_X) ->
            spawn(?MODULE, perf1, [self()])
        end),
    wait_for(N),
    EndTime = os:system_time(microsecond),
    T = EndTime - StartTime,
    ?debugFmt("~w processes took ~w us => ~f ops/sec", [N, T, (2 * N * 20000) / (T / 1000000)]),
    ok.


perf_test2() ->
    ecredis_sup:start_link(),
    perf_test1(1),
    perf_test1(2),
    perf_test1(5),
    perf_test1(10),
    perf_test1(20),
    perf_test1(50),
    perf_test1(100),
    perf_test1(200),
    ecredis_sup:stop(),
    true.

perf_test_() ->
  {timeout, 300, ?_assert(perf_test2())}.

