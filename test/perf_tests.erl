-module(perf_tests).

-include_lib("eunit/include/eunit.hrl").

-export([perf1/2]).

% N is number of processes, try to do around 100K operations total,
% but no more then 1000 per process and no less then 100
% this modification makes each perf test run faster and we can afford to run then as part of
% the CI
ops_to_do(N) ->
    %% this used to be just 2000 const.
    min(1000, max(100, 100000 div N)).

while(0, _F) -> ok;
while(N, F) ->
    erlang:apply(F, [N]),
    while(N -1, F).

set_get(N) ->
    Key = "oof" ++ integer_to_list(N),
    Val = "bar" ++ integer_to_list(N),
    {ok, _Res1} = ecredis:q(ecredis_perf, ["SET", Key, Val]),
    {ok, _Res2} = ecredis:q(ecredis_perf, ["GET", Key]),
    ok.

perf1(Parent, N) ->
    Start = os:system_time(millisecond),
    while(ops_to_do(N), fun set_get/1),
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
            spawn(?MODULE, perf1, [self(), N])
        end),
    wait_for(N),
    EndTime = os:system_time(microsecond),
    T = EndTime - StartTime,
    NumOps = (2 * N * ops_to_do(N)),
    ?debugFmt("~w processes executed ~p ops for ~w us => ~.1f ops/sec",
        [N, NumOps, T, NumOps / (T / 1000000)]),
    ok.


perf_test2() ->
    ecredis_test_util:start_cluster(),
    {ok, _} = ecredis:start_link(ecredis_perf, [{"127.0.0.1", 30051}]),
    perf_test1(1),
    perf_test1(2),
    perf_test1(5),
    perf_test1(10),
    perf_test1(20),
    perf_test1(50),
    perf_test1(100),
    perf_test1(200),
    ecredis:stop(ecredis_perf),
    ecredis_test_util:stop_cluster(),
    true.

perf_test_() ->
  {timeout, 300, ?_assert(perf_test2())}.

