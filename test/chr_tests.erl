-module(chr_tests).

-include_lib("eunit/include/eunit.hrl").

%% 100000 chr took 20921 us => 4779886.238708 ops/sec
%% 100000 tokens took 26375 us => 3791469.194313 ops/sec
%% 200000 chr took 34346 us => 5823094.392360 ops/sec
%% 200000 tokens took 50580 us => 3954132.068011 ops/sec

while(0, _F) -> ok;
while(N, F) ->
    erlang:apply(F, [N]),
    while(N -1, F).

perf_test1(N) ->
    StartTime = os:system_time(microsecond),
    while(N,
        fun(_X) ->
            Key = "abc{def}ghi",
            First = string:chr(Key, ${),
            Last = string:chr(Key, $}),
            
            string:substr(Key, First + 1, Last - 1)
        end),
    EndTime = os:system_time(microsecond),
    T = EndTime - StartTime,
    ?debugFmt("~w chr took ~w us => ~f ops/sec", [N, T, (N) / (T / 1000000)]),

    StartTime2 = os:system_time(microsecond),
    while(N,
        fun(_X) ->
            Key = "abc{def}ghi",
            Tokens = string:tokens(Key, "{}"),
            if length(Tokens) >= 3 -> lists:nth(2, Tokens); true -> Key end
        end),
    EndTime2 = os:system_time(microsecond),
    T2 = EndTime2 - StartTime2,
    ?debugFmt("~w tokens took ~w us => ~f ops/sec", [N, T2, (N) / (T2 / 1000000)]),
    ok.


perf_test2() ->
    perf_test1(1000),
    perf_test1(2000),
    true.

perf_test_() ->
  {timeout, 300, ?_assert(perf_test2())}.

