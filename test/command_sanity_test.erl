-module(command_sanity_test).

-include_lib("eunit/include/eunit.hrl").


single_command_test() ->
    ?assertEqual(ok, ecredis_command_parser:check_sanity_of_keys(["SET", "foo", "bar"])),
    ?assertEqual(ok, ecredis_command_parser:check_sanity_of_keys(["EXISTS", "foo"])),
    ok.

multi_command1_test() ->
    Command = [
        ["MULTI"],
        ["DEL",<<"pas:{1}">>],
        ["HSET",<<"pas:{1}">>,<<"sal">>, <<"123">>],
        ["HSET",<<"acc:{1}">>,<<"sal">>, <<"123">>],
        ["EXEC"]
    ],
    ?assertEqual(ok, ecredis_command_parser:check_sanity_of_keys(Command)),
    ok.

multi_command2_test() ->
    Command = [
        ["MULTI"],
        ["DEL",<<"pas:{1}">>],
        ["HSET",<<"acc:{12000}">>,<<"sal">>, <<"123">>],
        ["HSET",<<"acc:{5000}">>,<<"sal">>, <<"123">>],
        ["EXEC"]
    ],
    ?assertEqual(error, ecredis_command_parser:check_sanity_of_keys(Command)),
    ok.


multiple_commands_test() ->
    Command = [
        ["HSET",<<"acc:{1000000000376503286}">>,<<"na">>,<<"murali">>],
        ["INCR",<<"c_reg:{48064}.12419">>],
        ["INCR",<<"c_acc:{48064}.12419">>],
        ["DECR",<<"d_acc:{48064}.12419">>]
    ],
    ?assertEqual(ok, ecredis_command_parser:check_sanity_of_keys(Command)),
    ok.


eval_command1_test() ->
    Command = ["EVAL", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
            2, "acc:{123}", "pas:{123}", first, second],
    ?assertEqual(ok, ecredis_command_parser:check_sanity_of_keys(Command)),
    ok.


eval_command2_test() ->
    Command = ["EVAL", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
            2, "acc:{2}", "pas:{1}", first, second],
    ?assertEqual(error, ecredis_command_parser:check_sanity_of_keys(Command)),
    ok.


