-module(ecredis_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ecredis_sup:start_link().


cleanup(_) ->
    ecredis_sup:stop().

get_and_set(ClusterName) ->
    ?assertEqual({ok, <<"OK">>}, ecredis:q(ClusterName, ["SET", "key", "value"])),
    ?assertEqual({ok, <<"value">>}, ecredis:q(ClusterName, ["GET", "key"])),
    ?assertEqual({ok, undefined}, ecredis:q(ClusterName, ["GET", "nonexists"])).

get_and_set_a() ->
    get_and_set(ecredis_a).

get_and_set_b() ->
    get_and_set(ecredis_b).

binary(ClusterName) ->
    ?assertEqual({ok, <<"OK">>}, ecredis:q(ClusterName,
                                           [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
    ?assertEqual({ok, <<"value_binary">>}, ecredis:q(ClusterName, [<<"GET">>, <<"key_binary">>])),
    ?assertEqual([{ok, <<"value_binary">>}, {ok, <<"value_binary">>}],
                 ecredis:qp(ClusterName, [[<<"GET">>, <<"key_binary">>],
                                          [<<"GET">>, <<"key_binary">>]])).

binary_a() ->
    binary(ecredis_a).

binary_b() ->
    binary(ecredis_b).

delete(ClusterName) ->
    ?assertMatch({ok, _}, ecredis:q(ClusterName, ["DEL", "a"])),
    ?assertEqual({ok, <<"OK">>}, ecredis:q(ClusterName, ["SET", "b", "a"])),
    ?assertEqual({ok, <<"1">>}, ecredis:q(ClusterName, ["DEL", "b"])),
    ?assertEqual({ok, undefined}, ecredis:q(ClusterName, ["GET", "b"])).

delete_a() ->
    delete(ecredis_a).

delete_b() ->
    delete(ecredis_b).

pipeline(ClusterName) ->
    ?assertNotMatch([{ok, _},{ok, _},{ok, _}],
                    ecredis:qp(ClusterName, [["SET", "a1", "aaa"],
                                             ["SET", "a2", "aaa"],
                                             ["SET", "a3", "aaa"]])),
    ?assertMatch([{ok, _},{ok, _},{ok, _}],
                 ecredis:qp(ClusterName, [["LPUSH", "a", "aaa"],
                                          ["LPUSH", "a", "bbb"],
                                          ["LPUSH", "a", "ccc"]])).

pipeline_a() ->
    pipeline(ecredis_a).

pipeline_b() ->
    pipeline(ecredis_b).

multinode(ClusterName) ->
    N=1000,
    Keys = [integer_to_list(I) || I <- lists:seq(1, N)],
    [ecredis:q(ClusterName, ["SETEX", Key, "50", Key]) || Key <- Keys],
    _ = [{ok, integer_to_binary(list_to_integer(Key) + 1)} || Key <- Keys],
    %% ?assertMatch(Guard1, eredis_cluster:qmn([["INCR", Key] || Key <- Keys])),
    ecredis:q(ClusterName, ["SETEX", "a", "50", "0"]),
    _ = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1, 5)].
    %% ?assertMatch(Guard2, eredis_cluster:qmn([["INCR", "a"] || _I <- lists:seq(1,5)]))
 
multinode_a() ->
    multinode(ecredis_a).

multinode_b() ->
    multinode(ecredis_b).

eval_key(ClusterName) ->
    ecredis:q(ClusterName, ["del", "foo"]),
    ecredis:q(ClusterName, ["eval","return redis.call('set', KEYS[1],'bar')", "1", "foo"]),
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ClusterName, ["GET", "foo"])).

eval_key_a() ->
    eval_key(ecredis_a).

eval_key_b() ->
    eval_key(ecredis_b).

eval_sha(ClusterName) ->
    % In this test the key "load" will be used because the "script
    % load" command will be executed in the redis server containing
    % the "load" key. The script should be propagated to other redis
    % client but for some reason it is not done on Travis test
    % environment. @TODO : fix travis redis cluster configuration,
    % or give the possibility to run a command on an arbitrary
    % redis server (no slot derived from key name)
    ecredis:q(ClusterName, ["del", "load"]),
    {ok, Hash} = ecredis:q(ClusterName, 
                           ["script", "load", "return redis.call('set', KEYS[1], 'bar')"]),
    ecredis:q(ClusterName, ["evalsha", Hash, 1, "load"]),
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ClusterName, ["GET", "load"])).

bitstring_support(ClusterName) ->
    ecredis:q(ClusterName, [<<"set">>, <<"bitstring">>, <<"support">>]),
    ?assertEqual({ok, <<"support">>}, ecredis:q(ClusterName, [<<"GET">>, <<"bitstring">>])).

bitstring_support_a() ->
    bitstring_support(ecredis_a).

bitstring_support_b() ->
    bitstring_support(ecredis_b).
     
eval_sha_a() ->
    eval_sha(ecredis_a).

eval_sha_b() ->
    eval_sha(ecredis_b).

basic_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,
         [{"get and set a", fun get_and_set_a/0},
          {"get and set b", fun get_and_set_b/0},
          {"binary a", fun binary_a/0},
          {"binary b", fun binary_b/0},
          {"delete test a", fun delete_a/0},
          {"delete test b", fun delete_b/0},
          {"pipeline a", fun pipeline_a/0},
          {"pipeline b", fun pipeline_b/0},
          {"multi node a", fun multinode_a/0},
          {"multi node b", fun multinode_b/0},
          {"eval key a", fun eval_key_a/0},
          {"eval key b", fun eval_key_b/0},
          {"evalsha a", fun eval_sha_a/0},
          {"evalsha b", fun eval_sha_b/0},
          {"bitstring support a", fun bitstring_support_a/0},
          {"bitstring support b", fun bitstring_support_b/0}]
        }
    }.
