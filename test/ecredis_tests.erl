-module(ecredis_tests).

-include_lib("eunit/include/eunit.hrl").

-include("ecredis.hrl").

%%% TEST NOTES
%%% 
%%% Tests have been run only on a redis cluster with three primary nodes, and one
%%% replica node for each primary.


setup() ->
    ecredis_sup:start_link().


cleanup(_) ->
    ecredis_sup:stop().


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% get_and_set
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_and_set(ClusterName) ->
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ClusterName, ["SET", "key", "value"])
    ),
    ?assertEqual(
        {ok, <<"value">>},
        ecredis:q(ClusterName, ["GET", "key"])
    ),
    ?assertEqual(
        {ok, undefined},
        ecredis:q(ClusterName, ["GET", "nonexists"])
    ).

get_and_set_a() ->
    get_and_set(ecredis_a).

get_and_set_b() ->
    get_and_set(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% binary
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
binary(ClusterName) ->
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ClusterName,[<<"SET">>, <<"key_binary">>, <<"value_binary">>])
    ),
    ?assertEqual(
        {ok, <<"value_binary">>},
        ecredis:q(ClusterName, [<<"GET">>, <<"key_binary">>])
    ),
    ?assertEqual(
        [{ok, <<"value_binary">>}, {ok, <<"value_binary">>}],
        ecredis:qp(ClusterName, [
            [<<"GET">>, <<"key_binary">>],
            [<<"GET">>, <<"key_binary">>]
        ])
    ).

%%% Findings
%%% 
%%% Q: Does an "ASKING" command cause errors when the slot already belongs to
%%% the node?
%%% 
%%% Motivation: The following situation is possible:
%%% 
%%% [{ok, _}, {error, ASK Dest}, {error, ASK Dest}, {error, MOVED Dest}] = [["GET", "{key}1"], ["GET", "{key}2"], ["GET", "{key}3"], ["GET", "{key}4"]]
%%% 
%%% The final MOVED response indicated that the slot that {key} hashes to has been fully
%%% moved to the destination pointed to by Dest. (all destinations are the same since all
%%% hash to the same slot) When ["GET", "{key}2"], ["GET", "{key}3"] are resent,
%%% they are prepended with ASKING commands since that was the response they received,
%%% but the ASKING commands are no longer necessary since the slot now belongs to 
%%% the Destination node. 
%%% 
%%% A: No, it does not. Unnecessary ASKING commands don't cause falures :)


binary_a() ->
    binary(ecredis_a).

binary_b() ->
    binary(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% delete
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
delete(ClusterName) ->
    ?assertMatch(
        {ok, _},
        ecredis:q(ClusterName, ["DEL", "a"])
    ),
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ClusterName, ["SET", "b", "a"])
    ),
    ?assertEqual(
        {ok, <<"1">>},
        ecredis:q(ClusterName, ["DEL", "b"])
    ),
    ?assertEqual(
        {ok, undefined},
        ecredis:q(ClusterName, ["GET", "b"])
    ).

delete_a() ->
    delete(ecredis_a).

delete_b() ->
    delete(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% pipeline
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
pipeline(ClusterName) ->
    % qp queries expect all keys to hash to the same slot
    ?assertMatch(
        [{ok, _},{ok, _},{ok, _}],
        ecredis:qp(ClusterName, [
            ["LPUSH", "a", "aaa"],
            ["LPUSH", "a", "bbb"],
            ["LPUSH", "a", "ccc"]
        ])
    ),

    % hash tags guarantee that all keys hash to the same slot - only the name
    % inside the {} will be hashed
    ?assertMatch(
        [{ok, _},{ok, _},{ok, _}],
        ecredis:qp(ClusterName, [
            ["SET", "{foo}:a1", "aaa"],
            ["SET", "{foo}:a2", "aaa"],
            ["SET", "{foo}:a3", "aaa"]
        ])
    ),

    % qp pipelines are not redirected if all of the keys don't belong to the 
    % same node
    ?assertNotEqual(
        get_pid(ClusterName, "a1"),
        get_pid(ClusterName, "a2")
    ),
    ?assertNotMatch(
        [{ok, _},{ok, _},{ok, _}],
        ecredis:qp(ClusterName, [
            ["SET", "a1", "aaa"],
            ["SET", "a2", "aaa"],
            ["SET", "a3", "aaa"]
        ])
    ).

pipeline_a() ->
    pipeline(ecredis_a).

pipeline_b() ->
    pipeline(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% test_redirect_keeps_pipeline
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
test_redirect_keeps_pipeline(ClusterName) ->
    % assert that key and key2 hash to different nodes
    ?assertNotEqual(
        get_pid(ClusterName, "key1"),
        get_pid(ClusterName, "key2")
    ),

    % get the slot and pid for key1
    Slot1 = ecredis_command_parser:get_key_slot("key1"),
    {Pid1, Version1} = ecredis_server:get_eredis_pid_by_slot(ClusterName, Slot1),

    % construct the multi-node query
    Query = #query{
        query_type = qmn,
        cluster_name = ClusterName,
        command = [
            ["SET", "key1", "value1"],
            ["GET", "key1"],
            ["SET", "key2", "value2"],
            ["GET", "key2"]
        ],
        % last two queries have the same destination, so they should be
        % pipelined when redirected
        response = [
            {ok, <<"OK">>},
            {ok, <<"value1">>},
            {error,<<"MOVED 4998 127.0.0.1:30001">>},
            {error,<<"MOVED 4998 127.0.0.1:30001">>}
        ],
        slot = Slot1,
        pid = Pid1,
        version = Version1,
        retries = 0,
        indices = [1, 2, 3, 4]
    },

    % assert that there's only one query to be re-executed, and that it's a
    % pipeline with the correct commands in the correct order
    ?assertMatch(
        {_, [#query{command = [["SET","key2","value2"],["GET","key2"]]}]},
        ecredis:get_successes_and_retries(Query)
    ).

test_redirect_keeps_pipeline_a() ->
    test_redirect_keeps_pipeline(ecredis_a).

%% TODO(vipin): The following test needs cluster specific values.
%% test_redirect_keeps_pipeline_b() ->
%%    test_redirect_keeps_pipeline(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% asking_test
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
asking_test(ClusterName) ->
    % get the slots and pids of key1 and key2
    MigratingSlot = ecredis_command_parser:get_key_slot("key1"),
    {PidSrc, VersionSrc} = get_pid(ClusterName, "key1"),
    {PidDest, VersionDest} = get_pid(ClusterName, "key2"),

    % in the 3 node configuration, key1 and key2 belong to different slots,
    % and the slots belong to different nodes. This could be different in
    % different configurations, and the test keys would need to be adjusted accordingly
    ?assertNotEqual({PidSrc, VersionSrc}, {PidDest, VersionDest}),

    % clear all keys on PidSrc
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["FLUSHALL"],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
     })),

    % get the node ids of PidSrc and PidDest
    {ok, NodeId1} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "MYID"],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),
    {ok, NodeId2} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "MYID"],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),

    % the node id's are different from each other, since the pids are differnt
    ?assertNotEqual(
        NodeId1,
        NodeId2
    ),

    % this should NOT cause any redirects as the slot has not been moved
    {ok, <<"OK">>} = ecredis:q(ClusterName, ["SET", "{key1}:a", "valueA"]),

    % Do the above command using "MULTI".
    [{ok,<<"OK">>},{ok,<<"QUEUED">>},{ok,[<<"OK">>]}] = 
        ecredis:q(ClusterName, [["MULTI"], ["SET", "{key1}:a", "valueA"], ["EXEC"]]),

    % set PidDest to be ready to import MigratingSlot
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "IMPORTING", NodeId1],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),

    % set PidSrc to be ready to migrate MigratingSlot
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "MIGRATING", NodeId2],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),

    % this query should NOT produce an ASK error, since the key already exists
    {ok, <<"valueA">>} = ecredis:q(ClusterName, ["GET", "{key1}:a"]),

    % Do the above command using "MULTI".
    [{ok,<<"OK">>},{ok,<<"QUEUED">>},{ok,[<<"valueA">>]}] =
        ecredis:q(ClusterName, [["MULTI"], ["GET", "{key1}:a"], ["EXEC"]]),

    % this query should produce an ASK error, and get redirected
    {ok, <<"OK">>} = ecredis:q(ClusterName, ["SET", "{key1}:b", "valueB"]),

    % Do the above command using "MULTI".
    [{ok,<<"OK">>}, {error, _}, {error, _}] =
        ecredis:q(ClusterName, [["MULTI"], ["SET", "{key1}:b", "valueB"], ["EXEC"]]),

    % get the host and port of the destination node
    [[[Host, Port]]] = ecredis_server:lookup_address_info(ClusterName, PidDest),

    % migrate Slot1
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["MIGRATE", Host, Port, "{key1}:a", "0", "5000"],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),

    % notify the involved nodes that the migration is done
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "NODE", NodeId2],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "NODE", NodeId2],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),

    % this should cause a MOVED error, and refresh the mapping

    % Do the below command using "MULTI".
    [{ok,<<"OK">>},{ok,<<"QUEUED">>},{ok,[<<"OK">>]}] =
        ecredis:q(ClusterName, [["MULTI"], ["SET", "{key1}:a", "valueA"], ["EXEC"]]),
    % No moved error
    {ok, <<"OK">>} = ecredis:q(ClusterName, ["SET", "{key1}:a", "valueA"]),

    % since the mapping is refreshed, the pids should be equal because we
    % migrated key1 to the node where key2 lives
    ?assertEqual(
        get_pid(ClusterName, "key1"),
        get_pid(ClusterName, "key2")
    ),

    % Now we move MigratingSlot back to PidSrc, so that we don't interfere
    % with other test cases

    % set PidSrc to be ready to import MigratingSlot
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "IMPORTING", NodeId2],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),

    % set PidDest to be ready to migrate MigratingSlot
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "MIGRATING", NodeId1],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),

    % get the host and port of the destination node
    [[[Host2, Port2]]] = ecredis_server:lookup_address_info(ClusterName, PidSrc),

    % migrate Slot1
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["MIGRATE", Host2, Port2, "", "0", "5000", "KEYS", "{key1}:a", "{key1}:b"],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),

    % notify the involved nodes that the migration is done
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "NODE", NodeId1],
        pid = PidDest,
        version = VersionDest,
        retries = 0,
        indices = [1]
    })),
    {ok, <<"OK">>} = extract_response(ecredis:execute_query(#query{
        query_type = q,
        cluster_name = ClusterName,
        command = ["CLUSTER", "SETSLOT", MigratingSlot, "NODE", NodeId1],
        pid = PidSrc,
        version = VersionSrc,
        retries = 0,
        indices = [1]
    })),

    % this should cause a MOVED error, and refresh the mapping
    {ok, <<"OK">>} = ecredis:q(ClusterName, ["SET", "{key1}:a", "valueA"]),

    % Do the above command using "MULTI". No moved error.
    [{ok,<<"OK">>},{ok,<<"QUEUED">>},{ok,[<<"OK">>]}] =
        ecredis:q(ClusterName, [["MULTI"], ["SET", "{key1}:a", "valueA"], ["EXEC"]]),

    ok.

asking_test_a() ->
    asking_test(ecredis_a).

asking_test_b() ->
    asking_test(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% no_dup_after_successful_moved
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
no_dup_after_successful_moved(ClusterName) ->
    %% The pipeline will get sent to the node where the first key is stored, causing
    %% the SET query to respond with a MOVED error - the test succeeds if the first
    %% command in the pipeline did not get sent twice.
    ?assertMatch(
        {ok, _},
        ecredis:q(ClusterName, ["DEL", "key1"])
    ),
    ?assertMatch(
        [{ok, _}, {ok, _}],
        ecredis:qmn(ClusterName, [
            ["INCR", "key1"],
            ["SET", "key2", "value"]
        ])
    ),
    ?assertMatch(
        {ok, <<"1">>},
        ecredis:q(ClusterName, ["GET", "key1"])
    ).

no_dup_after_successful_moved_a() ->
    no_dup_after_successful_moved(ecredis_a).

no_dup_after_successful_moved_b() ->
    no_dup_after_successful_moved(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% successful_moved_maintains_ordering
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
successful_moved_maintains_oredering(ClusterName) ->
    ?assertMatch(
        [{ok, _}, {ok, _}, {ok, _}],
        ecredis:qmn(ClusterName, [
            ["DEL", "{key}1"],
            ["DEL", "key2"],
            ["DEL", "{key}3"]
        ] )
    ),

    % assert that key and key2 hash to different nodes
    ?assertNotEqual(
        get_pid(ClusterName, "key1"),
        get_pid(ClusterName, "key2")
    ),

    ?assertMatch(
        {ok, _},
        ecredis:q(ClusterName, ["SET", "{key}1", "value1"])
    ),
    ?assertMatch(
        {ok, _},
        ecredis:q(ClusterName, ["SET", "key2", "value2"])
    ),
    ?assertMatch(
        {ok, _},
        ecredis:q(ClusterName, ["SET", "{key}3", "value3"])
    ),

    % the first and third commands will succeed, and the second will cause a MOVED
    % error that needs to be resent. this ensures that ordering of responses is preserved
    ?assertMatch(
        [{ok, <<"value1">>}, {ok, <<"value2">>}, {ok, <<"value3">>}],
        ecredis:qmn(ClusterName, [
            ["GET", "{key}1"],
            ["GET", "key2"],
            ["GET", "{key}3"]
        ])
    ).

successful_moved_maintains_oredering_a() ->
    successful_moved_maintains_oredering(ecredis_a).

successful_moved_maintains_oredering_b() ->
    successful_moved_maintains_oredering(ecredis_b).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% multinode
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% multinode(ClusterName) ->
%     N=1000,
%     Keys = [integer_to_list(I) || I <- lists:seq(1, N)],
%     [ecredis:q(ClusterName, ["SETEX", Key, "50", Key]) || Key <- Keys],
%     _ = [{ok, integer_to_binary(list_to_integer(Key) + 1)} || Key <- Keys],
%     %% ?assertMatch(Guard1, eredis_cluster:qmn([["INCR", Key] || Key <- Keys])),
%     ecredis:q(ClusterName, ["SETEX", "a", "50", "0"]),
%     _ = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1, 5)].
%     %% ?assertMatch(Guard2, eredis_cluster:qmn([["INCR", "a"] || _I <- lists:seq(1,5)]))
 
% multinode_a() ->
%     multinode(ecredis_a).

% multinode_b() ->
%     multinode(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% eval_key
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
eval_key(ClusterName) ->
    ecredis:q(ClusterName, ["del", "foo"]),
    ecredis:q(ClusterName, ["eval","return redis.call('set', KEYS[1],'bar')", "1", "foo"]),
    ?assertEqual(
        {ok, <<"bar">>},
        ecredis:q(ClusterName, ["GET", "foo"])
    ).

eval_key_a() ->
    eval_key(ecredis_a).

eval_key_b() ->
    eval_key(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% eval_sha
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
eval_sha(ClusterName) ->
    % In this test the key "load" will be used because the "script
    % load" command will be executed in the redis server containing
    % the "load" key. The script should be propagated to other redis
    % client but for some reason it is not done on Travis test
    % environment. @TODO : fix travis redis cluster configuration,
    % or give the possibility to run a command on an arbitrary
    % redis server (no slot derived from key name)
    ecredis:q(ClusterName, ["del", "load"]),
    {ok, Hash} = ecredis:q(ClusterName, ["script", "load", "return redis.call('set', KEYS[1], 'bar')"]),
    ecredis:q(ClusterName, ["evalsha", Hash, 1, "load"]),

    ?assertEqual(
        {ok, <<"bar">>},
        ecredis:q(ClusterName, ["GET", "load"])
    ).

eval_sha_a() ->
    eval_sha(ecredis_a).

eval_sha_b() ->
    eval_sha(ecredis_b).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% bitstring_support
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
bitstring_support(ClusterName) ->
    ecredis:q(ClusterName, [<<"set">>, <<"bitstring">>, <<"support">>]),
    ?assertEqual(
        {ok, <<"support">>},
        ecredis:q(ClusterName, [<<"GET">>, <<"bitstring">>])
    ).

bitstring_support_a() ->
    bitstring_support(ecredis_a).

bitstring_support_b() ->
    bitstring_support(ecredis_b).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internal helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_pid(ClusterName, Key) ->
    ecredis_server:get_eredis_pid_by_slot(ClusterName,
            ecredis_command_parser:get_key_slot(Key)).


extract_response(#query{response = Response}) ->
    Response.


%% TODO(vipin): split into multiple test.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
basic_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,[
            {"get and set a", fun get_and_set_a/0},
            {"get and set b", fun get_and_set_b/0},
            {"binary a", fun binary_a/0},
            {"binary b", fun binary_b/0},
            {"delete a", fun delete_a/0},
            {"delete b", fun delete_b/0},
            {"pipeline a", fun pipeline_a/0},
            {"pipeline b", fun pipeline_b/0},
            {"test redirect keeps pipeline a", fun test_redirect_keeps_pipeline_a/0},
            %% TODO(vipin): The following test needs cluster specific values.
            % {"test redirect keeps pipeline b", fun test_redirect_keeps_pipeline_b/0},  
            {"asking test a", fun asking_test_a/0},
            {"asking test b", fun asking_test_b/0},
            {"no dup after successful moved a", fun no_dup_after_successful_moved_a/0},
            {"no dup after successful moved b", fun no_dup_after_successful_moved_b/0},
            {"successful moved maintains oredering a", fun successful_moved_maintains_oredering_a/0},
            {"successful moved maintains oredering b", fun successful_moved_maintains_oredering_b/0},
            % {"multi node a", fun multinode_a/0},
            % {"multi node b", fun multinode_b/0},
            {"eval key a", fun eval_key_a/0},
            {"eval key b", fun eval_key_b/0},
            {"evalsha a", fun eval_sha_a/0},
            {"evalsha b", fun eval_sha_b/0},
            {"bitstring support a", fun bitstring_support_a/0},
            {"bitstring support b", fun bitstring_support_b/0}
        ]}
    }.
