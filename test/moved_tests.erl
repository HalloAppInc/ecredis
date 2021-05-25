-module(moved_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    lager:start(),
    ecredis_test_util:start_cluster(),
    {ok, _Pid} = ecredis:start_link(ecredis_test, [{"127.0.0.1", 30051}]),
    ok.

cleanup(_) ->
    ok = ecredis:stop(ecredis_test),
    ecredis_test_util:stop_cluster(),
    ok.

all_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,
            {timeout, 10, [
                {"expand_cluster", fun expand_cluster/0}
            ]}}}.

expand_cluster() ->
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ecredis_test, ["SET", "foo", "bar"])
    ),
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_test, ["GET", "foo"])),

    Slot = ecredis_command_parser:get_key_slot("foo"),
    {Pid1, _Version1} = ecredis_server:get_eredis_pid_by_slot(ecredis_test, Slot),
    [[[_Host, Port]]] = ecredis_server:lookup_address_info(ecredis_test, Pid1),

    ecredis_test_util:add_node(30057, 30051, true), % master
    ecredis_test_util:add_node(30058, 30051, false), % slave
    ok = ecredis_test_util:migrate_slot(Slot, Port, 30057),

    % our cluster client should still think the key is in the old Pid.
    ?assertMatch({Pid1, _}, ecredis_server:get_eredis_pid_by_slot(ecredis_test, Slot)),

    timer:sleep(500),
    % get commands should work after a moved
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_test, ["GET", "foo"])),

    timer:sleep(100), % give some time for the change to get reflected in the slot map
    % checking again should give a diffrent Pid
    {Pid2, _Version2} = ecredis_server:get_eredis_pid_by_slot(ecredis_test, Slot),
    ?debugFmt("P1 ~p P2 ~p", [Pid1, Pid2]),
    ?assert(Pid1 =/= Pid2),
    [[[_Host, DestPort]]] = ecredis_server:lookup_address_info(ecredis_test, Pid2),
    ?assertEqual(30057, DestPort),

    ok = ecredis_test_util:migrate_slot(Slot, 30057, Port),

    ok = ecredis_test_util:remove_node(30058, 30051),
    ok = ecredis_test_util:remove_node(30057, 30051),

    ok.
