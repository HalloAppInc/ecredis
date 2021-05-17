-module(moved_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ecredis_sup:start_link().

cleanup(_) ->
    ecredis_sup:stop().

all_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,[
            {"expand_cluster", fun expand_cluster/0}
            ]}}.

expand_cluster() ->
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ecredis_a, ["SET", "foo", "bar"])
    ),

    Slot = ecredis_command_parser:get_key_slot("foo"),
    {Pid1, _Version1} = ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot),
    [[[_Host, Port]]] = ecredis_server:lookup_address_info(ecredis_a, Pid1),

    add_node(30007, 30001),
    ok = migrate_slot(Slot, Port, 30007),

    % our cluster client should still think the key is in the old Pid.
    ?assertMatch({Pid1, _}, ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot)),

    % get commands should work after a moved
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_a, ["GET", "foo"])),

    % checking again should give a diffrent Pid
    {Pid2, _Version2} = ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot),
    ?assert(Pid1 =/= Pid2),
    [[[_Host, DestPort]]] = ecredis_server:lookup_address_info(ecredis_a, Pid2),
    ?assertEqual(30007, DestPort),

    ok = migrate_slot(Slot, 30007, Port),

    ok = remove_node(30007, 30001),

    ok.

%==========================================================================================%
% Helper functions
%==========================================================================================%

add_node(Port, ExistingPort) ->
    cleanup_files(Port),

    Cmd = lists:flatten(io_lib:format("redis-server --port ~p --protected-mode yes --cluster-enabled yes --appendonly yes "
    "--appendfilename appendonly-~p.aof --dbfilename dump-~p.rdb "
    "--logfile ~p.log ""--daemonize yes", [Port, Port, Port, Port])),
    Res = os:cmd(Cmd),
    ?debugFmt("Starting node ~p: ~p", [Cmd, Res]),

    timer:sleep(1000),
    {ok, C} = eredis:start_link("127.0.0.1", Port),
    Res2 = os:cmd("redis-cli --cluster add-node 127.0.0.1:" ++ integer_to_list(Port) ++ " 127.0.0.1:" ++ integer_to_list(ExistingPort)),
    ?debugFmt("Add node ~p", [Res2]),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    NodeId.

remove_node(Port, _ExisitngPort) ->
    {ok, C} = eredis:start_link("127.0.0.1", Port),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    {ok, NodesBin} = eredis:q(C, ["CLUSTER", "NODES"]),
    eredis:stop(C),
    NodesBins = binary:split(NodesBin, <<"\n">>, [global]),

    Nodes = lists:map(
        fun
            (<<>>) ->
                undefined;
            (NodeBin) ->
                [_NodeId, HostPorts | _Rest] = binary:split(NodeBin, <<" ">>, [global]),
                [HostPort, _OtherPort] = binary:split(HostPorts, <<"@">>, [global]),
                [Host, Port2] = binary:split(HostPort, <<":">>, [global]),
                {binary_to_list(Host), binary_to_integer(Port2)}
        end,
        NodesBins),

    Cmd = "pkill -e -f 'redis-server \\\*:" ++  integer_to_list(Port) ++ "'",
    CmdRes = os:cmd(Cmd),
    ?debugFmt("~p -> ~p", [Cmd, CmdRes]),

    Nodes2 = lists:filter(
        fun
            (undefined) -> false;
            ({_, Port2}) ->  Port2 =/= Port
        end, Nodes),

    lists:foreach(
        fun ({Host, Port2}) ->
            case eredis:start_link(Host, Port2) of
                {ok, C1} ->
                    {ok, X} = eredis:q(C1, ["CLUSTER", "FORGET", NodeId]),
                    ?debugFmt("~p:~p ~p", [Host, Port2, X]),
                    eredis:stop(C1),
                    ok;
                _ -> ok
            end
        end, Nodes2),

    cleanup_files(Port),

    ok.

cleanup_files(Port) ->
    % cleanup any old files for this node.
    os:cmd(lists:flatten(io_lib:format(
        "rm nodes.conf appendonly-~p.aof dump-~p.rdb ~p.log", [Port, Port, Port]))),
    ok.

migrate_slot(Slot, FromPort, ToPort) ->
    ?debugFmt("migrate ~p from ~p to ~p", [Slot, FromPort, ToPort]),
    {ok, FromC} = eredis:start_link("127.0.0.1", FromPort),
    {ok, ToC} = eredis:start_link("127.0.0.1", ToPort),

    {ok, FromId} = eredis:q(FromC, ["CLUSTER", "MYID"]),
    {ok, ToId} = eredis:q(ToC, ["CLUSTER", "MYID"]),

    ?debugFmt("FromId: ~p ToId: ~p Slot: ~p", [FromId, ToId, Slot]),

    timer:sleep(1000),
    {ok, <<"OK">>} = eredis:q(ToC, ["CLUSTER", "SETSLOT", Slot, "IMPORTING", FromId]),
    {ok, <<"OK">>} = eredis:q(FromC, ["CLUSTER", "SETSLOT", Slot, "MIGRATING", ToId]),

    ok = migrate_keys(FromC, Slot, FromPort, ToPort),

    {ok, <<"OK">>} = eredis:q(ToC, ["CLUSTER", "SETSLOT", Slot, "NODE", ToId]),
    % When a master node owns only one slot and it gets migrated out the node becomes a slave
    % TODO: call SETNODE on all the masters
    % We only need to do SETSLOT on one of the nodes. https://redis.io/commands/cluster-setslot
    %%    {ok, <<"OK">>} = eredis:q(FromC, ["CLUSTER", "SETSLOT", Slot, "NODE", ToId]),
    ok.

migrate_keys(FromC, Slot, FromPort, ToPort) ->
    {ok, Keys} = eredis:q(FromC, ["CLUSTER", "GETKEYSINSLOT", Slot, 100]),
    case Keys of
        [] -> ok;
        _ ->
            lists:foreach(
                fun (Key) ->
                    ?debugFmt("Migrating ~p:~p from ~p to ~p", [Slot, Key, FromPort, ToPort]),
                    {ok, <<"OK">>} = eredis:q(FromC, ["MIGRATE", "127.0.0.1", ToPort, Key, 0, 5000])
                end, Keys),
            migrate_keys(FromC, Slot, FromPort, ToPort)
    end.

