-module(moved_tests).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ecredis_sup:start_link(),
    lager:start(),
    ok.

cleanup(_) ->
    ecredis_sup:stop().

all_test_() ->
    {inorder,
        {setup, fun setup/0, fun cleanup/1,
            {timeout, 10, [
                {"expand_cluster", fun expand_cluster/0}
            ]}}}.

expand_cluster() ->
    ?assertEqual(
        {ok, <<"OK">>},
        ecredis:q(ecredis_a, ["SET", "foo", "bar"])
    ),
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_a, ["GET", "foo"])),

    Slot = ecredis_command_parser:get_key_slot("foo"),
    {Pid1, _Version1} = ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot),
    [[[_Host, Port]]] = ecredis_server:lookup_address_info(ecredis_a, Pid1),

    add_node(30007, 30001),
    ok = migrate_slot(Slot, Port, 30007),

    % our cluster client should still think the key is in the old Pid.
    ?assertMatch({Pid1, _}, ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot)),

    timer:sleep(500),
    % get commands should work after a moved
    ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_a, ["GET", "foo"])),

    timer:sleep(100), % give some time for the change to get reflected in the slot map
    % checking again should give a diffrent Pid
    {Pid2, _Version2} = ecredis_server:get_eredis_pid_by_slot(ecredis_a, Slot),
    ?debugFmt("P1 ~p P2 ~p", [Pid1, Pid2]),
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

    Cmd = lists:flatten(io_lib:format("redis-server --port ~p --protected-mode yes "
        "--cluster-enabled yes --appendonly yes "
        "--appendfilename appendonly-~p.aof --dbfilename dump-~p.rdb "
        "--cluster-config-file nodes-~p.conf "
        "--logfile ~p.log ""--daemonize yes",
        [Port, Port, Port, Port, Port])),
    Res = os:cmd(Cmd),
    ?debugFmt("Starting node ~s: ~s", [Cmd, Res]),

    timer:sleep(500),
    {ok, C} = eredis:start_link("127.0.0.1", Port),
    Cmd2 = "redis-cli --cluster add-node 127.0.0.1:" ++ integer_to_list(Port) ++
        " 127.0.0.1:" ++ integer_to_list(ExistingPort),

    Res2 = os:cmd(Cmd2),
    ?debugFmt("Add node ~s", [Res2]),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    timer:sleep(500), % wait for the new node to join the cluter
    {ok, Nodes} = eredis:q(C, ["CLUSTER", "NODES"]),
    % TODO: assert the new node is master and is in the nodes reply
    ?debugFmt("Nodes: ~n~s", [Nodes]),
    NodeId.

remove_node(Port, _ExisitngPort) ->
    {ok, C} = eredis:start_link("127.0.0.1", Port),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    {ok, NodesBin} = eredis:q(C, ["CLUSTER", "NODES"]),
    eredis:stop(C),
    Nodes = parse_nodes(NodesBin),
    NodesHostPorts = [HostPort || {_, HostPort, _} <- Nodes],

    Cmd = "pkill -e -f 'redis-server \\\*:" ++  integer_to_list(Port) ++ "'",
    CmdRes = os:cmd(Cmd),
    ?debugFmt("~p -> ~p", [Cmd, CmdRes]),

    % remove self
    NodesHostPorts2 = lists:filter(
        fun
            (undefined) -> false;
            ({_, Port2}) ->  Port2 =/= Port
        end, NodesHostPorts),

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
        end, NodesHostPorts2),

    cleanup_files(Port),

    ok.


cleanup_files(Port) ->
    % cleanup any old files for this node.
    os:cmd(lists:flatten(io_lib:format(
        "rm nodes-~p.conf appendonly-~p.aof dump-~p.rdb ~p.log", [Port, Port, Port, Port]))),
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

    ?debugFmt("Migrating keys finished", []),
    {ok, Nodes} = eredis:q(ToC, ["CLUSTER", "NODES"]),

    % Set the slot in the To node
    {ok, <<"OK">>} = eredis:q(ToC, ["CLUSTER", "SETSLOT", Slot, "NODE", ToId]),
    % Tell all masters about the changes
    % (See very last point https://redis.io/commands/cluster-setslot)
    Masters = get_masters(parse_nodes(Nodes)),
    lists:foreach(
        fun
            ({_Host, Port}) when Port == ToPort ->
                ok;
            ({Host, Port}) ->
            {ok, C} = eredis:start_link(Host, Port),
            case eredis:q(C, ["CLUSTER", "SETSLOT", Slot, "NODE", ToId]) of
                {ok, <<"OK">>} -> ok;
                {error, Reason} -> ?debugFmt("Failed to SETSLOT ~s:~p ~s", [Host, Port, Reason])
            end
        end,
        Masters),

    ?debugFmt("Setslot to node", []),
    ok.

migrate_keys(FromC, Slot, FromPort, ToPort) ->
    {ok, Keys} = eredis:q(FromC, ["CLUSTER", "GETKEYSINSLOT", Slot, 100]),
    case Keys of
        [] -> ok;
        _ ->
            lists:foreach(
                fun (Key) ->
                    ?debugFmt("Migrating ~p:~p from ~p to ~p", [Slot, Key, FromPort, ToPort]),
                    Res = eredis:q(FromC, ["MIGRATE", "127.0.0.1", ToPort, Key, 0, 5000]),
                    case Res of
                        {ok, <<"OK">>} -> ok;
                        {error, Reason} = Err ->
                            ?debugFmt("~p MIGRATE ToPort:~p Key:~p Reason: ~p",
                                [FromC, ToPort, Key, Reason]),
                            error(Err)
                    end
                end, Keys),
            migrate_keys(FromC, Slot, FromPort, ToPort)
    end.

parse_nodes(Nodes) ->
    Nodes2 = binary:split(Nodes, <<"\n">>, [global, trim]),

    lists:map(
        fun(NodeBin) ->
            [NodeId, HostPort, MasterSlave | _Rest] = binary:split(NodeBin, <<" ">>, [global]),
            IsMaster = case binary:match(MasterSlave, <<"master">>) of
                nomatch -> false;
                _ -> true
            end,
            [HostPort2 | _] = binary:split(HostPort, <<"@">>, [global]),
            [Host, Port] = binary:split(HostPort2, <<":">>, [global]),
            {NodeId, {binary_to_list(Host), binary_to_integer(Port)}, IsMaster}
        end,
        Nodes2).

get_masters(ParsedNodes) ->
    lists:filtermap(
        fun
            ({_, _, false}) -> false;
            ({_, HostPort, true}) -> {true, HostPort}
        end,
        ParsedNodes).
