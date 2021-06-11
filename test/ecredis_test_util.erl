%%%-------------------------------------------------------------------
%%% @author nikola
%%% @copyright (C) 2021, Halloapp Inc.
%%% @doc
%%%
%%% @end
%%% Created : 24. May 2021 2:20 PM
%%%-------------------------------------------------------------------
-module(ecredis_test_util).
-author("nikola").

-include_lib("eunit/include/eunit.hrl").


%% API
-export([
    start_cluster/0,
    stop_cluster/0,
    add_node/3,
    remove_node/1,
    migrate_slot/3,
    migrate_slot_start/3,
    migrate_slot_end/3,
    migrate_keys/3
]).

start_cluster() ->
    ensure_redis_installed(),
    cleanup_files(),
    Res = os:cmd("cd ../scripts; ./create-cluster start; echo yes | ./create-cluster create"),
    ?debugFmt("cluster start: ~s", [Res]),
    ok.

stop_cluster() ->
    Res = os:cmd("cd ../scripts; ./create-cluster stop"),
    ?debugFmt("cluster stop: ~s", [Res]),
    ok.

add_node(Port, ExistingPort, IsMaster) ->

    Cmd = lists:flatten(io_lib:format("cd ../scripts; redis-server --port ~p --protected-mode yes "
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

    Cmd3 = case IsMaster of
        true -> Cmd2;
        false -> Cmd2 ++ " --cluster-slave"
    end,
    Res3 = os:cmd(Cmd3),
    ?debugFmt("Add node ~p", [Res3]),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    timer:sleep(500), % wait for the new node to join the cluter
    {ok, Nodes} = eredis:q(C, ["CLUSTER", "NODES"]),
    % TODO: assert the new node is master and is in the nodes reply
    ?debugFmt("Nodes: ~n~s", [Nodes]),
    NodeId.

% Removes the node from the cluster and stops it. Make sure the node is not serving any slots.
remove_node(Port) ->
    {ok, C} = eredis:start_link("127.0.0.1", Port),
    {ok, NodeId} = eredis:q(C, ["CLUSTER", "MYID"]),
    {ok, NodesBin} = eredis:q(C, ["CLUSTER", "NODES"]),
    eredis:stop(C),
    Nodes = parse_nodes(NodesBin),
    NodesHostPorts = [HostPort || {_, HostPort, _} <- Nodes],

    % kills redis-server running on this port
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

    ok.



migrate_slot_start(Slot, FromPort, ToPort) ->
    ?debugFmt("migrate ~p from ~p to ~p", [Slot, FromPort, ToPort]),
    {ok, FromC} = eredis:start_link("127.0.0.1", FromPort),
    {ok, ToC} = eredis:start_link("127.0.0.1", ToPort),

    {ok, FromId} = eredis:q(FromC, ["CLUSTER", "MYID"]),
    {ok, ToId} = eredis:q(ToC, ["CLUSTER", "MYID"]),

    ?debugFmt("FromId: ~p ToId: ~p Slot: ~p", [FromId, ToId, Slot]),

    timer:sleep(1000),
    {ok, <<"OK">>} = eredis:q(ToC, ["CLUSTER", "SETSLOT", Slot, "IMPORTING", FromId]),
    {ok, <<"OK">>} = eredis:q(FromC, ["CLUSTER", "SETSLOT", Slot, "MIGRATING", ToId]),
    ok = eredis:stop(FromC),
    ok = eredis:stop(ToC),
    ok.

migrate_slot_end(Slot, _FromPort, ToPort) ->
    {ok, ToC} = eredis:start_link("127.0.0.1", ToPort),
    {ok, ToId} = eredis:q(ToC, ["CLUSTER", "MYID"]),
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


migrate_slot(Slot, FromPort, ToPort) ->
    ok = migrate_slot_start(Slot, FromPort, ToPort),
    ok = migrate_keys(Slot, FromPort, ToPort),
    ok = migrate_slot_end(Slot, FromPort, ToPort),
    ok.

migrate_keys(Slot, FromPort, ToPort) ->
    {ok, FromC} = eredis:start_link("127.0.0.1", FromPort),
    ok = migrate_keys(FromC, Slot, FromPort, ToPort),
    ok = eredis:stop(FromC),
    ?debugFmt("Migrating keys finished Slot:~p ~p -> ~p", [Slot, FromPort, ToPort]),
    ok.

migrate_keys(FromC, Slot, FromPort, ToPort) ->
    % get 100 keys that need to be migrated
    {ok, Keys} = eredis:q(FromC, ["CLUSTER", "GETKEYSINSLOT", Slot, 100]),
    case Keys of
        [] -> ok;
        _ ->
            lists:foreach(
                fun (Key) ->
                    ?debugFmt("Migrating ~p:~p from ~p to ~p", [Slot, Key, FromPort, ToPort]),
                    % 0 is the destination DB
                    % 5000 is timeout of this operation. More on redis.io
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


ensure_redis_installed() ->
    ?assertNotEqual("", os:cmd("which redis-server")).


cleanup_files() ->
    os:cmd("cd ../scripts; ./create-cluster clean"),
    ok.

