%%%-------------------------------------------------------------------
%%% @author nikola
%%% @copyright (C) 2021, Halloapp Inc.
%%% @doc
%%%
%%% @end
%%% Created : 30. Apr 2021 11:24 AM
%%%-------------------------------------------------------------------
-module(init_tests).
-author("nikola").

-export([
    start_and_stop/0,
    stop_by_pid/0,
    wrong_cluster_name/0
]).

-include_lib("eunit/include/eunit.hrl").

simple_test() ->
    ?assert(true).

cluster_name_test() ->
    ?assertError(
        {badarg, "bad_test_name", "must be atom"},
        ecredis:start_link("bad_test_name", [{"node", 1234}])).

init_nodes_list_test() ->
    ?assertError(
        {badarg, undefined, "must be list"},
        ecredis:start_link(good_cluster_name, undefined)).

init_nodes_empty_test() ->
    ?assertError(
        {badarg, [], "must not be empty"},
        ecredis:start_link(good_cluster_name, [])).

start_and_stop() ->
    {ok, Pid} = ecredis:start_link(init_cluster_test, [{"127.0.0.1", 30051}]),
    ?assertEqual(true, is_process_alive(Pid)),
    ok = ecredis:stop(init_cluster_test),
    ?assertEqual(false, is_process_alive(Pid)),
    ok.

stop_by_pid() ->
    {ok, Pid} = ecredis:start_link(init_cluster_test, [{"127.0.0.1", 30051}]),
    ?assertEqual(true, is_process_alive(Pid)),
    ok = ecredis:stop(Pid),
    ?assertEqual(false, is_process_alive(Pid)),
    ok.

wrong_cluster_name() ->
    ?assertError({ecredis_invalid_client, foo}, ecredis:q(foo, ["GET", "foo"])),
    ok.

