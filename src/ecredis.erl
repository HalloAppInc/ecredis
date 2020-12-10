-module(ecredis).

-define(ECREDIS_SERVER, ecredis_server).

%% API.
-export([
    start_link/1,
    qp/2,
    q/2
]).

-include("ecredis.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link({ClusterName :: atom(), InitNodes :: [{}]}) -> {ok, pid()}.
start_link({ClusterName, InitNodes}) ->
    error_logger:info_msg("starting ~p", [ClusterName]),
    gen_server:start_link({local, ClusterName}, ?ECREDIS_SERVER, [ClusterName, InitNodes], []).


%% Executes qp.
-spec qp(ClusterName :: atom(), Commands :: redis_pipeline_command()) -> redis_pipeline_result().
qp(ClusterName, Commands) ->
    query(ClusterName, Commands).
%% TODO(murali@): ensure that these commands contain keys from same slot, else return an error.


%% Executes q.
-spec q(ClusterName :: atom(), Command :: redis_command()) -> redis_result().
q(ClusterName, Command) ->
    query(ClusterName, Command).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

query(ClusterName, Command) ->
    Key = ecredis_command_parser:get_key_from_command(Command),
    query(ClusterName, Command, Key).


query(_Cluster, Command, undefined) ->
    error_logger:error_msg("Unable to execute: ~p, invalid cluster key~n", [Command]),
    {error, invalid_cluster_key, Command};
query(ClusterName, Command, Key) ->
    Slot = ecredis_command_parser:get_key_slot(Key),
    execute_slot_query(ClusterName, Command, Slot, 0).


execute_slot_query(ClusterName, Command, Slot, Counter) ->
    case ecredis_server:get_eredis_pid(ClusterName, Slot) of
        undefined -> execute_query(ClusterName, undefined, Command, Slot, undefined, Counter);
        {Pid, Version} -> execute_query(ClusterName, Pid, Command, Slot, Version, Counter)
    end.


execute_query(_Cluster, undefined, Command, Slot, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    error_logger:error_msg("Unable to execute: ~p, slot: ~p has no connection~n", [Command, Slot]),
    {error, no_connection, Command};
execute_query(ClusterName, Pid, Command, Slot, Version, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    case eredis_query(Pid, Command) of
        % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            error_logger:warning_msg("no_connection, ~p v: ~p", [ClusterName, Version]),
            {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
            execute_slot_query(ClusterName, Command, Slot, Counter + 1);

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case.
        {error, tcp_closed} ->
            error_logger:warning_msg("tcp_closed, ~p v: ~p", [ClusterName, Version]),
            execute_query(ClusterName, Pid, Command, Slot, Version, Counter + 1);

        % Redis explicitly say our slot mapping is incorrect, we need to refresh it.
        {error, <<"MOVED ", _/binary>>} ->
            error_logger:warning_msg("moved, ~p v: ~p", [ClusterName, Version]),
            {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
            execute_slot_query(ClusterName, Command, Slot, Counter + 1);

        Result ->
            %% When querying multiple commands, result will be an array.
            %% Check for errors if slot mapping is incorrect, we need to refresh and remap them.
            case check_for_moved_errors(Result) of
                true ->
                    case ecredis_command_parser:check_sanity_of_keys(Command) of
                        ok ->
                            %% TODO(murali@): execute only queries that failed.
                            error_logger:warning_msg("moved, ~p v: ~p", [ClusterName, Version]),
                            {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
                            execute_slot_query(ClusterName, Command, Slot, Counter + 1);
                        error ->
                            error_logger:error_msg("clustername, ~p v: ~p, invalid_keys in command: ~p",
                                    [ClusterName, Version, Command]),
                            Result
                    end;
                false ->
                    Result
            end
    end.


eredis_query(Pid, [[X|_]|_] = Commands) when is_list(X); is_binary(X) ->
    eredis:qp(Pid, Commands);
eredis_query(Pid, Command) ->
    eredis:q(Pid, Command).


-spec throttle_retries(integer()) -> ok.
throttle_retries(0) -> ok;
throttle_retries(_) -> timer:sleep(?REDIS_RETRY_DELAY).


check_for_moved_errors([{error, <<"MOVED ", _/binary>>} | _Rest]) ->
    true;
check_for_moved_errors([_ | Rest]) ->
    check_for_moved_errors(Rest);
check_for_moved_errors(_) ->
    false.

