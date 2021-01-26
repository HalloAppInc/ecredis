-module(ecredis_logger).

-export([
    log_error/2,
    log_warning/2
]).

-include("ecredis.hrl").

log_error(Error, Query) ->
    log(fun error_logger:warning_msg/2, Error, Query).

log_warning(Error, Query) ->
    log(fun error_logger:warning_msg/2, Error, Query).

log(F, Error, Query) ->
    erlang:apply(F, ["~p, Query type: ~p, Cluster name: ~p, Map version: ~p, Command: ~p, Slot: ~p, Pid: ~p, Response: ~p, Retries: ~p, Indices: ~p",[
        Error,
        Query#query.query_type,
        Query#query.cluster_name,
        Query#query.version,
        Query#query.command,
        Query#query.slot,
        Query#query.pid,
        Query#query.response,
        Query#query.retries,
        Query#query.indices
    ]]).

