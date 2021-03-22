-module(ecredis_logger).

-export([
    log_error/2,
    log_warning/2
]).

-include("ecredis.hrl").
-include("logger.hrl").

log_error(Error, Query) ->
    {Fmt, Args} = log(Error, Query),
    ?ERROR(Fmt, Args).

log_warning(Error, Query) ->
    {Fmt, Args} = log(Error, Query),
    ?WARNING(Fmt, Args).

log(Error, Query) ->
    Fmt = "~p, Query type: ~p, Cluster name: ~p, Map version: ~p, Command: ~p, Slot: ~p, Pid: ~p, Response: ~p, Retries: ~p, Indices: ~p",
    Args = [
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
    ],
    {Fmt, Args}.

