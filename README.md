# ecredis - Erlang Redis Cluster Client

Resources used in the building of ecredis:
- https://github.com/Tiroshan/eredis_cluster_client
- https://github.com/Nordix/eredis_cluster

ecredis is a high performance Erlang client for Redis Cluster. It uses eredis as the underlying
implementation. Allows connecting to multiple redis clusters.

## Features

* Allows connections to multiple redis clusters at the same time
* High throughput. The requests are executed directly on the callers process.
* Uses single eredis connection per node instead of pool of connections.

## Creating a client

```erlang
{ok, Pid} = ecredis:start_link(my_cluster, [{"127.0.0.1", 30000}]),
{ok, <<"OK">>} = ecredis:q(my_cluster, ["SET", "foo", "bar"]),
```

## Query Specs

### Single Queries

```erlang
-spec q(ClusterName :: atom(), Command :: redis_command()) -> redis_result().
```
`q` should be used to send simple queries to Redis. 

### Pipelines

```erlang
-spec qp(ClusterName :: atom(), Commands :: redis_command()) -> redis_result().
```
`qp` should be used for pipeline commands to Redis. It is assumed that every key in the pipeline
hashes to the same slot. (prints out error and does not handle redirects if they don't)

### Multi-Node Queries

```erlang
-spec qmn(ClusterName :: atom(), Commands :: redis_command()) -> redis_result().
```
`qmn` should be used to send a group of commands that don't all hash to the same slot. Internally,
`qmn` separates the list of commands by destination (preserving order), and sends these as individual
pipelines. Because of the nature of Redis Cluster, commands across slots are not guaranteed to happen
in order. (commands that hash to the same slot do have this guarantee - see redis hash tags)

