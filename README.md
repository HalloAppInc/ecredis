# ecredis - Erlang Redis Cluster Client

Resources used in the building of ecredis:
- https://github.com/Tiroshan/eredis_cluster_client
- https://github.com/Nordix/eredis_cluster



## Query Specs

### Single Queries

```erlang
-spec q(ClusterName :: atom(), Command :: redis_command()) -> redis_result()
```
`q` should be used to send simple queries to Redis. 

### Pipelines

```erlang
-spec qp(ClusterName :: atom(), Commands :: redis_command()) -> redis_result()
```
`qp` should be used for pipeline commands to Redis.

#### Caller Assumptions

- Every key in the pipeline hashes to the same slot (prints out error if they don't)
  - Note: redirects are still served, even if the keys don't all hash to the same slot

### Multi-Node Queries

```erlang
-spec qmn(ClusterName :: atom(), Commands :: redis_command()) -> redis_result()
```
`qmn` should be used to send a group of commands that don't all hash to the same slot. Internally, `qmn` separates the list of commands by destination (preserving order), and sends these as individual pipelines. Because of the nature of Redis Cluster, commands across slots are not guaranteed to happen in order.(commands that hash to the same slot do have this guarantee - see redis hash tags).

