-module(ecredis).
-include("ecredis.hrl").
-include("logger.hrl").
-include_lib("stdlib/include/assert.hrl").

%% API.
-export([
    start_link/1,
    start_link/2,
    stop/1,
    q/2,
    qp/2,
    qmn/2,
    qa/2,
    get_nodes/1,
    flushdb/1,
    qn/3
]).

-ifdef(TEST).
-export([
    query_by_slot/1,
    execute_query/1,
    get_successes_and_retries/1
]).
-endif.

-export_type([
    redis_command/0,
    redis_pipeline_command/0,
    rnode/0
]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start Redis Cluster client. ClusterName should be atom representing the name
% of this Redis Cluster client. This name should be based in future calls to q() API.
% InitNodes is a list of initial nodes to connect to.
-spec start_link(ClusterName, InitNodes) -> {ok, pid()} when
        ClusterName :: atom(),
        InitNodes :: list(InitNode),
        InitNode :: {Host, Port},
        Host :: string(),
        Port :: integer().
start_link(ClusterName, InitNodes) ->
    start_link(ClusterName, InitNodes, []).


%% @doc Start Redis Cluster client. ClusterName should be atom representing the name
%% of this Redis Cluster client. This name should be based in future calls to q() API.
%% InitNodes is a list of initial nodes to connect to. Options is a list of options:
%% <dl>
%% <dt>`{host, Host}'</dt><dd>DNS name or IP address as string; or unix domain
%% socket as `{local, Path}' (available in OTP 19+); default `"127.0.0.1"'</dd>
%% <dt>`{port, Port}'</dt><dd>Integer, default is 6379</dd>
%% <dt>`{database, Database}'</dt><dd>Integer (or string containing a number);
%% 0 for default database</dd>
%% <dt>`{username, Username}'</dt><dd>String; default: no username</dd>
%% <dt>`{password, Password}'</dt><dd>String; default: no password</dd>
%% <dt>`{reconnect_sleep, ReconnectSleep}'</dt><dd>Integer of milliseconds to
%% sleep between reconnect attempts; default: 100</dd>
%% <dt>`{connect_timeout, Timeout}'</dt><dd>Timeout value in milliseconds to use
%% when connecting to Redis; default: 5000</dd>
%% <dt>`{socket_options, SockOpts}'</dt><dd>List of
%% <a href="https://erlang.org/doc/man/gen_tcp.html">gen_tcp options</a> used
%% when connecting the socket; default is `?SOCKET_OPTS'</dd>
%% <dt>`{tls, TlsOpts}'</dt><dd>Enabling TLS and a list of
%% <a href="https://erlang.org/doc/man/ssl.html">ssl options</a>; used when
%% establishing a TLS connection; default is off</dd>
%% <dt>`{name, Name}'</dt><dd>Tuple to register the client with a name
%% such as `{local, atom()}'; for all options see `ServerName' at
%% <a href="https://erlang.org/doc/man/gen_server.html#start_link-4">gen_server:start_link/4</a>;
%% default: no name</dd>
%% </dl>
-spec start_link(ClusterName, InitNodes, Options) -> {ok, pid()} when
    ClusterName :: atom(),
    InitNodes :: list(InitNode),
    InitNode :: {Host, Port},
    Host :: string(),
    Port :: integer(),
    Options :: proplists:proplist().
start_link(ClusterName, InitNodes, Options)
        when is_atom(ClusterName), is_list(InitNodes), is_list(Options), length(InitNodes) > 0 ->
    ecredis_server:start_link(ClusterName, InitNodes, Options);
start_link(ClusterName, _, _) when not is_atom(ClusterName) ->
    error({badarg, ClusterName, "must be atom"});
start_link(_, InitNodes, _) when not is_list(InitNodes) ->
    error({badarg, InitNodes, "must be list"});
start_link(_, InitNodes, _) when is_list(InitNodes), length(InitNodes) =:= 0 ->
    error({badarg, InitNodes, "must not be empty"});
start_link(_, _, Options) when not is_list(Options) ->
    error({badarg, Options, "must be list"});
start_link(_, _, _) ->
    error(badarg).


% TODO: this API is deprecated, use start_link/2
-spec start_link({ClusterName :: atom(), InitNodes :: [{}]}) -> {ok, pid()}.
start_link({ClusterName, InitNodes}) ->
    start_link(ClusterName, InitNodes).


-spec stop(ClusterName :: atom() | pid()) -> ok.
stop(ClusterName) when is_atom(ClusterName); is_pid(ClusterName) ->
    ecredis_server:stop(ClusterName).


-spec q(ClusterName :: atom(), Command :: redis_command()) -> redis_result().
q(ClusterName, Command) ->
    Query = #query{
        query_type = q,
        cluster_name = ClusterName,
        command = Command,
        indices = [1]
    },
    case query_by_command(Query) of
        {ok, Response} ->
            Response;
        {error, Reason} ->
            Reason
    end.


-spec qp(ClusterName :: atom(), Commands :: redis_pipeline_command()) -> redis_pipeline_result().
qp(ClusterName, Commands) ->
    Query = #query{
        query_type = qp,
        cluster_name = ClusterName,
        command = Commands,
        indices = lists:seq(1, length(Commands))
    },
    case query_by_command(Query) of
        {ok, Response} ->
            Response;
        {error, Reason} ->
            Reason
    end.

%% @doc Run command on each master node of the redis cluster.
%% @end
-spec qa(ClusterName :: atom(), Command :: redis_command()) -> [redis_result()].
qa(ClusterName, Command) ->
    Nodes = get_nodes(ClusterName),
    Res = [qn(ClusterName, Node, Command) || Node <- Nodes],
    % make sure list of nodes hasn't changed
    NewNodesSet = sets:from_list(get_nodes(ClusterName)),
    InitialNodesSet = sets:from_list(Nodes),
    case NewNodesSet of
        InitialNodesSet -> Res;
        _ -> 
            Added = sets:to_list(
                sets:subtract(NewNodesSet, InitialNodesSet)),
            Removed = sets:to_list(
                sets:subtract(InitialNodesSet, NewNodesSet)),
            error_logger:error_msg("Unable to execute ~p on ~p - nodes ~p added, ~p removed~n", [Command, ClusterName, Added, Removed]),
            {error, changed_node_list_try_again}
    end.

%% @doc Perform flushdb command on each node of the redis cluster.
%% @end
-spec flushdb(ClusterName :: atom()) -> ok | {error, Reason::bitstring()}.
flushdb(ClusterName) ->
    Result = qa(ClusterName, ["FLUSHDB"]),
    case proplists:lookup(error, Result) of
        none ->
            ok;
        Error ->
            Error
    end.

%% @doc Return a list of master nodes.
%% TODO(shashank): modify this to use the ETS table instead of gen_server state
-spec get_nodes(ClusterName :: atom()) -> [rnode()].
get_nodes(ClusterName) ->
    ecredis_server:get_node_list(ClusterName).

%% @doc Execute the given command at the provided node.
-spec qn(ClusterName :: atom(), Node :: rnode(), Command :: redis_command()) -> redis_result().
qn(ClusterName, Node, Command) ->
    % We use an existing connection to this Node.
    case ecredis_server:get_eredis_pid_by_node(ClusterName, Node) of
        {ok, Pid} ->
            Query = #query{
                query_type = qn,
                cluster_name = ClusterName,
                command = Command,
                indices = [1],
                pid = Pid,
                node = Node,
                retries = 0,
                version = 0 % FIXME: we should have a version here also
            },
            Res = execute_query(Query),
            Res#query.response;
        {error, missing} ->
            {error, {not_connected, ClusterName, Node}}
    end.


%%% @doc Execute a multi-node query. Groups commands by destination node.
-spec qmn(ClusterName :: atom(), Commands :: redis_pipeline_command()) -> redis_pipeline_result().
qmn(ClusterName, Commands) ->
    Queries = group_commands(Commands, ClusterName), % list of queries, one per PID
    Res = lists:map(fun(A) ->
        case A#query.pid of
            undefined -> A;
            _ -> execute_query(A)
        end
        end, Queries),
    {_, Result} = merge_responses(Res),
    Result.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% INTERNAL FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% @doc return a list of qmn queries, one for each destination node
-spec group_commands(Commands :: redis_command(), ClusterName :: atom()) -> list().
group_commands(Commands, ClusterName) ->
    IndexedCommands = lists:zip(lists:seq(1, length(Commands)), Commands),
    Queries = lists:map(fun({Index, Command}) -> get_query(Index, Command, ClusterName) end, IndexedCommands),
    group_by_destination(Queries).

get_query(Index, Command, ClusterName) ->
    Dest = get_destination(ClusterName, Command),
    case Dest of 
        {error, Reason} ->
            make_query(ClusterName, qmn, Command, Reason, undefined, Index);
        {ok, _Slot, Pid, _Version} ->
            make_query(ClusterName, qmn, Command, undefined, Pid, Index)
    end.

make_query(ClusterName, QueryType, Command, Response, Pid, Index) ->
    #query{
        cluster_name = ClusterName,
        query_type = QueryType,
        command = Command,
        response = Response,
        pid = Pid,
        indices = [Index],
        retries = 0
    }.

%%% @doc finds the key and then slot, and finally the PID of the node
%%% corresponding to Command.
get_destination(ClusterName, Command) ->
    case ecredis_command_parser:get_key_from_command(Command) of 
        undefined ->
            ecredis_logger:log_error("Unable to execute - invalid cluster key", Command),
            {error, {invalid_cluster_key, Command}};
        Key ->
            Slot = ecredis_command_parser:get_key_slot(Key),
            {Pid, Version} = ecredis_server:get_eredis_pid_by_slot(ClusterName, Slot),
            {ok, Slot, Pid, Version}
    end.


%% @doc Use the command of the given query to determine which slot the command
%% should be sent to, then update the query config and query by slot
-spec query_by_command(Query :: #query{}) -> {ok, redis_result()} | {error, term()}.
query_by_command(#query{command = Command} = Query) ->
    case ecredis_command_parser:get_key_from_command(Command) of
        undefined ->
            ecredis_logger:log_error("Unable to execute - invalid cluster key", Query),
            {error, {invalid_cluster_key, Command}};
        Key ->
            Slot = ecredis_command_parser:get_key_slot(Key),
            CommandType = ecredis_command_parser:get_command_type(Command),
            NewQuery = Query#query{command_type = CommandType, slot = Slot, version = 0, retries = 0},
            case query_by_slot(NewQuery) of
                #query{response = Response} ->
                    {ok, Response};
                Err ->
                    {error, Err}
            end
    end.


%% @doc Use the slot of the given query to determine where to send the command,
%% then update the query config and execute the query
-spec query_by_slot(Query :: #query{}) -> #query{}.
query_by_slot(#query{retries = Retries} = Query) when Retries >= ?REDIS_CLUSTER_REQUEST_TTL ->
    % Recursion depth is reached - return the most recent error
    ecredis_logger:log_error("Max retries reached", Query),
    Query;
query_by_slot(#query{command = Command, retries = Retries} = Query) ->
    throttle_retries(Retries, Query),
    case get_pid_and_map_version(Query) of
        undefined ->
            ecredis_logger:log_error("Unable to execute - slot has no connection", Query),
            % Slot was not mapped to any pid - remap the cluster and try again
            % FIXME: I don't see how the remap will work because the version in the query is not set
            {ok, NewVersion} = remap_cluster(Query),
            query_by_slot(Query#query{
                response = {error, no_connection, Command},
                retries = Retries + 1,
                version = NewVersion
            });
        {Pid, Version} ->
            execute_query(Query#query{pid = Pid, version = Version})
    end.


%% @doc Execute the given query. Separate out the successful commands and retry
%% any commands that fail. If the recursion depth is reached, just return the error.
-spec execute_query(#query{}) -> #query{}.
execute_query(#query{retries = Retries} = Query) when Retries >= ?REDIS_CLUSTER_REQUEST_TTL ->
    % Recursion depth is reached - return the most recent error
    ecredis_logger:log_error("Max retries reached", Query),
    Query;
execute_query(#query{command = Command, retries = Retries, pid = Pid} = Query) ->
    throttle_retries(Retries, Query),
    % run the query in eredis
    Result = eredis_query(Pid, Command),
    NewQuery = filter_out_asking_result(Query#query{response = Result}),
    case get_successes_and_retries(NewQuery) of
        {_Successes, []} ->
            % All commands were successful - return the query as is
            NewQuery;
        {Successes, QueriesToResend} ->
            case check_sanity_of_keys(NewQuery) of
                ok ->
                    % Reexecute all queries that failed
                    [ecredis_logger:log_error("Retrying query: ", Q) || Q <- QueriesToResend],
                    NewSuccesses = [execute_query(Q) || Q <- QueriesToResend],
                    % Put the original successes and new successes back in order.
                    % The merging logic is primarily intended for qmn, as qp redirects
                    % will always be pipelined in one command. Redirects are very 
                    % uncommon, so it's simpler to keep the code common between qp and qmn
                    % even if it can be more efficiently done for qp
                    {Indices, Response} = merge_responses(NewSuccesses ++ Successes),
                    % Update the query config with the full, ordered set of responses
                    Query#query{indices = Indices, response = Response};
                error ->
                    ecredis_logger:log_error("All keys in pipeline command are not mapped to the same slot", Query),
                    NewQuery
            end
    end.


%% @doc Separates successful commands form those that need to be retried. If the
%% command got a redirect error, make a new query config with the updated pid
-spec get_successes_and_retries(#query{}) -> {[#query{}], [#query{}]}.
get_successes_and_retries(#query{response = {ok, _}} = Query) ->
    % The query was successful - add the query to the successes list
    {[Query], []};
get_successes_and_retries(#query{response = {error, <<"ERR no such key">>}} = Query) ->
    % The query failed and there is no reason to try it again.
    {[Query], []};
get_successes_and_retries(#query{
        response = {error, <<"MOVED ", _>>}, query_type = qn} = Query) ->
    % We don't retry MOVED errors on qn queries
    {[Query], []};
get_successes_and_retries(#query{
        response = {error, <<"ASK ", _>>}, query_type = qn} = Query) ->
    % We don't retry ASK errors on qn queries
    {[Query], []};
get_successes_and_retries(#query{
        response = {error, <<"MOVED ", Dest/binary>>}} = Query) ->
    handle_moved(Query, Dest);
get_successes_and_retries(#query{
        response = {error, <<"ASK ", Dest/binary>>}} = Query) ->
    handle_ask(Query, Dest);
get_successes_and_retries(#query{response = {error, Reason}, query_type = qn,
        cluster_name = ClusterName, node = Node, retries = Retries} = Query) ->
    {ok, NewVersion} = remap_cluster(Query),
    {ok, Pid} = ecredis_server:lookup_eredis_pid(ClusterName, Node),
    ecredis_logger:log_error("Error ~p in query ~p~n", [Reason, Query]),
    {[], [Query#query{retries = Retries + 1, pid = Pid, version = NewVersion}]};
get_successes_and_retries(#query{response = {error, <<"TRYAGAIN">>}, retries = Retries} = Query) ->
    % We get TRYAGAIN error if this query is for 2 or more keys on a slot that is being migrated,
    % and the keys are currently not in the same node.
    ecredis_logger:log_error("TRYAGAIN error", Query),
    {[], [Query#query{retries = Retries + 1}]};
get_successes_and_retries(#query{response = {error, no_connection}} = Query) ->
    handle_error(no_connection, Query);
get_successes_and_retries(#query{response = {error, <<"CLUSTERDOWN Hash slot not served">>}} = Query) ->
    handle_error(cluster_down, Query);
get_successes_and_retries(#query{response = {error, _}, retries = Retries} = Query) ->
    % TODO fill in handlers for other errors, as for when to retry or when to not
    % - tcp_closed?
    ecredis_logger:log_error("Other error", Query),
    {[], [Query#query{retries = Retries + 1}]};
get_successes_and_retries(#query{
        command = _Command,
        command_type = multi,
        response = Responses} = Query) when is_list(Responses) ->
    LastResponse = lists:last(Responses),
    MultiRes = case LastResponse of
        {ok, _} ->
            {[Query], []};
        {error, <<"EXECABORT ", _/binary>>} ->
            ?assert(length(Responses) > 1),
            %% multi command should behave similar to qp for accessing the error, except it should
            %% fail or succeed as a unit.
            %% e.g.
            %% [["MULTI"],["GET","{key1}:a"],["GET","{key1}:b"]["EXEC"]]
            %% -> [{ok, <<"OK">>}, {ok, <<"QUEUED">>}, {ok, <<"QUEUED">>}, {ok, [<<"1">>, <<"2">>]}]
            %% On failure you might get something like:
            %% [{ok, <<"OK">>}, {error, <<"ASK 9189 127.0.0.1:30005">>}, {ok, <<"QUEUED">>},
            %% {error, <<"EXECABORT Transaction discarded because of previous errors.">>}]
            EarlierResponses = lists:sublist(Responses, length(Responses) - 1),
            FirstError = lists:foldr(
                fun
                    (_Response, {error, _} = Err) ->
                        Err;
                    ({error, _} = Err, undefined) ->
                        Err;
                    (_, undefined) ->
                        undefined
                end,
                undefined,
                EarlierResponses),

            case FirstError of
                {error, <<"MOVED ", Dest/binary>>} ->
                    handle_moved(Query, Dest);
                {error, <<"ASK ", Dest/binary>>} ->
                    % [["MULTI"],["GET","{key1}:a"],["EXEC"]] should be converted into
                    % [["ASKING"],["MULTI"],["GET","{key1}:a"],["EXEC"]]
                    handle_ask(Query, Dest);
                {error, _} ->
                    % Unknown error, return as is.
                    ecredis_logger:log_warning("Unknown error", Query),
                    {[Query], []};
                undefined ->
                    % This is not expected, It looks like we could not find the error.
                    % but the transaction was aborted because of an error.
                    ecredis_logger:log_warning("Unknown error", Query),
                    {[Query], []}
            end
    end,
    MultiRes; 
get_successes_and_retries(#query{
        command = Commands,
        response = Responses,
        indices = Indices} = Query) when is_list(Responses) ->
    % Check each command in a pipeline individually for errors, then aggregate
    % the lists of successes and retries
    IndexCommandResponseList = lists:zip3(Indices, Commands, Responses),
    % Separate the pipeline into individual commands
    %
    % TODO(vipin): Need to capture the cluster version.
    PossibleRetries = [Query#query{
        command = Command,
        response = Response,
        indices = [Index]} || {Index, Command, Response} <- IndexCommandResponseList],
    {Successes, NeedToRetries} = lists:unzip([get_successes_and_retries(Q) || Q <- PossibleRetries]),
    {lists:flatten(Successes), group_by_destination(lists:flatten(NeedToRetries))}.


-spec handle_moved(#query{}, binary()) -> {[#query{}], [#query{}]}.
handle_moved(#query{retries = Retries} = Query, Dest) ->
    % The command was sent to the wrong node - refresh the mapping, update
    % the query to reflect the new pid, and add the query to the retries list
    ecredis_logger:log_warning("MOVED", Query),
    case handle_redirect(Query, moved, Dest) of
        {ok, Slot, Pid, NewVersion} ->
            {[], [Query#query{
                slot = Slot,
                pid = Pid,
                retries = Retries + 1,
                version = NewVersion
            }]};
        {error, _Reason} ->
            % Unable to connect to the redirect destination. Return the error as-is
            {[Query], []}
    end.


-spec handle_ask(#query{}, binary()) -> {[#query{}], [#query{}]}.
handle_ask(#query{command = Command, retries = Retries,
        command_type = CommandType} = Query, Dest) ->
    % The command's slot is in the process of migration - upate the query to reflect
    % the new pid, prepend the ASKING command, and add the query to the retries list
    ecredis_logger:log_warning("ASK", Query),
    case handle_redirect(Query, ask, Dest) of
        {ok, Slot, Pid, Version} ->
            Command2 = case CommandType of
                multi -> [["ASKING"] | Command];
                _ -> [["ASKING"], Command]
            end,
            ?WARNING("Command (~p) ~p -> ~p", [Query#query.command_type, Command, Command2]),
            {[], [Query#query{
                command = Command2,
                slot = Slot,
                pid = Pid,
                retries = Retries + 1,
                version = Version
            }]};
        undefined ->
            % Unable to connect to the redirect destination. Return the error as-is
            {[Query], []}
    end.

-spec handle_error(Error :: atom(), Query :: #query{}) -> {[#query{}], [#query{}]}.
handle_error(Error,
        #query{cluster_name = ClusterName, slot = Slot, retries = Retries, pid = OldPid} = Query) ->
    {ok, RemapNewVersion} = remap_cluster(Query),
    case get_pid_and_map_version(Query) of
        {Pid, NewVersion} ->
            ?ERROR("Error ~p ~p Slot: ~p Pid ~p -> ~p in query ~p~n",
                [Error, ClusterName, Slot, OldPid, Pid, Query]),
            {[], [Query#query{retries = Retries + 1, pid = Pid, version = NewVersion}]};
        undefined ->
            ?ERROR("Error ~p ~p no pid for query after remap ~p~n",
                [Error, ClusterName, Query]),
            {[], [Query#query{retries = Retries + 1, version = RemapNewVersion}]}
    end.

%% @doc Merge a list of queries into a list of queries where each destination is
%% unique. If two queries in the original list have the same destination, they become
%% a pipeline command, where order is preserved based on the querys' indices
group_by_destination(Queries) ->
    maps:values(lists:foldr(fun group_by_destination/2, maps:new(), Queries)).


%% @doc Add a query to a map. If a query to the same destination already exists,
%% add the command, response, and index to the query.  
-spec group_by_destination(#query{}, map()) -> map().
group_by_destination(#query{command = [["ASKING"], Command],
        response = Response, pid = Pid, indices = [Index]} = Query, GroupedCommands) ->
    case maps:get(Pid, GroupedCommands, undefined) of
        #query{command = Commands, response = Responses, indices = Indices} ->
            NewQuery = Query#query{
                command = [["ASKING"], Command | Commands],
                response = [Response | Responses],
                indices = [Index | Indices]
            },
            maps:update(Pid, NewQuery, GroupedCommands);
        undefined ->
            NewQuery = Query#query{
                command = [["ASKING"], Command],
                response = [Response]
            },
            maps:put(Pid, NewQuery, GroupedCommands)
    end;
group_by_destination(#query{command = Command,
        response = Response, pid = Pid, indices = [Index]} = Query, GroupedCommands) ->
    case maps:get(Pid, GroupedCommands, undefined) of
        #query{command = Commands, response = Responses, indices = Indices} ->
            NewQuery = Query#query{
                command = [Command | Commands],
                response = [Response | Responses],
                indices = [Index | Indices]
            },
            maps:update(Pid, NewQuery, GroupedCommands);
        undefined ->
            NewQuery = Query#query{
                command = [Command],
                response = [Response]
            },
            maps:put(Pid, NewQuery, GroupedCommands)
    end.


%% @doc Send the command to the given redis node 
-spec eredis_query(pid(), redis_command()) -> redis_result().
eredis_query(Pid, [[X|_]|_] = Commands) when is_list(X); is_binary(X) ->
    eredis:qp(Pid, Commands);
eredis_query(Pid, Command) ->
    eredis:q(Pid, Command).


%% @doc If the command is being retried, sleep the process for a little bit to
%% allow for remapping to occur
-spec throttle_retries(integer(), any()) -> ok.
throttle_retries(0, _Query) ->
    ok;
throttle_retries(N, Query) ->
    % exponential backoff up to REDIS_MAX_RETRY_DELAY
    Delay = round(?REDIS_INIT_RETRY_DELAY * math:pow(2, N - 1)),
    ?DEBUG("retrying after ~pms ~p", [Delay, Query#query.command]),
    timer:sleep(erlang:min(Delay, ?REDIS_MAX_RETRY_DELAY)).


%% @doc Get the pid associated with the given destination. If we don't have existing connection,
%% we will call add_node to start a new connection.
-spec handle_redirect(Query :: #query{}, RedirectType :: moved | ask, Dest :: binary())
            -> {ok, Slot :: integer(), Pid :: pid(), Version :: integer()} | {error, Reason :: any()}.
handle_redirect(#query{cluster_name = ClusterName, version = Version}, RedirectType, Dest) ->
    [SlotBin, AddrPort] = binary:split(Dest, <<" ">>),
    [Address, Port] = binary:split(AddrPort, <<":">>),
    Node = #node{address = binary_to_list(Address), port = binary_to_integer(Port)},
    Slot = binary_to_integer(SlotBin),
    % lookup the pid of node we are redirected to
    Result = case ecredis_server:get_eredis_pid_by_node(ClusterName, Node) of
        {ok, Pid} ->
            % if we have a connection just follow the redirect
            {ok, Slot, Pid, Version};
        {error, missing} ->
            % if we are missing a connection we need to ask the ecredis_server to add a connection.
            % this is gen_server call, adding the node happens on the ecredis_server process
            case ecredis_server:add_node(ClusterName, Node) of
                {ok, Pid} ->
                    % TODO: maybe in the future the version should increment when a new node is added
                    {ok, Slot, Pid, Version};
                {error, _Reason} = Err ->
                    Err
            end
    end,
    % we notify the ecredis_server process about the redirect. This will update the slot map
    % for this slot only and also schedule full remap
    case RedirectType of
        moved -> ecredis_server:handle_moved_async(ClusterName, Version, Slot, Node);
        ask -> ok
    end,

    Result.


%% @doc Use the indices list from a query to tag each of the responses.
-spec index_responses(#query{}) -> [{integer(), redis_result()}].
index_responses(#query{response = Responses, indices = Indices}) when is_list(Responses) ->
    lists:zip(Indices, Responses);
index_responses(#query{response = Response, indices = [Index]}) ->
    [{Index, Response}].


%% @doc Merge the responses of all of the queries based on the indices of the
%% resonses. Used to re-order the responses if some had to be resent due to errors
-spec merge_responses([[#query{}]]) -> {[integer()], redis_result()}.
merge_responses([#query{query_type = q, response = Response, indices = [Index]}]) ->
    {[Index], Response};
merge_responses(QueryList) ->
    IndexedResponses = lists:map(fun index_responses/1, QueryList),
    lists:unzip(lists:merge(IndexedResponses)).


%% @doc When a query receives an ASK response, we prepend the ASKING command
%% onto that query to allow the query to be serviced. An ASKING command receives
%% <<"OK">> from redis. But, the client didn't send these commands, so we need to
%% remove these responses so they don't get returned to the client. 
-spec filter_out_asking_result(#query{}) -> #query{}.
filter_out_asking_result(#query{query_type = q,
        command = [["ASKING"], Command],
        response = [_AskResponse, Response]} = Query) ->
    Query#query{command = Command, response = Response};
filter_out_asking_result(#query{command_type = multi,
        command = [["ASKING"] | Command],
        response = [_AskResponse | Response]} = Query) ->
    Query#query{command = Command, response = Response};
filter_out_asking_result(#query{query_type = QueryType,
        command = Commands, response = Responses} = Query)
        when is_list(Commands), is_list(Responses), QueryType =:= qmn orelse QueryType =:= qp ->
    {FilteredCommands, FilteredResponses} = lists:unzip(lists:filter(fun
        ({["ASKING"], {ok, <<"OK">>}}) -> 
            false;
        (_) ->
            true
        end, lists:zip(Commands, Responses))),
    Query#query{command = FilteredCommands, response = FilteredResponses};
filter_out_asking_result(Query) ->
    % Single command
    Query.


%% @doc This is just a wrapper to allow for a cleaner interface above :)
-spec remap_cluster(#query{}) -> {ok, integer()}.
remap_cluster(#query{cluster_name = ClusterName, version = Version}) ->
    ecredis_server:remap_cluster(ClusterName, Version).


%% @doc This is just a wrapper to allow for a cleaner interface above :)
-spec get_pid_and_map_version(#query{}) -> {pid(), integer()} | undefined.
get_pid_and_map_version(#query{cluster_name = ClusterName, slot = Slot}) ->
    ecredis_server:get_eredis_pid_by_slot(ClusterName, Slot).


-spec check_sanity_of_keys(#query{}) -> ok | error.
check_sanity_of_keys(#query{query_type = qp, command = Commands}) ->
    ecredis_command_parser:check_sanity_of_keys(Commands);
check_sanity_of_keys(_Query) ->
    ok.

% check_for_moved_errors([{error, <<"MOVED ", _/binary>>} | _Rest]) ->
%     true;
% check_for_moved_errors([_ | Rest]) ->
%     check_for_moved_errors(Rest);
% check_for_moved_errors(_) ->
%     false.
