-module(ecredis_server).
-behaviour(gen_server).

-include("logger.hrl").

%% To be used to name the ets tables.
-define(NODE_PIDS, node_pids).
-define(SLOT_PIDS, slot_pids).

%% API.
-export([
    start_link/2,
    stop/1,
    get_eredis_pid_by_slot/2,
    get_eredis_pid_by_node/2,
    remap_cluster/2,
    handle_moved_async/4,
    get_node_list/1,
    add_node/2,
    lookup_address_info/2  %% Used for tests only.
]).


%% Callbacks for gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("ecredis.hrl").

%% Type definition.
-record(state, {
    cluster_name :: atom(),
    init_nodes = [] :: [#node{}],
    node_list = [] :: [#node{}],
    version = 0 :: integer()  %% Used to avoid unnecessary refresh of Redis slots.
}).

-type state() :: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link(ClusterName, InitNodes) -> {ok, pid()} when
        ClusterName :: atom(),
        InitNodes :: list(InitNode),
        InitNode :: {Host, Port},
        Host :: string(),
        Port :: integer().
start_link(ClusterName, InitNodes) ->
    gen_server:start_link({local, ClusterName}, ?MODULE, [ClusterName, InitNodes], []).


-spec stop(ClusterName :: atom() | pid()) -> ok.
stop(ClusterName) when is_atom(ClusterName); is_pid(ClusterName) ->
    gen_server:stop(ClusterName).


% TODO: it would be better if this API returns {ok, Pid, Version} | {error, missing}
-spec get_eredis_pid_by_slot(ClusterName :: atom(), Slot :: integer()) ->
    {Pid :: pid(), Version :: integer()} | undefined | no_return().
get_eredis_pid_by_slot(ClusterName, Slot) ->
    try
        TName = ets_table_name(ClusterName, ?SLOT_PIDS),
        Result = ets:lookup(TName, Slot),
        case Result of
            [] -> undefined;
            [{Slot, {Pid, Version}}] -> {Pid, Version}
        end
    catch
        error : badarg ->
            erlang:error({ecredis_invalid_client, ClusterName})
    end.

-spec get_eredis_pid_by_node(ClusterName :: atom(), Node :: #node{}) ->
    {ok, Pid :: pid()} | {error, missing} | no_return().
get_eredis_pid_by_node(ClusterName, Node) ->
    try
        case ets:lookup(ets_table_name(ClusterName, ?NODE_PIDS),
                [Node#node.address, Node#node.port]) of
            [] -> {error, missing};
            [{_, Pid}] -> {ok, Pid}
        end
    catch
        error : badarg ->
            erlang:error({ecredis_wrong_client, ClusterName})
    end.


-spec remap_cluster(ClusterName :: atom(), Version :: integer()) -> {ok, Version :: integer()}.
remap_cluster(ClusterName, Version) ->
    gen_server:call(ClusterName, {remap_cluster, Version}).

-spec handle_moved_async(ClusterName :: atom(), Version :: integer(),
        Slot :: integer(), Node :: node())
            -> {ok, Pid :: pid(), Version :: integer()}.
handle_moved_async(ClusterName, Version, Slot, Node) ->
    gen_server:cast(ClusterName, {handle_moved, Version, Slot, Node}).

-spec get_node_list(ClusterName :: atom()) -> [#node{}].
get_node_list(ClusterName) ->
    gen_server:call(ClusterName, get_nodes).

% Add a new node to this cluster usually after a MOVED/ASK error. We check if we don't already
% have a connection, and if not we create a new eredis client and store it in our ets tables.
-spec add_node(ClusterName :: atom(), Node :: node()) -> {ok, pid()} | {error, any()}.
add_node(ClusterName, Node) ->
    gen_server:call(ClusterName, {add_node, Node}).

% FIXME: this function return value is not good. It should be {Host, Port} | {undefined, undefined}
-spec lookup_address_info(ClusterName :: atom(), Pid :: pid()) -> [[term()]].
lookup_address_info(ClusterName, Pid) ->
    ets:match(ets_table_name(ClusterName, ?NODE_PIDS), {'$1', Pid}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec remap_cluster_internal(State :: #state{}, Version :: integer()) -> State :: #state{}.
remap_cluster_internal(State, Version) ->
    if
        % Version should always be <= the the State#state.version. Its safer to refresh on =<.
        State#state.version == Version ->
            reload_slots_map(State);
        State#state.version < Version ->
            error_logger:info_msg("Cluster: ~p Version: ~p > state.version ~p",
                [State#state.cluster_name, Version, State#state.version]),
            reload_slots_map(State);
        true ->
            State
    end.

-spec handle_moved_internal(State :: state(), Version :: integer(),
        Slot :: integer(), Node :: #node{}) -> state().
handle_moved_internal(State, Version, Slot, Node) ->
    % moved replied imply the move is permanent. Get connection to the new node and update
    % the slot map for just this node.
    ClusterName = State#state.cluster_name,
    % find existing connection or connect to Node
    case lookup_eredis_pid(ClusterName, Node) of
        {ok, Pid} ->
            {CurPid, SlotVersion} = get_eredis_pid_by_slot(ClusterName, Slot),
            case {Version >= SlotVersion, CurPid =:= Pid} of
                {true, true} -> State;
                {true, false} ->
                    ets:insert(ets_table_name(ClusterName, ?SLOT_PIDS),
                        {Slot, {Pid, SlotVersion}}),
                    OldNode = lookup_address_info(ClusterName, CurPid),
                    NewNode = lookup_address_info(ClusterName, Pid),
                    error_logger:info_msg("Cluster: ~p Slot: ~p MOVED ~p:~p -> ~p:~p",
                        [ClusterName, Slot, CurPid, OldNode, Pid, NewNode]),
                    % Schedule async full cluster remap. The idea is to be proactive after one move,
                    % it is likely there are more.
                    gen_server:cast(ClusterName, {remap_cluster, Version}),
                    State;
                {false, _} -> State
            end;
        {error, Reason} ->
            error_logger:error_msg(
                "Cluster: ~p Error connecting to new Node: ~p:~p Slot:~p moved Reason: ~p",
                [ClusterName, Node#node.address, Node#node.port, Slot, Reason]),
            State
    end.


%% TODO(vipin): Need to reset connection on Redis node removal.
-spec reload_slots_map(State :: #state{}) -> State :: #state{}.
reload_slots_map(State) ->
    error_logger:info_msg("ecredis reload_slots_map cluster ~p v: ~p",
        [State#state.cluster_name, State#state.version]),
    ClusterSlots = get_cluster_slots(State#state.cluster_name, State#state.init_nodes),
    SlotsMaps = parse_cluster_slots(ClusterSlots),
    NewState = connect_all_slots(State, SlotsMaps),
    create_eredis_pids_cache(NewState, SlotsMaps),
    NodeList = [SlotsMap#slots_map.node || SlotsMap <- SlotsMaps],
    NewState#state{node_list = NodeList}.


-spec get_cluster_slots(ClusterName :: atom(), InitNodes :: [#node{}]) -> 
    [[binary() | [binary()]]].
get_cluster_slots(_ClusterName, []) ->
    erlang:error(cannot_connect_to_cluster);
get_cluster_slots(ClusterName, [Node | NodesRest]) ->
    Res = lookup_eredis_pid(ClusterName, Node),
    case Res of
        {ok, Pid} ->
          case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
            {error, <<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error, <<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                ClusterInfo;
            _ ->
                get_cluster_slots(ClusterName, NodesRest)
          end;
        _ ->
            get_cluster_slots(ClusterName, NodesRest)
  end.


-spec get_cluster_slots_from_single_node(#node{}) -> [[binary() | [binary()]]].
get_cluster_slots_from_single_node(Node) ->
    [
        [
            <<"0">>,
            integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
            [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]
        ]
    ].


-spec parse_cluster_slots([[binary() | [binary()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
    parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Index, Acc) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Index + 1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).


-spec connect_all_slots(State :: #state{}, [#slots_map{}]) -> #state{}.
connect_all_slots(State, SlotsMapList) ->
    [connect_node(State#state.cluster_name, SlotsMap#slots_map.node) || SlotsMap <- SlotsMapList],
    increment_version(State).


-spec connect_node(ClusterName :: atom(), #node{}) -> ok.
connect_node(ClusterName, Node) ->
    %% Will make an attempt to connect if no connection already present.
    lookup_eredis_pid(ClusterName, Node),
    ok.


%% Creates tuple with one element per Redis slot. Each element maps the Redis slot to its eredis
%% Pid.
-spec create_eredis_pids_cache(State :: #state{}, SlotsMaps :: [#slots_map{}]) -> ok.
create_eredis_pids_cache(State, SlotsMaps) ->
    SlotsCache = [[{Index, SlotsMap#slots_map.index} ||
        Index <- lists:seq(SlotsMap#slots_map.start_slot, SlotsMap#slots_map.end_slot)] ||
        SlotsMap <- SlotsMaps],
    FlatSlotsCache = lists:flatten(SlotsCache),
    SortedSlotsCache = lists:sort(FlatSlotsCache),
    SlotsCache2 = [Index || {_, Index} <- SortedSlotsCache],
    SlotsCacheTuple = list_to_tuple(SlotsCache2),
    SlotsMapsTuple = list_to_tuple(SlotsMaps),
    [cache_eredis_pids(State, SlotsCacheTuple, SlotsMapsTuple, Slot) ||
        Slot <- lists:seq(0, ?REDIS_CLUSTER_HASH_SLOTS - 1)],
    ok.


-spec cache_eredis_pids(State :: state(), SlotsCache :: [integer()],
                        SlotsMaps :: [#slots_map{}], Slot :: integer()) -> ok.
cache_eredis_pids(State, SlotsCache, SlotsMaps, Slot) ->
    RedisNodeIndex = element(Slot + 1, SlotsCache),
    SlotsMap = element(RedisNodeIndex, SlotsMaps),
    Result = lookup_eredis_pid(State#state.cluster_name, SlotsMap#slots_map.node),
    case Result of
        {ok, Pid} ->
            ets:insert(ets_table_name(State#state.cluster_name, ?SLOT_PIDS),
                {Slot, {Pid, State#state.version}});
        {error, _} ->
             %% TODO(vipin): Maybe retry after sometime.
             ok
    end,
    ok.


-spec increment_version(State :: state()) -> state().
increment_version(#state{cluster_name = ClusterName, version = Version} = State) ->
    ets:insert(ets_table_name(ClusterName, ?SLOT_PIDS), {version, Version + 1}),
    State#state{version = Version + 1}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% To manage list of eredis connections
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_ets_tables(ClusterName) ->
    ets:new(ets_table_name(ClusterName, ?NODE_PIDS),
            [protected, set, named_table, {read_concurrency, true}]),
    ets:new(ets_table_name(ClusterName, ?SLOT_PIDS),
            [protected, set, named_table, {read_concurrency, true}]).


%% Looks up existing redis client pid using Ip, Port and return Pid of the client
%% connection. If no such connection is present, this method establishes the connection.
-spec lookup_eredis_pid(ClusterName :: atom(), Node :: #node{}) ->
    {ok, Pid :: pid()} | {error, any()}.
lookup_eredis_pid(ClusterName, Node) ->
    case get_eredis_pid_by_node(ClusterName, Node) of
        {ok, Pid} -> {ok, Pid};
        {error, missing} ->
            case add_node_internal(ClusterName, Node) of
                {ok, Pid} ->
                    {ok, Pid};
                {error, Reason} ->
                    error_logger:error_msg(
                        "Cluster: ~p Unable to connect with Redis Node: ~p:~p, Reason: ~p",
                        [ClusterName, Node#node.address, Node#node.port, Reason]),
                    {error, Reason}
            end
    end.


ets_table_name(ClusterName, Purpose) ->
    list_to_atom(atom_to_list(ClusterName) ++ "." ++ atom_to_list(Purpose)
        ++ "." ++ atom_to_list(?MODULE)).


safe_eredis_start_link(Ip, Port) ->
    %% eredis client's gen_server terminates if it is unable to connect with redis. We trap the
    %% exit signal just so ecredis process also does not terminate. Returned error needs to be
    %% handled by the caller.
    %% Refer: https://medium.com/erlang-battleground/the-unstoppable-exception-9dfb009852f5
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Ip, Port),
    process_flag(trap_exit, false),
    Payload.

% TODO: We need to also add the node to the state.nodes?
%%% Runs only on the cluster PID.
-spec add_node_internal(ClusterName :: atom(), Node :: #node{}) -> {ok, pid()} | {error, any()}.
add_node_internal(ClusterName, Node) ->
    case safe_eredis_start_link(Node#node.address, Node#node.port) of
        {ok, Pid} ->
            ets:insert(ets_table_name(ClusterName, ?NODE_PIDS),
                {[Node#node.address, Node#node.port], Pid}),
            {ok, Pid};
        {error, _Reason} = Err ->
            Err
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server call backs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init([ClusterName, InitNodes]) ->
    create_ets_tables(ClusterName),
    InitNodes2 = [#node{address = Address, port = Port} || {Address, Port} <- InitNodes],

    State = #state{
        cluster_name = ClusterName,
        init_nodes = InitNodes2,
        node_list = [],
        version = 0
    },
    {ok, reload_slots_map(State)}.


handle_call({remap_cluster, Version}, _From, State) ->
    NewState = remap_cluster_internal(State, Version),
    {reply, {ok, NewState#state.version}, NewState};
handle_call({add_node, Node}, _From, State) ->
    ClusterName = State#state.cluster_name,
    {reply, lookup_eredis_pid(ClusterName, Node), State};
handle_call(get_nodes, _From, State) ->
    {reply, State#state.node_list, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({handle_moved, Version, Slot, Node}, State) ->
    State2 = handle_moved_internal(State, Version, Slot, Node),
    {noreply, State2};
handle_cast({remap_cluster, Version}, State) ->
    State2 = remap_cluster_internal(State, Version),
    {noreply, State2};
handle_cast({ping, Ts, Id, From}, State) ->
    From ! {ack, Ts, Id, self()},
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
