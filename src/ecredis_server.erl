-module(ecredis_server).
-behaviour(gen_server).

%% To be used to name the ets tables.
-define(NODE_PIDS, node_pids).
-define(SLOT_PIDS, slot_pids).

%% API.
-export([
    get_eredis_pid_by_slot/2,
    lookup_eredis_pid/2,
    remap_cluster/2,
    get_node_list/1,
    lookup_address_info/2  %% Used for tests only.
]).


%% Callbacks for gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("ecredis.hrl").

%% Type definition.
-record(state, {
    cluster_name :: string(),
    init_nodes :: [#node{}],
    node_list :: [#node{}],
    version :: integer()  %% Used to avoid unnecessary refresh of Redis slots.
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_eredis_pid_by_slot(ClusterName :: atom(), Slot :: integer()) -> 
    {Pid :: pid(), Version :: integer()} | undefined.
get_eredis_pid_by_slot(ClusterName, Slot) ->
    Result = ets:lookup(ets_table_name(ClusterName, ?SLOT_PIDS), Slot),
    case Result of
        [] -> undefined;
        [{_, Result2}] -> Result2
    end. 


-spec remap_cluster(ClusterName :: atom(), Version :: integer()) -> {ok, Version :: integer()}.
remap_cluster(ClusterName, Version) ->
    gen_server:call(ClusterName, {remap_cluster, Version}).


-spec lookup_address_info(ClusterName :: atom(), Pid :: pid()) -> [[term()]]. 
lookup_address_info(ClusterName, Pid) ->
    ets:match(ets_table_name(ClusterName, ?NODE_PIDS), {'$1', Pid}).


get_node_list(ClusterName) ->
    gen_server:call(ClusterName, get_nodes).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec remap_cluster_internal(State :: #state{}, Version :: integer()) -> State :: #state{}.
remap_cluster_internal(State, Version) ->
    if
       State#state.version == Version ->
           reload_slots_map(State);
       true ->
           State
    end.


-spec connect_all_nodes(State :: #state{}, InitNodes :: [#node{}]) -> #state{}.
connect_all_nodes(_State, []) ->
    #state{};
connect_all_nodes(State, InitNodes) ->
    NewState = State#state{
        init_nodes = InitNodes,
        version = 0
    },
    reload_slots_map(NewState).


%% TODO(vipin): Need to reset connection on Redis node removal.
-spec reload_slots_map(State :: #state{}) -> State :: #state{}.
reload_slots_map(State) ->
    ClusterSlots = get_cluster_slots(State#state.cluster_name, State#state.init_nodes),
    SlotsMaps = parse_cluster_slots(ClusterSlots),
    NewState = connect_all_slots(State, SlotsMaps),
    create_eredis_pids_cache(NewState, SlotsMaps),
    NodeList = [SlotsMap#slots_map.node || SlotsMap <- SlotsMaps],
    NewState#state{node_list = NodeList}.


-spec get_cluster_slots(ClusterName :: atom(), InitNodes :: [#node{}]) -> 
    [[binary() | [binary()]]].
get_cluster_slots(_ClusterName, []) ->
    throw({error, cannot_connect_to_cluster});
get_cluster_slots(ClusterName, [Node|T]) ->
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
                get_cluster_slots(ClusterName, T)
          end;
        _ ->
            get_cluster_slots(ClusterName, T)
  end.


-spec get_cluster_slots_from_single_node(#node{}) -> [[binary() | [binary()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].


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
    State#state{
        version = State#state.version + 1
    }.


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


-spec cache_eredis_pids(State :: #state{}, SlotsCache :: [integer()], 
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
    Res = ets:lookup(ets_table_name(ClusterName, ?NODE_PIDS),
                     [Node#node.address, Node#node.port]),  
    case Res of
        [] ->
           Result = safe_eredis_start_link(Node#node.address, Node#node.port),
           case Result of
               {ok, Pid} ->
                   ets:insert(ets_table_name(ClusterName, ?NODE_PIDS),
                              {[Node#node.address, Node#node.port], Pid}),
                   {ok, Pid};
               {error, Reason} ->
                   error_logger:error_msg("Unable to connect with Redis Node: ~p:~p, Reason: ~p~n",
                                          [Node#node.address, Node#node.port, Reason]),
                   {error, Reason}
            end;
        [{_, Pid}] -> {ok, Pid}
    end.


ets_table_name(ClusterName, Purpose) ->
    list_to_atom(atom_to_list(ClusterName) ++ atom_to_list(Purpose) ++ atom_to_list(?MODULE)).


safe_eredis_start_link(Ip, Port) ->
    %% eredis client's gen_server terminates if it is unable to connect with redis. We trap the
    %% exit signal just so ecredis process also does not terminate. Returned error needs to be
    %% handled by the caller.
    %% Refer: https://medium.com/erlang-battleground/the-unstoppable-exception-9dfb009852f5
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Ip, Port),
    process_flag(trap_exit, false),
    Payload.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server call backs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init([ClusterName, InitNodes]) ->
    State = #state{
        cluster_name = ClusterName,
        node_list = []
    },
    create_ets_tables(ClusterName),
    InitNodes2 = [#node{address = Address, port = Port} || {Address, Port} <- InitNodes],
    {ok, connect_all_nodes(State, InitNodes2)}.


handle_call({remap_cluster, Version}, _From, State) ->
    NewState = remap_cluster_internal(State, Version),
    {reply, {ok, NewState#state.version}, NewState};
handle_call(get_nodes, _From, State) ->
    {reply, State#state.node_list, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
