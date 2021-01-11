-module(ecredis).

-define(ECREDIS_SERVER, ecredis_server).

%% API.
-export([
    start_link/1,
    q/2,
    qp/2
]).

-include("ecredis.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link({ClusterName :: atom(), InitNodes :: [{}]}) -> {ok, pid()}.
start_link({ClusterName, InitNodes}) ->
    gen_server:start_link({local, ClusterName}, ?ECREDIS_SERVER, [ClusterName, InitNodes], []).


%% Executes q.
-spec q(ClusterName :: atom(), Command :: redis_command()) -> redis_result().
q(ClusterName, Command) ->
    query_by_command(ClusterName, Command).


%% Executes qp.
-spec qp(ClusterName :: atom(), Commands :: redis_pipeline_command()) -> redis_pipeline_result().
qp(ClusterName, Commands) ->
    query_by_command(ClusterName, Commands).
%% TODO(murali@): ensure that these commands contain keys from same slot, else return an error.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% INTERNAL FUNCTIONS
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec query_by_command(ClusterName :: atom(), Command :: redis_command()) -> redis_result().
query_by_command(ClusterName, Command) ->
    Key = ecredis_command_parser:get_key_from_command(Command),
    query_by_key(ClusterName, Command, Key).


-spec query_by_key(ClusterName :: atom(), Command :: redis_command(),
                   Key :: any()) -> redis_result().
query_by_key(_Cluster, Command, undefined) ->
    error_logger:error_msg("Unable to execute: ~p, invalid cluster key~n", [Command]),
    {error, invalid_cluster_key, Command};
query_by_key(ClusterName, Command, Key) ->
    Slot = ecredis_command_parser:get_key_slot(Key),
    query_by_slot(ClusterName, Slot, Command).


-spec query_by_slot(ClusterName :: atom(), Slot :: integer(),
                    Command :: redis_command()) -> redis_result().
query_by_slot(ClusterName, Slot, Command) ->
    case ecredis_server:get_eredis_pid(ClusterName, Slot) of
        undefined ->
            error_logger:error_msg("Unable to execute: ~p, bad client ~n", [ClusterName]),
            {error, bad_client, ClusterName};
        {Node, Version} ->
            query_by_node(ClusterName, Node, Command, Version, 0)
    end.


-spec query_by_node(ClusterName :: atom(), Node :: pid(), Command :: redis_command(),
                    Version :: integer(), Counter :: integer()) -> redis_result().
query_by_node(ClusterName, Node, Command, Version, Counter) ->
    throttle_retries(Counter),
    Response = send_query(Node, Command),
    Result = handle_redirects(ClusterName, Response, Command, Version),
    case handle_result(ClusterName, Result, Version) of
        retry ->
            query_by_node(ClusterName, Node, Command, Version, Counter + 1);
        Result ->
            Result
    end.


-spec throttle_retries(integer()) -> ok.
throttle_retries(0) ->
    ok;
throttle_retries(_) -> 
    timer:sleep(?REDIS_RETRY_DELAY).


send_query(Pid, [[X|_]|_] = Commands) when is_list(X); is_binary(X) ->
    eredis:qp(Pid, Commands);
send_query(Pid, Command) ->
    eredis:q(Pid, Command).


-spec handle_redirects(ClusterName :: atom(), Response :: redis_result(), Command :: redis_command(),
                       Version :: integer()) -> retry | redis_result().
handle_redirects(ClusterName, {error, <<"MOVED ", RedirectInfo/binary>>} = Response,
                 Command, Version) ->
    case parse_redirect_info(RedirectInfo) of
        {ok, Node} ->
            % Our mapping was wrong. Remap the cluster, then send the command
            % to the node redis tells us
            ecredis_server:remap_cluster(ClusterName, Version),
            send_query(Node, Command);
        {error, _Err} ->
            % Response was corrupt, or the node does not exist
            % error_logger:error_msg("MOVED redirect failed: ")
            Response
    end;
handle_redirects(_ClusterName, {error, <<"ASK ", RedirectInfo/binary>>} = Response,
                 Command, _Version) ->
    case parse_redirect_info(RedirectInfo) of
        {ok, Node} ->
            % ASK response says where the slot for this command is served, but
            % we shouldn't remap the cluster as migration is ongoing
            case send_query(Node, [[<<"ASKING">>], Command]) of
                [{ok, <<"OK">>}, NewResponse] ->
                    NewResponse;
                _AskingFailed ->
                    Response
            end;
        {error, _Err} ->
            % error_logger:error_msg("ASK redirect failed: ")
            ok
    end;
handle_redirects(ClusterName, Response, [[X|_]|_] = Command, Vesrion)
                 when is_list(X) orelse is_binary(X), is_list(Response) ->
    % TODO(@ethan): write handler for pipeline commands
    Response.
    

-spec handle_result(ClusterName :: atom(), Result :: redis_result(),
                    Version :: integer()) -> retry | redis_result().
handle_result(ClusterName, {error, no_connection}, Version) ->
    % If we detect a node went down, we should refresh the slot mapping
    % error_logger:warning_msg("no_connection, ~p v: ~p", [ClusterName, Version]),
    {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
    retry;
handle_result(ClusterName, {error, tcp_closed}, Version) ->
    % If the tcp connection is closed (connection timeout), the redis worker
    % will try to reconnect, thus the connection should be recovered for
    % the next request. We don't need to refresh the slot mapping in this
    % case.      
    error_logger:warning_msg("tcp_closed, ~p v: ~p", [ClusterName, Version]),
    retry;
handle_result(ClusterName, {error, <<"MOVED ", _/binary>>}, Version) ->
    % Redis explicitly say our slot mapping is incorrect, we need to refresh it.
    error_logger:warning_msg("moved, ~p v: ~p", [ClusterName, Version]),
    {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
    retry;
handle_result(ClusterName, {error, <<"ASK ", _/binary>>}, Version) ->
    % Migration ongoing
    error_logger:warning_msg("ask, ~p v: ~p", [ClusterName, Version]),
    retry;
handle_result(ClusterName, {error, <<"TRYAGAIN ", _/binary>>}, Version) ->
    % Resharding ongoing, only partial keys exists
    error_logger:warning_msg("tryagain, ~p v: ~p", [ClusterName, Version]),
    retry;
handle_result(ClusterName, {error, <<"CLUSTERDOWN ", _/binary>>}, Version) ->
    % Hash not served, can be triggered temporarily due to resharding
    error_logger:warning_msg("clusterdown, ~p v: ~p", [ClusterName, Version]),
    retry;
handle_result(_ClusterName, Result, _Version) ->
    Result.

    
%% RedirectInfo comes in the form of <<"ASK ADDRESS">> or <<"MOVED ADDRESS">>
-spec parse_redirect_info(RedirectInfo :: binary()) ->
          {ok, Node :: pid()} | {error, any()}.
parse_redirect_info(RedirectInfo) ->
    %% TODO(@ethan): implement this
    ok.





% query(ClusterName, Command) ->
%     Key = ecredis_command_parser:get_key_from_command(Command),
%     case Key of
%         undefined ->
%             error_logger:error_msg("Unable to execute: ~p, invalid cluster key~n", [Command]),
%             {error, invalid_cluster_key, Command};
%         Key ->
%             query_by_key(ClusterName, Command, Key)
%     end.


% query_by_key(ClusterName, Command, Key) ->
%     Slot = ecredis_command_parser:get_key_slot(Key),
%     case Slot of
%         undefined ->
%             error_logger:error_msg("Unable to execute: ~p, slot: ~p has no connection~n", [Command, Slot]),
%             {error, no_connection, Command};
%         Slot ->
%             query_by_slot(ClusterName, Command, Key, Slot, 0)
%     end.


% query_by_slot(ClusterName, Command, Key, Slot, NumRetries) ->
    


% query_by_command(ClusterName, Command) ->
%     case ecredis_command_parser:get_key_from_command(Command) of
%         undefined ->
%             error_logger:error_msg("Unable to execute: ~p, invalid cluster key~n", [Command]),
%             {error, invalid_cluster_key, Command};
%         Key ->
%             query_by_key(ClusterName, Command, Key)
%     end.






%%% Current Issues:
%%% When implementinig asking, we should only go one level deep - that is, if we ask and 
%%% the response is not something that is an answer, we need to just return the error (i think)
%%%
%%%
%%%
%%% This is what the control flow should be:
%%% 1. get the key from the command
%%% 2. get the slot from the key
%%% 3. get the node from the slot
%%% 4. send the request to that slot
%%% 5. handle the response (i.e. respond to redirects if needed
%%% 6. try again if we need to
%%% 
%%%
%%%
%%%
%%%
% %%%
% execute_query(_Cluster, undefined, Command, Slot, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
%     error_logger:error_msg("Unable to execute: ~p, slot: ~p has no connection~n", [Command, Slot]),
%     {error, no_connection, Command};
% execute_query(ClusterName, Pid, Command, Slot, Version, Counter) ->
%     %% Throttle retries
%     throttle_retries(Counter),
%     Response = eredis_query(Pid, Command),
%     Result = handle_response(ClusterName, Command, Response, Version),
%     case handle_result(Result, Version) of
%         retry ->
%             execute_query(ClusterName, Pid, Command, Slot, Version, Counter + 1);
%         Result ->
%             Result
%     end.



%%% Definitely need separate functions for handling ask errors, because of the nature of the directed call
%%% this should be pretty similiar to what eredis_cluster already does

    % case eredis_query(Pid, Command) of
    %     % If we detect a node went down, we should probably refresh the slot
    %     % mapping.
    %     {error, no_connection} ->
    %         error_logger:warning_msg("no_connection, ~p v: ~p", [ClusterName, Version]),
    %         {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
    %         execute_slot_query(ClusterName, Command, Slot, Counter + 1);

    %     % If the tcp connection is closed (connection timeout), the redis worker
    %     % will try to reconnect, thus the connection should be recovered for
    %     % the next request. We don't need to refresh the slot mapping in this
    %     % case.
    %     {error, tcp_closed} ->
    %         error_logger:warning_msg("tcp_closed, ~p v: ~p", [ClusterName, Version]),
    %         execute_query(ClusterName, Pid, Command, Slot, Version, Counter + 1);

    %     % Redis explicitly say our slot mapping is incorrect, we need to refresh it.
    %     {error, <<"MOVED ", _/binary>>} ->
    %         error_logger:warning_msg("moved, ~p v: ~p", [ClusterName, Version]),
    %         {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
    %         execute_slot_query(ClusterName, Command, Slot, Counter + 1);

    %     {error, <<"ASK ", RedirectInfo/binary>>} ->
    %         %% TODO(@ethan): add logic for forwarding request to redirect location
    %         case parse_redirect_info(RedirectInfo) of
    %             {ok, RedirectPid} ->
    %                 AskingPipeline = [[<<"ASKING">>], Command],
    %                 execute_query(ClusterName, RedirectPid, AskingPipeline, Slot, Version, Counter + 1)
    %         end,
    %         error_logger:warning_msg("ask, ~p v: ~p", [ClusterName, Version]);
            

    %     Result ->
    %         %% When querying multiple commands, result will be an array.
    %         %% Check for errors if slot mapping is incorrect, we need to refresh and remap them.
    %         case check_for_moved_errors(Result) of
    %             true ->
    %                 case ecredis_command_parser:check_sanity_of_keys(Command) of
    %                     ok ->
    %                         %% TODO(@ethan): execute only queries that failed.
    %                         %% TODO(@ethan): check for ask responses in pipelines as well
    %                         error_logger:warning_msg("moved, ~p v: ~p", [ClusterName, Version]),
    %                         {ok, _} = ecredis_server:remap_cluster(ClusterName, Version),
    %                         execute_slot_query(ClusterName, Command, Slot, Counter + 1);
    %                     error ->
    %                         error_logger:error_msg("All keys in pipeline command are not mapped
    %                                 to the same slot, clustername, ~p v: ~p, command: ~p", [ClusterName, Version, Command]),
    %                         Result
    %                 end;
    %             false ->
    %                 Result
    %         end
    % end.




%%% borrowed from https://github.com/Nordix/eredis_cluster/blob/master/src/eredis_cluster.erl
%%% 
%% Parses the Rest as in <<"ASK ", Rest/binary>> and returns an
%% existing pool if any or an error.
% -spec parse_redirect_info(RedirectInfo :: binary()) ->
%           {ok, ExistingPool :: atom()} | {error, any()}.
% parse_redirect_info(RedirectInfo) ->
%     try
%         [_Slot, AddrPort] = binary:split(RedirectInfo, <<" ">>),
%         [Addr0, PortBin] = string:split(AddrPort, ":", trailing),
%         Port = binary_to_integer(PortBin),
%         IPv6Size = byte_size(Addr0) - 2, %% Size of address possibly without 2 brackets
%         Addr = case Addr0 of
%                    <<"[", IPv6:IPv6Size/binary, "]">> ->
%                        %% An IPv6 address wrapped in square brackets.
%                        IPv6;
%                    _ ->
%                        Addr0
%                end,
%         %% Validate the address string
%         {ok, _} = inet:parse_address(binary:bin_to_list(Addr)),
%         eredis_cluster_pool:get_existing_pool(Addr, Port)
%     of
%         {ok, Pool} ->
%             {ok, Pool};
%         {error, _} = Error ->
%             Error
%     catch
%         error:{badmatch, _} ->
%             %% Couldn't parse or validate the address
%             {error, bad_redirect};
%         error:badarg ->
%             %% binary_to_integer/1 failed
%             {error, bad_redirect}
%     end.
