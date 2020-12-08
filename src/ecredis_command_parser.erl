-module(ecredis_command_parser).

%% API.
-export([
    get_key_from_command/1,
    get_key_slot/1
]).

-include("ecredis.hrl").

%%% =============================================================================
%% @doc Return the first key in the command arguments.
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For eval and evalsha command we will look at the fourth term.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================
-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_binary(X) ->
    get_key_from_command([[binary_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1, Term2|Rest]) when is_binary(Term1) ->
    get_key_from_command([binary_to_list(Term1), Term2|Rest]);
get_key_from_command([Term1, Term2|Rest]) when is_binary(Term2) ->
    get_key_from_command([Term1, binary_to_list(Term2)|Rest]);
get_key_from_command([Term1, Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "keys" ->
            undefined;
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

%% =============================================================================
%% @doc Get key for command where the key is in th 4th position (eval and
%% evalsha commands)
%% @end
%% =============================================================================
-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_, KeyName|_]) when is_binary(KeyName) ->
    binary_to_list(KeyName);
get_key_from_rest([_, KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key :: any()) -> Slot :: integer().
get_key_slot(K) ->
    %% cast any type of key to a list.
    Key = lists:concat([K]),
    KeyToBeHashed = hashed_key(Key),
    ecredis_hasher:hash(KeyToBeHashed).


hashed_key(Key) ->
    case string:chr(Key, ${) of
        0 -> Key;
        Start ->
            case string:chr(string:substr(Key, Start + 1), $}) of
                0 -> Key;
                Length ->
                    if
                       Length =:= 1 ->
                           Key;
                       true ->
                           string:substr(Key, Start + 1, Length-1)
                    end
            end
    end.


-spec check_sanity_of_keys(redis_command()) -> ok | error.
check_sanity_of_keys([[X|Y]|Z] = Commands) ->  
    AllKeys = lists:foldl(fun get_key_from_command/1, [], Commands),
    validate_keys(AllKeys);
check_sanity_of_keys([Term1, Term2|Rest]) when is_binary(Term1) ->
    check_sanity_of_keys([binary_to_list(Term1), Term2|Rest]);
get_key_from_command([Term1, Term2|Rest]) when is_binary(Term2) ->
    check_sanity_of_keys([Term1, binary_to_list(Term2)|Rest]);
check_sanity_of_keys([Term1, Term2|Rest]) ->
    AllKeys = case string:to_lower(Term1) of
        "eval" ->
            get_keys_from_rest(Rest);
        "evalsha" ->
            get_keys_from_rest(Rest);
        _ ->
            ok
    end,
    validate_keys(AllKeys).


-spec get_keys_from_rest([anystring()]) -> [string()].
get_keys_from_rest([KeyNum | Rest]) when is_integer(KeyNum) ->
    Keys = lists:sublist(Rest, KeyNum),
    lists:map(
        fun(Key) when is_binary(Key) -> binary_to_list(Key),
            (Key) when is_list(Key) -> Key
        end, Keys);
get_key_from_rest(_) ->
    undefined.


-spec validate_keys([any()]) -> ok | error.
validate_keys(AllKeys) ->
    ValidKeys = lists:filter(fun(Key) -> Key =/= undefined end, AllKeys),
    case ValidKeys of
        []-> ok;
        _ ->
            ValidKeysSlots = lists:map(fun get_key_slot/1, ValidKeys),
            [Slot1 | _] = ValidKeysSlots,
            case lists:all(fun(Slot) -> Slot =:= Slot1 end, ValidKeysSlots) of
                true -> ok;
                false -> error
            end
    end.

