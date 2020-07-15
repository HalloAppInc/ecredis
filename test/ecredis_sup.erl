-module(ecredis_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0]).
-export([init/1]).

-export([stop/0]).

-include("ecredis.hrl").

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    Procs = [{ecredis_a, {ecredis, start_link, [{ecredis_a, [{"127.0.0.1", 30001}]}]},
              permanent, 5000, worker, [dynamic]},
             {ecredis_b, {ecredis, start_link, [{ecredis_b, [{"127.0.0.1", 30005}]}]},
              permanent, 5000, worker, [dynamic]}],
    {ok, {{one_for_one, 1, 5}, Procs}}.

-spec stop() -> ok.
stop() ->
    ok.


