-module(nebula2_sup).
-compile([{parse_transform, lager_transform}]).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    lager:info("Starting Nebula2..."),
    {ok, {{one_for_one, 1, 5}, []}}.
