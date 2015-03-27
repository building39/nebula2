-module(nebula2_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    io:format("Starting Nebula2...~n"),
%%    PoolerSup = {pooler_sup, {pooler_sup, start_link, []},
%%             permanent, infinity, supervisor, [pooler_sup]},
    {ok, {{one_for_one, 1, 5}, []}}.
