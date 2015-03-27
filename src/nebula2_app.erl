-module(nebula2_app).
-behaviour(application).

-include("nebula.hrl").

-export([start/2]).
-export([stop/1]).

-export([app_config/2,
         riak_port/0,
         riak_servers/0,
         riak_pools_init/0,
         riak_pools_max/0,
         riak_location/0
]).

start(_Type, _Args) ->
    Dispatch = cowboy_router:compile([
        {'_', [{"/cdmi", cdmi_handler, []}]}
    ]),
    cowboy:start_http(my_http_listener, 100, [{port, 8080}],
        [{env, [{dispatch, Dispatch}]}]
    ),
    lager:start(),
    application:start(pooler),
    nebula2_sup:start_link().

stop(_State) ->
    ok.

%% @doc Returns the geographical location configured for nebula.
-spec nebula_app:riak_location() -> string().
riak_location() -> 
    app_config(riak_location, ?DEFAULT_RIAK_LOCATION).

%% @doc Returns the initial number of riak worker pools.
-spec nebula_app:riak_pools_init() -> int.
riak_pools_init() ->
    app_config(riak_pools_init, ?DEFAULT_RIAK_POOLS_INIT).

%% @doc Returns the maximum number of riak worker pools.
-spec nebula_app:riak_pools_max() -> int.
riak_pools_max() ->
    app_config(riak_pools_max, ?DEFAULT_RIAK_POOLS_MAX).

%% @doc Returns the port that riak listens on.
-spec nebula_app:riak_port() -> integer().
riak_port() ->
    app_config(riak_port, ?DEFAULT_RIAK_PORT).

%% @doc Returns the IP address or hostname that riak listens on.
-spec nebula_app:riak_server() -> [string()].
riak_servers() ->
    app_config(riak_servers, ?DEFAULT_RIAK_SERVERS).

%% @doc Gets a configuration item from the environment.
-spec nebula_app:app_config(string(), string()) -> string().
app_config(Name, Default) ->
    handle_app_env(application:get_env(nebula2, Name), Default).

handle_app_env({ok, Value}, _Default) -> Value;
handle_app_env(undefined, Default) -> Default.