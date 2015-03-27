-module(nebula2_app).
-behaviour(application).

-include("nebula.hrl").

-export([start/2]).
-export([stop/1]).

-export([app_config/2,
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

%% @doc Gets a configuration item from the environment.
-spec nebula_app:app_config(string(), string()) -> string().
app_config(Name, Default) ->
    handle_app_env(application:get_env(nebula2, Name), Default).

handle_app_env({ok, Value}, _Default) -> Value;
handle_app_env(undefined, Default) -> Default.