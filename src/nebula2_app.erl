-module(nebula2_app).
-behaviour(application).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("nebula.hrl").

-export([start/2]).
-export([stop/1]).

-export([app_config/2,
         cdmi_location/0
]).

start(_Type, _Args) ->
    Dispatch = cowboy_router:compile([
        {'_', [{"/cdmi/[...]", cdmi_handler, []}]}
    ]),
    cowboy:start_http(my_http_listener, 100, [{port, 8080}],
        [{env, [{dispatch, Dispatch}]}]
    ),
    lager:start(),
    application:start(pooler),
    mcd:start_link(?MEMCACHE, ["localhost", 11211]),
    nebula2_sup:start_link().

stop(_State) ->
    ok.

%% @doc Returns the geographical location configured for nebula.
-spec nebula_app:cdmi_location() -> string().
cdmi_location() -> 
    app_config(cdmi_location, ?DEFAULT_LOCATION).

%% @doc Gets a configuration item from the environment.
-spec nebula_app:app_config(string(), string()) -> string().
app_config(Name, Default) ->
    handle_app_env(application:get_env(nebula2, Name), Default).

handle_app_env({ok, Value}, _Default) -> Value;
handle_app_env(undefined, Default) -> Default.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
%% @doc Test the cdmi_location/0 function.
%%      This test expects cdmi_location to be set to "US-TX" in rel/sys.config
location_test() -> 
    ?assert(cdmi_location() == "US-TX").
%% @doc Test the app_config/2 function.
%%      This test expects cdmi_location to be set to "US-TX" in rel/sys.config
app_config1_test() ->
    ?assert(app_config(cdmi_location, ?DEFAULT_LOCATION) == "US-TX").
%% @doc Test the app_config/2 function default value operation
%%      This test expects cdmi_location to be set to "US-TX" in rel/sys.config
app_config2_test() ->
    ?assert(app_config(non_existent, "test success") == "test success").
-endif.