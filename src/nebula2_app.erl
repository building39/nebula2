-module(nebula2_app).
-behaviour(application).

-ifdef(TESTX).
-include_lib("eunit/include/eunit.hrl").
-endif.
-include("nebula.hrl").

-export([start/2]).
-export([stop/1]).

-export([app_config/2
]).

start(_Type, _Args) ->
    lager:debug("Starting folsom..."),
    application:start(folsom),
    lager:debug("folsom allegedly started."),
    Dispatch = cowboy_router:compile([
        {'_', [{"/cdmi/[...]", cdmi_handler, []},
               {"/bootstrap/[...]", bootstrap_handler, []}]}
    ]),
    cowboy:start_http(my_http_listener, 1000, [{port, 8080}],
        [{env, [{dispatch, Dispatch}]}]
    ),
    lager:start(),
    application:start(pooler),
    mcd:start_link(?MEMCACHE, ["localhost", 11211]),
    nebula2_sup:start_link().

stop(_State) ->
    ok.

%% @doc Gets a configuration item from the environment.
-spec nebula2_app:app_config(atom(), string()) -> any().
app_config(Name, Default) ->
    handle_app_env(application:get_env(nebula2, Name), Default).

handle_app_env({ok, Value}, _Default) -> Value;
handle_app_env(undefined, Default) -> Default.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNITX).
%% @doc Test the app_config/2 function.
%%      This test expects cdmi_location to be set to "US-TX" in rel/sys.config
app_config1_test() ->
    ?assert(app_config(cdmi_location, ?DEFAULT_LOCATION) == "US-TX").
%% @doc Test the app_config/2 function default value operation
%%      This test expects cdmi_location to be set to "US-TX" in rel/sys.config
app_config2_test() ->
    ?assert(app_config(non_existent, "test success") == "test success").
-endif.