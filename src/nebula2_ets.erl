%% @author mmartin
%% @doc
%% Backend metadata storage module targeting ETS.
%% @end


-module(nebula2_ets).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include("nebula.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([available/1,
         delete/2,
         get/2,
         get_domain_maps/2,
         put/3,
         search/2,
         update/3]).

%% @doc ETS is always available.
-spec available(pid()) -> boolean().
available(Pid) when is_pid(Pid) ->
%    ?nebMsg("Entry"),
    true.

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    {error, 404}.

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    {error, 404}.

%% @doc Get the domain maps.
-spec get_domain_maps(pid(), object_path()) -> {error, 404|500} |{ok, map()}.
get_domain_maps(Pid, Path) when is_pid(Pid), is_list(Path) ->
%    ?nebMsg("Entry"),
    {error, 404}.

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec put(pid(),
          object_oid(),   %% Oid
          map()           %% Data to store
         ) -> {error, term()} | {ok, object_oid()}.
put(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_map(Data) ->
%    ?nebMsg("Entry"),
    {error, 403}.

%% @doc Search an index for objects.
-spec search(string(), cdmi_state()) -> {error, term()}|{ok, map()}.
search(Path, State) when is_list(Path), is_tuple(State) ->
%    ?nebMsg("Entry"),
    {error, 404}.

%% @doc Update an existing key/value pair.
-spec update(pid(),
             object_oid(),      %% Oid
             binary()           %% Data to store
            ) -> ok | {error, term()}.

update(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_map(Data) ->
%    ?nebMsg("Entry"),
    {error, 404}.

%% ====================================================================
%% Internal functions
%% ====================================================================


%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

nebula2_riak_test_() ->
    {foreach,
        fun() ->
            meck:new(riakc_pb_socket, [non_strict]),
            meck:new(riakc_obj, [non_strict])
        end,
        fun(_) ->
            meck:unload(riakc_pb_socket),
            meck:unload(riakc_obj)
        end,
        [
            {"Test available/1",
                fun() ->
                    ?assertMatch(true, available(self()))
                end
            },
            {"Test available/1 contract",
                fun() ->
                    ?assertException(error, function_clause, available(not_a_pid))
                end
            },
            {"Test delete/2",
                fun() ->
                    ?assertMatch({error, 404}, delete(self(), <<"binary">>))
                end
            },
            {"Test delete/2 contract",
                fun() ->
                    ?assertException(error, function_clause, delete(not_a_pid, <<"binary">>)),
                    ?assertException(error, function_clause, delete(self(), not_a_binary))
                end
            },
            {"Test get/2",
                fun() ->
                    ?assertMatch({error, 404}, get(self(), <<"binary">>))
                end
            },
            {"Test get/2 contract",
                fun() ->
                    ?assertException(error, function_clause, get(not_a_pid, <<"binary">>)),
                    ?assertException(error, function_clause, get(self(), not_a_binary))
                end
            },
            {"Test get_domain_maps/2",
                fun() ->
                    ?assertMatch({error, 404}, get_domain_maps(self(), "Path"))
                end
            },
            {"Test get_domain_maps/2 contract",
                fun() ->
                    ?assertException(error, function_clause, get_domain_maps(not_a_pid, "Path")),
                    ?assertException(error, function_clause, get_domain_maps(self(), not_a_path))
                end
            },
            {"Test put/3",
                fun() ->
                    ?assertMatch({error, 403}, put(self(), <<"binary">>, maps:new()))
                end
            },
            {"Test put/3 contract",
                fun() ->
                    ?assertException(error, function_clause, put(not_a_pid, <<"binary">>, maps:new())),
                    ?assertException(error, function_clause, put(self(), not_a_binary, maps:new())),
                    ?assertException(error, function_clause, put(self(), <<"binary">>, not_a_map))
                end
            },
            {"test search/2",
                fun() ->
                    ?assertMatch({error, 404}, search("Path", {self(), maps:new()}))
                end
            },
            {"Test search/2 contract",
                fun() ->
                    ?assertException(error, function_clause, search(not_a_path, {self(), maps:new()})),
                    ?assertException(error, function_clause, search("Path", not_a_tuple))
                end
            },
            {"test update/3",
                fun() ->
                    ?assertMatch({error, 404}, update(self(), <<"binary">>, maps:new()))
                end
            },
            {"Test update/3 contract",
                fun() ->
                    ?assertException(error, function_clause, update(not_a_pid, <<"binary">>, maps:new())),
                    ?assertException(error, function_clause, update(self(), not_a_binary, maps:new())),
                    ?assertException(error, function_clause, update(self(), <<"binary">>, not_a_map))
                end
            }
        ]
    }.

-endif.

