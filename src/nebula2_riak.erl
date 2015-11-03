%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% riak parameters
-include("nebula2_riak.hrl").
%% Domain Maps Query

%% ====================================================================
%% API functions
%% ====================================================================
-export([delete/2,
         get/2,
         get_domain_maps/2,
         put/3,
         available/1,
         search/2,
         update/3]).

%% @doc Ping the riak cluster.
-spec available(pid()) -> boolean().
available(Pid) when is_pid(Pid) ->
%    ?nebMsg("Entry"),
    case riakc_pb_socket:ping(Pid) of
        pong ->
                true;
        _R ->
             false
    end.

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    riakc_pb_socket:delete(Pid,
                           ?RIAK_TYPE_AND_BUCKET,
                           Oid).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    case riakc_pb_socket:get(Pid, ?RIAK_TYPE_AND_BUCKET, Oid) of
                {ok, Object} ->
                    Data = jsx:decode(riakc_obj:get_value(Object), [return_maps]),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
    end.

%% @doc Get the domain maps.
-spec get_domain_maps(pid(), object_path()) -> binary().
get_domain_maps(Pid, Path) when is_pid(Pid), is_list(Path) ->
%    ?nebMsg("Entry"),
    execute_search(Pid, "sp:\\" ++ Path).

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec put(pid(),
          object_oid(),   %% Oid
          map()           %% Data to store
         ) -> {'error', _} | {'ok', _}.
put(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_map(Data) ->
%    ?nebMsg("Entry"),
    Json = jsx:encode(Data),
    Object = riakc_obj:new(?RIAK_TYPE_AND_BUCKET,
                            Oid,
                            Json,
                            list_to_binary(?CONTENT_TYPE_JSON)),
    case riakc_pb_socket:put(Pid, Object) of
        ok ->
            {ok, Oid};
        {error, Term} ->
            {error, Term}
    end.

%% @doc Search an index for objects.
-spec search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) when is_list(Path), is_tuple(State) ->
%    ?nebMsg("Entry"),
    {Pid, _} = State,
    Query = "sp:\\" ++ Path,
    Result =  execute_search(Pid, Query),
    Result.

%% @doc Update an existing key/value pair.
-spec update(pid(),
             object_oid(),      %% Oid
             binary()           %% Data to store
            ) -> ok | {error, term()}.

update(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_binary(Data) ->
%    ?nebMsg("Entry"),
    case get(Pid, Oid) of
        {error, Term} ->
            {error, Term};
        {ok, _O} ->
            {ok, Obj} = riakc_pb_socket:get(Pid, 
                                            ?RIAK_TYPE_AND_BUCKET,
                                            Oid),
            NewObj = riakc_obj:update_value(Obj, Data),
            riakc_pb_socket:put(Pid, NewObj)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Execute a search.
-spec execute_search(pid(),              %% Riak client pid.
                     search_predicate()  %% URI.
                    ) -> {error, 404|500} |{ok, map()}.
execute_search(Pid, Query) when is_pid(Pid) ->
%    ?nebMsg("Entry"),
    Index = ?CDMI_INDEX,
    Response = case riakc_pb_socket:search(Pid, Index, Query) of
                   {ok, Results} ->
                       case Results#search_results.num_found of
                           0 ->
                               {error, 404}; %% Return 404
                           1 ->
                               [{_, Doc}] = Results#search_results.docs,
                               Oid = proplists:get_value(<<"_yz_rk">>, Doc),
                               get(Pid, Oid);
                           _N ->
                               {error, 500} %% Something's funky - return 500
                       end;
                   _ ->
                       {error, 404}
               end,
    Response.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

available_test() ->
    meck:new(riakc_pb_socket, [non_strict]),
    Pid = self(),
    meck:expect(riakc_pb_socket, ping, [Pid], pong),
    ?assertMatch(true, available(Pid)),
    meck:expect(riakc_pb_socket, ping, [Pid], pang),
    ?assertMatch(false, available(Pid)),
    meck:unload(riakc_pb_socket).

delete_test() ->
    Pid = self(),
    TestOid = ?TestOid,
    meck:new(riakc_pb_socket, [non_strict]),
    Return1 = ok,
    meck:expect(riakc_pb_socket, delete, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], Return1),
    ?assertMatch(Return1, delete(Pid, TestOid)),
    Return2 = {error, not_found},
    meck:expect(riakc_pb_socket, delete, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], Return2),
    ?assertMatch(Return2, delete(Pid, TestOid)),
    ?assertException(error, function_clause, delete(not_a_pid, TestOid)),
    ?assertException(error, function_clause, delete(Pid, not_a_list)),
    meck:unload(riakc_pb_socket).

search_test() ->
    Pid = self(),
    Path = ?TestSystemDomainHash ++ "/system_configuration/"++ "domain_maps",
    TestBinary = ?TestBinary,
    TestSearchResults_1_Result = ?TestSearchResults_1_Result,
    TestMap = jsx:decode(TestBinary, [return_maps]),
    TestQuery = ?TestQuery2,
    TestRiakObject = ?TestRiakObject,
    TestOid = ?TestOid,
    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    meck:expect(riakc_pb_socket, get, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], {ok, TestRiakObject}),
    meck:expect(riakc_obj, get_value, [TestRiakObject], TestBinary),

    meck:expect(riakc_pb_socket, search, [Pid, ?CDMI_INDEX, TestQuery], {ok, TestSearchResults_1_Result}),
    ?assertMatch({ok, TestMap}, execute_search(Pid, TestQuery)),
    ?assertMatch({ok, TestMap}, search(Path, {Pid, maps:new()})),
    ?assertMatch({ok, TestMap}, get_domain_maps(Pid, Path)),

    meck:expect(riakc_pb_socket, search, [Pid, ?CDMI_INDEX, TestQuery], {ok, ?TestSearchResults_0_Result}),
    ?assertMatch({error, 404}, execute_search(Pid, TestQuery)),

    meck:expect(riakc_pb_socket, search, [Pid, ?CDMI_INDEX, TestQuery], {ok, ?TestSearchResults_2_Result}),
    ?assertMatch({error, 500}, execute_search(Pid, TestQuery)),

    meck:expect(riakc_pb_socket, search, [Pid, ?CDMI_INDEX, TestQuery], {error, not_found}),
    ?assertMatch({error, 404}, execute_search(Pid, TestQuery)),
    
    ?assertException(error, function_clause, execute_search(not_a_pid, TestQuery)),
    ?assertException(error, function_clause, execute_search(Pid, not_a_list)),

    meck:unload(riakc_obj),
    meck:unload(riakc_pb_socket).

get_test() ->
    Pid = self(),
    TestBinary = ?TestBinary,
    TestMap = jsx:decode(?TestBinary, [return_maps]),
    TestOid = ?TestOid,
    TestRiakObject = ?TestRiakObject,
    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    
    meck:expect(riakc_pb_socket, get, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], {ok, TestRiakObject}),
    meck:expect(riakc_obj, get_value, [TestRiakObject], TestBinary),
    
    ?assertMatch(TestBinary, riakc_obj:get_value(TestRiakObject)),
    ?assertMatch({ok, TestMap}, get(Pid, TestOid)),
    
    meck:unload(riakc_obj),
    meck:unload(riakc_pb_socket).

put_test() ->    %% update for containers is currently broken.
    Pid = self(),
    TestBinary = ?TestBinary,
    TestMap = jsx:decode(?TestBinary, [return_maps]),
    TestOid = ?TestOid,
    TestRiakObject = ?TestRiakObject,
    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    meck:expect(riakc_obj, new, [?RIAK_TYPE_AND_BUCKET, TestOid, TestBinary, list_to_binary(?CONTENT_TYPE_JSON)], TestRiakObject),
    meck:expect(riakc_pb_socket, put, [Pid, TestRiakObject], ok),
    ?assertMatch({ok, TestOid}, put(Pid, TestOid, TestMap)),
    meck:expect(riakc_pb_socket, put, [Pid, TestRiakObject], {error, ioerror}),
    ?assertMatch({error,ioerror}, put(Pid, TestOid, TestMap)),
    ?assertException(error, function_clause, put(not_a_pid, TestOid, TestMap)),
    ?assertException(error, function_clause, put(Pid, not_an_oid, TestMap)),
    ?assertException(error, function_clause, put(Pid, TestOid, not_a_map)),
    meck:unload(riakc_obj),
    meck:unload(riakc_pb_socket).

update_test() ->    %% update for containers is currently broken.
    Pid = self(),
    TestBinary = ?TestBinary,
    TestOid = ?TestOid,
    TestRiakObject = ?TestRiakObject,
    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    Return1 = {ok, TestRiakObject},
    meck:expect(riakc_pb_socket, get, [Pid, ?RIAK_TYPE_AND_BUCKET, TestOid], Return1),
    meck:expect(riakc_obj, get_value, [TestRiakObject], TestBinary),
    meck:expect(riakc_obj, update_value, [TestRiakObject, TestBinary], TestRiakObject),
    meck:expect(riakc_pb_socket, put, [Pid, TestRiakObject], ok),
    ?assertMatch(ok, update(Pid, TestOid, TestBinary)),
    Return2 = {error, not_found},
    meck:expect(riakc_pb_socket, get, [Pid, ?RIAK_TYPE_AND_BUCKET, TestOid], Return2),
    ?assertMatch(Return2, update(Pid, TestOid, TestBinary)),
    ?assertException(error, function_clause, update(not_a_pid, TestOid, TestBinary)),
    ?assertException(error, function_clause, update(Pid, not_an_oid, TestBinary)),
    meck:unload(riakc_obj),
    meck:unload(riakc_pb_socket).

-endif.