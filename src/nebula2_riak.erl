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
-define(BUCKET_TYPE, "cdmi").
-define(BUCKET_NAME, "cdmi").
-define(CDMI_INDEX, "cdmi_idx").
-define(NAME_PREFIX, "cdmi").
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
-spec nebula2_riak:available(pid()) -> boolean().
available(Pid) when is_pid(Pid) ->
%    ?nebMsg("Entry"),
    case riakc_pb_socket:ping(Pid) of
        pong ->
                true;
        _R ->
             false
    end.

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec nebula2_riak:delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    ?nebMsg("Entry"),
    riakc_pb_socket:delete(Pid,
                           {list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                           Oid).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_binary(Oid) ->
%%    ?nebMsg("Entry"),
    case riakc_pb_socket:get(Pid, {list_to_binary(?BUCKET_TYPE),
                                   list_to_binary(?BUCKET_NAME)},
                                   Oid) of
                {ok, Object} ->
                    Data = jsx:decode(riakc_obj:get_value(Object), [return_maps]),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
    end.

%% @doc Get the domain maps.
-spec nebula2_riak:get_domain_maps(pid(), object_path()) -> binary().
get_domain_maps(Pid, Path) ->
    ?nebMsg("Entry"),
    execute_search(Pid, "sp:\\" ++ Path).

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                       object_oid(),   %% Oid
                       map()           %% Data to store
                      ) -> {'error', _} | {'ok', _}.
put(Pid, Oid, Data) ->
    ?nebMsg("Entry"),
    do_put(Pid, Oid, Data).

-spec nebula2_riak:do_put(pid(), object_oid(), map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) ->
    ?nebMsg("Entry"),
    Json = jsx:encode(Data),
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            Oid,
                            Json,
                            list_to_binary("application/json")),
    case riakc_pb_socket:put(Pid, Object) of
        ok ->
            {ok, Oid};
        {error, Term} ->
            {error, Term}
    end.

%% @doc Search an index for objects.
-spec nebula2_riak:search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) when is_binary(Path)->
    Path2 = binary_to_list(Path),
    search(Path2, State);
search(Path, State) ->
    ?nebMsg("Entry"),
    {Pid, _} = State,
    Query = "sp:\\" ++ Path,
    Result =  execute_search(Pid, Query),
    Result.

%% @doc Update an existing key/value pair.
-spec nebula2_riak:update(pid(),
                          object_oid(),      %% Oid
                          map()              %% Data to store
                         ) -> ok | {error, term()}.
update(Pid, Oid, Data) when is_binary(Oid) ->
    update(Pid, binary_to_list(Oid), Data);
update(Pid, Oid, Data) ->
    ?nebMsg("Entry"),
    case get(Pid, Oid) of
        {error, E} ->
            {error, E};
        {ok, _} ->
            {ok, Obj} = riakc_pb_socket:get(Pid, 
                                            {list_to_binary(?BUCKET_TYPE),
                                             list_to_binary(?BUCKET_NAME)},
                                            Oid),
            NewObj = riakc_obj:update_value(Obj, Data),
            riakc_pb_socket:put(Pid, NewObj)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {error, 404|500} |{ok, map()}.
execute_search(Pid, Query) ->
    ?nebMsg("Entry"),
    Index = list_to_binary(?CDMI_INDEX),
    ?nebFmt("Query: ~p", [Query]),
    Response = case riakc_pb_socket:search(Pid, Index, Query) of
                   {ok, Results} ->
                       case Results#search_results.num_found of
                           0 ->
                               ?nebMsg("Not Found"),
                               {error, 404}; %% Return 404
                           1 ->
                               [{_, Doc}] = Results#search_results.docs,
                               ?nebFmt("Doc: ~p", [Doc]),
                               fetch(Pid, Doc);
                           _N ->
                               ?nebMsg("Something funky"),
                               {error, 500} %% Something's funky - return 500
                       end;
                   _ ->
                       ?nebMsg("WTF?"),
                       {error, 404}
               end,
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, map()}.
fetch(Pid, Data) when is_pid(Pid); is_list(Data) ->
    ?nebMsg("Entry"),
    Oid = proplists:get_value(<<"_yz_rk">>, Data),
    ?nebFmt("Oid: ~p", [Oid]),
    Response = get(Pid, Oid),
    ?nebFmt("Response: ~p", [Response]),
    Response.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

available_test() ->
    meck:new(riakc_pb_socket, [non_strict]),
    Pid = self(),
    meck:expect(riakc_pb_socket, ping, [Pid], pong),
    ?assertMatch(true, nebula2_riak:available(Pid)),
    meck:expect(riakc_pb_socket, ping, [Pid], pang),
    ?assertMatch(false, nebula2_riak:available(Pid)),
    meck:unload(riakc_pb_socket).
fetch_test() ->
    Pid = self(),
    TestBinary = ?TestBinary,
    TestData = ?TestData,
    TestMap = jsx:decode(TestBinary, [return_maps]),
    TestObject = ?TestObject,
    TestOid = ?TestOid,
    meck:new(riakc_pb_socket, [non_strict]),
    meck:new(riakc_obj, [non_strict]),
    meck:expect(riakc_pb_socket, get, [Pid, {<<"cdmi">>, <<"cdmi">>}, TestOid], {ok, TestObject}),
    meck:expect(riakc_obj, get_value, [TestObject], TestBinary),
    Got = fetch(Pid, TestData),
    ?assertMatch({ok, TestMap}, Got),
    meck:unload(riakc_pb_socket).
    
-endif.
