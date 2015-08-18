%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([get/2,
         put/4,
         ping/1,
         search/2,
         update/3]).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, json_value()}|{error, term()}.
get(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry nebula2_riak:get"),
    get(Pid, binary_to_list(Oid));
get(Pid, Oid) ->
    lager:debug("Entry nebula2_riak:get"),
    Response = case raw_get(Pid, Oid) of
                    {ok, Object} ->
                        Data = jsx:decode(riakc_obj:get_value(Object), [return_maps]),
                        {ok, Data};
                    {error, Term} ->
                        {error, Term}
    end,
    lager:debug("nebula2_riak:get Contents: ~p", [Response]),
    Response.

%% @doc Ping the riak cluster.
-spec nebula2_riak:ping(pid()) -> boolean().
ping(Pid) ->
    lager:debug("Entry nebula2_riak:ping"),
    case riakc_pb_socket:ping(Pid) of
        pong -> lager:debug("Connected to Riak..."),
                true;
        R -> lager:debug("Can't ping Riak: ~p", [R]),
             false
    end.

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                      object_name(),  %% Object
                      object_oid(),   %% Oid
                      map()           %% Data to store
                     ) -> {'error', _} | {'ok', _}.
put(Pid, ObjectName, Oid, Data) ->
    lager:debug("Entry nebula2_riak:put Creating object ~p", [ObjectName]),
    do_put(Pid, Oid, Data).

-spec nebula2_riak:do_put(pid(), object_oid(), map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) when is_binary(Oid) ->
    lager:debug("Entry nebula2_riak:do_put"),
    do_put(Pid, binary_to_list(Oid), Data);
do_put(Pid, Oid, Data) ->
    lager:debug("Entry nebula2_riak:do_put"),
    lager:debug("nebula2_riak:do_put Data is ~p", [Data]),
    Json = jsx:encode(Data),
    lager:debug("nebula2_riak:do_put JSON is ~p", [Json]),
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            list_to_binary(Oid),
                            Json,
                            list_to_binary("application/json")),
    case riakc_pb_socket:put(Pid, Object) of
        ok ->
            {ok, Oid};
        {error, Term} ->
            {error, Term}
    end.

%% @doc Search an index for objects.
-spec nebula2_riak:search(string(), Req) -> {error, 404|500}|{ok, string()}
         when Req::cowboy_req:req().
search(Path, State) ->
    lager:debug("Entry nebula2_riak:search"),
    lager:debug("Search: Path: ~p", [Path]),
    lager:debug("Search: State: ~p", [State]),
    {Pid, EnvMap} = State,
    Query = create_query(Path, EnvMap),
    lager:debug("nebula2_riak:search searching for key: ~p", [Query]),
    Result =  execute_search(Pid, Query),
    lager:debug("nebula2_riak:search result: ~p", [Result]),
    Result.

%% @doc Update an existing key/value pair.
-spec nebula2_riak:update(pid(),
                          object_oid(),      %% Oid
                          map()              %% Data to store
                         ) -> ok | {error, term()}.
update(Pid, Oid, Data) ->
    lager:debug("Entry nebula2_riak:update"),
    lager:debug("nebula2_riak:update Updating object ~p", [Oid]),
    case get(Pid, Oid) of
        {error, E} ->
            lager:debug("riak_put got error ~p from search", [E]),
            {error, E};
        {ok, _} ->
            lager:debug("riak_update updating ~p with ~p", [Oid, Data]),
            Json = jsx:encode(Data),
            {ok, Obj} = riakc_pb_socket:get(Pid, 
                                            {list_to_binary(?BUCKET_TYPE),
                                             list_to_binary(?BUCKET_NAME)},
                                            Oid),
            lager:debug("parms: ~p", [Json]),
            NewObj = riakc_obj:update_value(Obj, Json),
            riakc_pb_socket:put(Pid, NewObj)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Create a search key.
-spec nebula2_riak:create_query(string(), map()) -> string().
create_query(Path, EnvMap) ->
    lager:debug("Entry nebula2_riak:create_query"),
    Parts = string:tokens(Path, "/"),
    lager:debug("Path: ~p Parts: ~p", [Path, Parts]),
    ParentURI = nebula2_utils:get_parent_uri(Parts),
    lager:debug("ParentURI: ~p", [ParentURI]),
    ObjectName = nebula2_utils:get_object_name(Parts, Path),
    lager:debug("ObjectName: ~p", [ObjectName]),
    Method = maps:get(<<"method">>, EnvMap),
    ObjectType = case Method of
                     <<"GET">> ->
                         maps:get(<<"accept">>, EnvMap);
                     <<"PUT">> ->
                         maps:get(<<"content-type">>, EnvMap);
                     _ ->
                         lager:error("create_query: need to implement for method ~p", [Method]),
                         maps:get(<<"content-type">>, EnvMap)
                 end,
    create_query(ObjectName, ObjectType, ParentURI, EnvMap).

-spec nebula2_riak:create_query(string(), string(), string(), map()) -> string().
create_query(ObjectName, _, _, _) when ObjectName == "";
                                       ObjectName == "/"; 
                                       ObjectName == <<"">>; 
                                       ObjectName == <<"/">> ->
    lager:debug("Entry nebula2_riak:create_query"),
    "objectName:\\/";
create_query(ObjectName, ContentType, ParentURI, _) when ContentType =:= <<?CONTENT_TYPE_CDMI_CAPABILITY>>; 
                                                         ContentType =:= <<"*/*">> ->
    lager:debug("Entry create_query: capability object"),
    "parentURI:\\" ++ ParentURI ++ " AND objectName:\\" ++ ObjectName;
create_query(ObjectName, ContentType, ParentURI, EnvMap) ->
    lager:debug("Entry create_query: ~p", [ContentType]),
    DomainURI = maps:get(<<"domainURI">>, EnvMap),
    "domainURI:\\" ++ DomainURI ++ " AND parentURI:\\" ++ ParentURI ++ " AND objectName:\\" ++ ObjectName.
    
%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {error, 404|500} |{ok, string()}.
execute_search(Pid, Query) ->
    lager:debug("Entry nebula2_riak:execute_search"),
    Index = list_to_binary(?CDMI_INDEX),
    lager:debug("Query: ~p Index: ~p", [Query, ?CDMI_INDEX]),
    {ok, Results} = riakc_pb_socket:search(Pid, Index, Query),
    Response = case Results#search_results.num_found of
                    0 ->
                        lager:debug("search found zero results"),
                        {error, 404}; %% Return 404
                    1 ->
                        [{_, Doc}] = Results#search_results.docs,
                        lager:debug("search found one result: ~p", [Doc]),
                        fetch(Pid, Doc);
                    N ->
                        lager:debug("search returned ~p results", [N]),
                        {error, 500} %% Something's funky - return 500
    end,
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, string()}.
fetch(Pid, Data) ->
    lager:debug("Entry nebula2_riak:fetch: fetching key: ~p", [Data]),
    ObjectId = binary_to_list(proplists:get_value(<<"_yz_rk">>, Data)),
    lager:debug("fetch found ObjectId: ~p", [ObjectId]),
    Response = case nebula2_riak:get(Pid, ObjectId) of
                   {ok, Data} ->
                       {ok, _R} = mcd:set(?MEMCACHE, ObjectId, {ok, Data}, ?MEMCACHE_EXPIRY),
                       {ok, Data};
                   Other ->
                       Other
               end,
    lager:debug("Response: ~p", [Response]),
    Response.

%% Raw riak get.
-spec raw_get(pid(), object_oid()) -> {ok, riakc_obj()} | {error, term()}.
raw_get(Pid, Oid) ->
    lager:debug("Entry nebula2_riak:raw_get"),
    riakc_pb_socket:get(Pid,
                        {list_to_binary(?BUCKET_TYPE),
                         list_to_binary(?BUCKET_NAME)},
                        list_to_binary(Oid)).

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

create_query_test() ->
    Data = "{\"domainURI\": \"/cdmi_domains/some_domain\",\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {\"cdmi_owner\": \"my_id\"}}",
    Data2 = create_query(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3NvbWVfZG9tYWlubXlfaWQvbXkvcGFyZW50QW5PYmplY3ROYW1l">> == SearchKey).

%% Test with no {metadata: {cdmi_owner: ""}}
create_query2_test() ->
    Data = "{\"domainURI\": \"/cdmi_domains/some_domain\",\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {}}",
    Data2 = create_query(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3NvbWVfZG9tYWluYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).

%% Test with no {domainURI: }
create_query3_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {\"cdmi_owner\": \"my_id\"}}",
    Data2 = create_query(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vbXlfaWQvbXkvcGFyZW50QW5PYmplY3ROYW1l">> == SearchKey).

%% Test with no {domainURI: } AND no {metadata: {cdmi_owner}}
create_query4_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {}}",
    Data2 = create_query(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).

%% Test with no {domainURI: } AND no {metadata: }
create_query5_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\"}",
    Data2 = create_query(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).
-endif.