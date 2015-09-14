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
-export([delete/2,
         get/2,
         get_domain_maps/1,
         put/4,
         ping/1,
         search/2,
         search/3,
         update/3]).

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec nebula2_riak:delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    mcd:delete(?MEMCACHE, Oid),
    riakc_pb_socket:delete(Pid,
                           {list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                           Oid).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_binary(Oid) ->
    get(Pid, binary_to_list(Oid));
get(Pid, Oid) ->
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            {ok, Data};
        _ ->
            case riakc_pb_socket:get(Pid,
                                     {list_to_binary(?BUCKET_TYPE),
                                      list_to_binary(?BUCKET_NAME)},
                                      list_to_binary(Oid)) of
                {ok, Object} ->
                    Data = jsx:decode(riakc_obj:get_value(Object), [return_maps]),
                    {ok, _R} = mcd:set(?MEMCACHE, Oid, Data, ?MEMCACHE_EXPIRY),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
            end
    end.

%% @doc Get the domain maps.
-spec nebula2_riak:get_domain_maps(pid()) -> list().
get_domain_maps(Pid) ->
    case execute_search(Pid, ?DOMAIN_MAPS_QUERY) of
        {ok, Result} ->
            maps:get(<<"value">>, Result);
        _ ->
            []
    end.

%% @doc Ping the riak cluster.
-spec nebula2_riak:ping(pid()) -> boolean().
ping(Pid) ->
    case riakc_pb_socket:ping(Pid) of
        pong -> % lager:debug("Connected to Riak..."),
                true;
        _R -> % lager:debug("Can't ping Riak: ~p", [R]),
             false
    end.

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                      object_name(),  %% Object
                      object_oid(),   %% Oid
                      map()           %% Data to store
                     ) -> {'error', _} | {'ok', _}.
put(Pid, _ObjectName, Oid, Data) ->
    do_put(Pid, Oid, Data).

-spec nebula2_riak:do_put(pid(), object_oid(), map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) when is_binary(Oid) ->
    do_put(Pid, binary_to_list(Oid), Data);
do_put(Pid, Oid, Data) ->
    % lager:debug("nebula2_riak:do_put Data is ~p", [Data]),
    Json = jsx:encode(Data),
    % lager:debug("nebula2_riak:do_put JSON is ~p", [Json]),
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            list_to_binary(Oid),
                            Json,
                            list_to_binary("application/json")),
    case riakc_pb_socket:put(Pid, Object) of
        ok ->
            mcd:set(?MEMCACHE, Oid, Data, ?MEMCACHE_EXPIRY),
            {ok, Oid};
        {error, Term} ->
            {error, Term}
    end.

%% @doc Search an index for objects.
-spec nebula2_riak:search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) ->
    {Pid, EnvMap} = State,
    Query = create_query(Path, EnvMap),
    % lager:debug("Query: ~p", [Query]),
    execute_search(Pid, Query).

%% @doc Search an index for objects, but don't search on domainUri
-spec nebula2_riak:search(string(), cdmi_state(), nodomain) -> {error, 404|500}|{ok, map()}.
search(_Path, State, nodomain) ->
    {Pid, EnvMap} = State,
    ParentURI = maps:get(<<"parentURI">>, EnvMap),
    ObjectName = maps:get(<<"objectName">>, EnvMap),
    Query = "parentURI:\\" ++ ParentURI ++ " AND objectName:\\" ++ ObjectName,
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
    % lager:debug("nebula2_riak:update Updating object ~p", [Oid]),
    case get(Pid, Oid) of
        {error, E} ->
            {error, E};
        {ok, _} ->
            Json = jsx:encode(Data),
            {ok, Obj} = riakc_pb_socket:get(Pid, 
                                            {list_to_binary(?BUCKET_TYPE),
                                             list_to_binary(?BUCKET_NAME)},
                                            Oid),
            NewObj = riakc_obj:update_value(Obj, Json),
            mcd:set(?MEMCACHE, Oid, Data, ?MEMCACHE_EXPIRY),
            riakc_pb_socket:put(Pid, NewObj)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Create a search key.
-spec nebula2_riak:create_query(string(), map()) -> string().
create_query(Path, EnvMap) ->
    Parts = string:tokens(Path, "/"),
    ParentURI = nebula2_utils:get_parent_uri(Parts),
    ObjectName = nebula2_utils:get_object_name(Parts, Path),
    Method = maps:get(<<"method">>, EnvMap),
    ObjectType = case Method of
                     <<"GET">> ->
                         maps:get(<<"accept">>, EnvMap);
                     <<"PUT">> ->
                         maps:get(<<"content-type">>, EnvMap);
                     <<"DELETE">> ->
                         undefined;
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
    "objectName:\\/";
create_query(ObjectName, ContentType, ParentURI, _) when ContentType =:= <<?CONTENT_TYPE_CDMI_CAPABILITY>>; 
                                                         ContentType =:= <<"*/*">> ->
    "parentURI:\\" ++ ParentURI ++ " AND objectName:\\" ++ ObjectName;
create_query(ObjectName, _ContentType, ParentURI, EnvMap) ->
    DomainURI = maps:get(<<"domainURI">>, EnvMap),
    "domainURI:\\" ++ DomainURI ++ " AND parentURI:\\" ++ ParentURI ++ " AND objectName:\\" ++ ObjectName.
    
%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {error, 404|500} |{ok, map()}.
execute_search(Pid, Query) ->
    lager:debug("Query: ~p", [Query]),
    Index = list_to_binary(?CDMI_INDEX),
    {ok, Results} = riakc_pb_socket:search(Pid, Index, Query),
    Response = case Results#search_results.num_found of
                    0 ->
                        {error, 404}; %% Return 404
                    1 ->
                        [{_, Doc}] = Results#search_results.docs,
                        fetch(Pid, Doc);
                    _N ->
                        {error, 500} %% Something's funky - return 500
    end,
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, map()}.
fetch(Pid, Data) ->
    Oid = binary_to_list(proplists:get_value(<<"_yz_rk">>, Data)),
    Response = nebula2_riak:get(Pid, Oid),
    Response.

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
