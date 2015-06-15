%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).
-compile([{parse_transform, lager_transform}]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("riakc/include/riakc.hrl").
-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([get/2,
         get_mapped/2,
         post/3,
         put/4,
         ping/1,
         prepend_name/1,
         strip_name/1,
         search/2,
         create_sk/1,
         update/3]).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, json_value()}.
get(Pid, Oid) ->
    Response = case riakc_pb_socket:get(Pid,
                                        {list_to_binary(?BUCKET_TYPE),
                                         list_to_binary(?BUCKET_NAME)},
                                         list_to_binary(Oid)) of
                    {ok, Object} ->
                        Data = jsx:decode(riakc_obj:get_value(Object)),
                        lager:debug("nebula2_riak:get json: ~p", [Data]),
                        Contents = binary_to_list(jsx:encode(strip_name(jsx:decode(riakc_obj:get_value(Object))))),
                        {ok, Contents};
                    {error, Term} ->
                        {error, Term}
    end,
    lager:debug("nebula2_riak:get Contents: ~p", [Response]),
    Response.

%% @doc Get a value from riak by bucket type, bucket and key. Return map.
-spec nebula2_riak:get_mapped(pid(), object_oid()) -> {ok, json_value()}.
get_mapped(Pid, Oid) ->
    Response = case riakc_pb_socket:get(Pid,
                                        {list_to_binary(?BUCKET_TYPE),
                                         list_to_binary(?BUCKET_NAME)},
                                         list_to_binary(Oid)) of
                    {ok, Object} ->
                        Contents = jsx:decode(riakc_obj:get_value(Object)),
                        {ok, maps:from_list(Contents)};
                    {error, Term} ->
                        {error, Term}
    end,
    lager:debug("nebula2_riak:get Contents: ~p", [Response]),
    Response.
    
%% @doc Ping the riak cluster.
-spec nebula2_riak:ping(pid()) -> boolean().
ping(Pid) ->
    case riakc_pb_socket:ping(Pid) of
        pong -> lager:debug("Connected to Riak..."),
                true;
        R -> lager:debug("Can't ping Riak: ~p", [R]),
             false
    end.

%% @doc Post a value to riak by bucket type, bucket and key.
-spec nebula2_riak:post(pid(),
                       object_oid(),              %% Object ID
                       binary()                   %% Data to store
                      ) -> {ok, object_oid()}.
post(Pid, Oid, Data) ->
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                           undefined,
                           Data),
    {ok, RetObj} = riakc_pb_socket:put(Pid, Object),
    {riakc_obj, _, Oid, _, _, _, _} = RetObj,
    {ok, Oid}.

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                      string(),              %% Object
                      object_oid(),          %% Oid
                      map()                  %% Data to store
                     ) -> {ok, object_oid()}.
put(Pid, ObjectName, Oid, Data) ->
    lager:debug("nebula2_riak:put Creating object ~p", [ObjectName]),
    case search(Pid, ObjectName) of
        {error, 404} ->
            do_put(Pid, Oid, prepend_name(Data));
        {error, E} ->
            lager:debug("riak_put got error ~p from search", [E]),
            {error, E};
        {ok, _} ->
            lager:debug("riak_put got conflict error"),
            {error, 409};
        {Status, Return} ->
            lager:debug("riak_put wtf??? ~p ~p", [Status, Return])
    end.
-spec nebula2_riak:do_put(pid(), object_oid, map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) ->
    lager:debug("nebula2_riak:do_put"),
    lager:debug("nebula2_riak:do_put Data is ~p", [Data]),
    Json = jsx:encode(Data),
    create_sk(Data),
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
-spec nebula2_riak:search(pid(),
                         search_predicate() %% Search predicate
                        ) -> {ok, string()}.
search(Pid, []) ->
    search(Pid, "cdmi/");
search(Pid, "/") ->
   search(Pid, "cdmi/");
search(Pid, RawUri) ->
    Uri = case nebula2_utils:beginswith(RawUri, "cdmi/") of
              true ->
                  RawUri;
              false ->
                  "cdmi" ++ RawUri
    end,
    lager:debug("nebula2_riak:search searching for uri: ~p", [Uri]),
    Data = case mcd:get(?MEMCACHE, Uri) of
                    {error, notfound} ->
                        execute_search(Pid, Uri);
                    {ok, {error, 404}} ->
                        execute_search(Pid, Uri);
                    {ok, Doc} -> 
                        lager:debug("Search got cache hit: ~p", [Doc]),
                        Doc
    end,
    Data.

%% @doc Update an existing key/value pair.
-spec nebula2_riak:update(pid(),
                      object_oid(),          %% Oid
                      map()                  %% Data to store
                     ) -> {ok, object_oid()}.
update(Pid, Oid, Data) ->
    lager:debug("nebula2_riak:update Updating object ~p", [Oid]),
    case get(Pid, Oid) of
        {error, E} ->
            lager:debug("riak_put got error ~p from search", [E]),
            {error, E};
        {ok, _} ->
            lager:debug("riak_update updating ~p with ~p", [Oid, Data]),
            do_put(Pid, Oid, Data)
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Create a search key.
-spec nebula2_riak:create_sk(map()                    %% map of cdmi data
                            ) -> map().               %% updated cdmi data
create_sk(Data) ->
    DomainURI = binary_to_list(maps:get(<<"domainURI">>, Data, <<"/cdmi_domains/system_domain/">>)),
    ParentURI = binary_to_list(maps:get(<<"parentURI">>, Data, <<"/root/">>)),
    ObjectName = binary_to_list(maps:get(<<"objectName">>, Data)),
    Metadata = maps:get(<<"metadata">>, Data, jsx:decode(list_to_binary("{\"cdmi_owner\": \"" ++ ?DEFAULT_ADMINISTRATOR ++ "\"}"), [return_maps])),
    Owner = binary_to_list(maps:get(<<"cdmi_owner">>, Metadata, <<?DEFAULT_ADMINISTRATOR>>)),
    SearchKey = base64:encode("sk" ++ DomainURI ++ Owner ++ ParentURI ++ ObjectName),
    Metadata2 = maps:put(<<"nebula_sk">>, SearchKey, Metadata),
    lager:debug("create_sk DomainURI: ~p~n ParentURI: ~p~n ObjectName: ~p~n Metadata: ~p~n Owner: ~p~n SearchKey: ~p~n Metadata2: ~p",
                [DomainURI, ParentURI, ObjectName, Metadata, Owner, SearchKey, Metadata2]),
    maps:put(<<"metadata">>, Metadata2, Data).
    
%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {ok, string()}.
execute_search(Pid, Uri) ->
    lager:debug("nebula2:execute_search: Search Uri: ~p", [Uri]),
    Query = list_to_binary("metadata.nebula_sk:" ++ Uri),
    Index = list_to_binary(?CDMI_INDEX),
    lager:debug("Query: ~p Index: ~p", [Query, ?CDMI_INDEX]),
    {ok, Results} = riakc_pb_socket:search(Pid, Index, Query),
    NumFound = Results#search_results.num_found,
    Doc = Results#search_results.docs,
    lager:debug("Search returned ~p results", [NumFound]),
    lager:debug("Search returned documents: ~p", [Doc]),
    Response = case NumFound of
                    0 ->
                        lager:debug("search found zero results"),
                        {error, 404}; %% Return 404
                    1 ->
                        lager:debug("search found one result"),
                        fetch(Pid, Doc);
                    N ->
                        lager:debug("search returned ~p results", [N]),
                        {error, 500} %% Something's funky - return 500
    end,
    {ok, _R} = mcd:set(?MEMCACHE, Uri, Response, ?MEMCACHE_EXPIRY),
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, string()}.
fetch(Pid, Data) ->
    lager:debug("nebula2:fetch: fetching key: ~p", [Data]),
    [{<<?CDMI_INDEX>>, Results}] = Data,
    ObjectId = binary_to_list(proplists:get_value(<<"_yz_rk">>, Results)),
    lager:debug("fetch found ObjectId: ~p", [ObjectId]),
    nebula2_riak:get(Pid, ObjectId).

%% prepend 'cdmi' to object name
prepend_name(Data) ->
    {<<"objectName">>, OldName} = lists:keyfind(<<"objectName">>, 1, Data),
    NewName = list_to_binary(?NAME_PREFIX ++ binary_to_list(OldName)),
    lists:keyreplace(<<"objectName">>, 1, Data, {<<"objectName">>, NewName}).

%% strip 'cdmi' from object name
strip_name(Data) ->
    lager:debug("strip_name parm ~p", [Data]),
    Got = lists:keyfind(<<"objectName">>, 1, Data),
    lager:debug("strip_name got ~p", [Got]),
    OldName = case lists:keyfind(<<"objectName">>, 1, Data) of
                  {<<"objectName">>, Name} ->
                      Name;
                  Failure ->
                      lager:debug("strip_name failure: ~p", [Failure]),
                      Failure
              end,
    lager:debug("Stripping cdmi from front of ~p", [OldName]),
    case OldName of
        <<"/">> ->
            Data;
        _Other ->
            {_, NewName} = lists:split(string:len(?NAME_PREFIX), binary_to_list(OldName)),
            lists:keyreplace(<<"objectName">>, 1, Data, {<<"objectName">>, list_to_binary(NewName)})
    end.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
prepend_name_test() ->
    Data = prepend_name([{<<"objectID">>,<<"oid">>},
                         {<<"objectName">>,<<"/objectName">>},
                         {<<"parentID">>,<<"parentId">>}]),
    ?assert(lists:keyfind(<<"objectName">>, 1, Data) == {<<"objectName">>, <<"cdmi/objectName">>}).
    
strip_name_test() ->
    Data = strip_name([{<<"objectID">>,<<"oid">>},
                       {<<"objectName">>,<<"cdmi/objectName">>},
                       {<<"parentID">>,<<"parentId">>}]),
    ?assert(lists:keyfind(<<"objectName">>, 1, Data) == {<<"objectName">>, <<"/objectName">>}).

strip_name_root_test() ->
    Data = strip_name([{<<"objectID">>,<<"oid">>},
                       {<<"objectName">>,<<"/">>},
                       {<<"parentID">>,<<"parentId">>}]),
    ?assert(lists:keyfind(<<"objectName">>, 1, Data) == {<<"objectName">>, <<"/">>}).

create_sk_test() ->
    Data = "{\"domainURI\": \"/cdmi_domains/some_domain\",\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {\"cdmi_owner\": \"my_id\"}}",
    Data2 = create_sk(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3NvbWVfZG9tYWlubXlfaWQvbXkvcGFyZW50QW5PYmplY3ROYW1l">> == SearchKey).

%% Test with no {metadata: {cdmi_owner: ""}}
create_sk2_test() ->
    Data = "{\"domainURI\": \"/cdmi_domains/some_domain\",\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {}}",
    Data2 = create_sk(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3NvbWVfZG9tYWluYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).

%% Test with no {domainURI: }
create_sk3_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {\"cdmi_owner\": \"my_id\"}}",
    Data2 = create_sk(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vbXlfaWQvbXkvcGFyZW50QW5PYmplY3ROYW1l">> == SearchKey).

%% Test with no {domainURI: } AND no {metadata: {cdmi_owner}}
create_sk4_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\",\"metadata\": {}}",
    Data2 = create_sk(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).

%% Test with no {domainURI: } AND no {metadata: }
create_sk5_test() ->
    Data = "{\"parentURI\": \"/my/parent\",\"objectName\": \"AnObjectName\"}",
    Data2 = create_sk(jsx:decode(list_to_binary(Data), [return_maps])),
    Metadata = maps:get(<<"metadata">>, Data2),
    SearchKey = maps:get(<<"nebula_sk">>, Metadata),
    ?assert(<<"c2svY2RtaV9kb21haW5zL3N5c3RlbV9kb21haW4vYWRtaW5pc3RyYXRvci9teS9wYXJlbnRBbk9iamVjdE5hbWU=">> == SearchKey).
-endif.