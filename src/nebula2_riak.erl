%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
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
available(Pid) ->
    lager:debug("Entry"),
    case riakc_pb_socket:ping(Pid) of
        pong ->
                true;
        _R ->
             false
    end.

%% @doc Delete an object from riak by bucket type, bucket and key.
-spec nebula2_riak:delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    lager:debug("Entry"),
    riakc_pb_socket:delete(Pid,
                           {list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                           Oid).

%% @doc Get a value from riak by bucket type, bucket and key. Return string.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, map()}|{error, term()}.
get(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    get(Pid, binary_to_list(Oid));
get(Pid, Oid) ->
    lager:debug("Entry"),
    case riakc_pb_socket:get(Pid, {list_to_binary(?BUCKET_TYPE),
                                   list_to_binary(?BUCKET_NAME)},
                                   list_to_binary(Oid)) of
                {ok, Object} ->
                    Data = jsx:decode(riakc_obj:get_value(Object), [return_maps]),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
    end.

%% @doc Get the domain maps.
-spec nebula2_riak:get_domain_maps(pid(), object_path()) -> binary().
get_domain_maps(Pid, Path) ->
    lager:debug("Entry"),
    execute_search(Pid, "sp:\\" ++ Path).

%% @doc Put a value with content type to riak by bucket type, bucket and key. 
-spec nebula2_riak:put(pid(),
                      object_oid(),   %% Oid
                      map()           %% Data to store
                     ) -> {'error', _} | {'ok', _}.
put(Pid, Oid, Data) ->
    lager:debug("Entry"),
    do_put(Pid, Oid, Data).

-spec nebula2_riak:do_put(pid(), object_oid(), map()) -> {ok|error, object_oid()|term()}.
do_put(Pid, Oid, Data) when is_binary(Oid) ->
    lager:debug("Entry"),
    do_put(Pid, binary_to_list(Oid), Data);
do_put(Pid, Oid, Data) ->
    lager:debug("Entry"),
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
    lager:debug("Entry"),
    % lager:debug("State: ~p", [State]),
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
    lager:debug("Entry"),
    update(Pid, binary_to_list(Oid), Data);
update(Pid, Oid, Data) ->
    % lager:debug("nebula2_riak:update Updating object ~p", [Oid]),
    lager:debug("Entry"),
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
    lager:debug("Entry"),
    Index = list_to_binary(?CDMI_INDEX),
    lager:debug("Query: ~p", [Query]),
    Response = case riakc_pb_socket:search(Pid, Index, Query) of
                   {ok, Results} ->
                       case Results#search_results.num_found of
                           0 ->
                               lager:debug("Not Found"),
                               {error, 404}; %% Return 404
                           1 ->
                               [{_, Doc}] = Results#search_results.docs,
                               lager:debug("Doc: ~p", [Doc]),
                               fetch(Pid, Doc);
                           _N ->
                               lager:debug("Something funky"),
                               {error, 500} %% Something's funky - return 500
                       end;
                   _ ->
                       lager:debug("WTF?"),
                       {error, 404}
               end,
    Response.

%% @doc Fetch document.
-spec nebula2_riak:fetch(pid(), list()) -> {ok, map()}.
fetch(Pid, Data) ->
    lager:debug("Entry"),
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
