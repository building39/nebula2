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
         post/3,
         put/4,
         ping/1,
         search/2]).

%% @doc Get a value from riak by bucket type, bucket and key.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, json_value()}.
get(Pid, []) ->
    get(Pid, "/");
get(Pid, Oid) ->
    Response = case riakc_pb_socket:get(Pid,
                                        {list_to_binary(?BUCKET_TYPE),
                                         list_to_binary(?BUCKET_NAME)},
                                         list_to_binary(Oid)) of
                    {ok, Object} ->
                        Contents = binary_to_list(riakc_obj:get_value(Object)),
                        {ok, Contents};
                    {error, Term} ->
                        {error, Term}
    end,
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
    ObjectName2 = "cdmi/" ++ ObjectName,
    case search(Pid, ObjectName2) of
        {error, 404} ->
            do_put(Pid, Oid, Data);
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
search(Pid, Uri) ->
    lager:debug("nebula2_riak:search searching for uri: ~p", [Uri]),
    Response = case mcd:get(?MEMCACHE, Uri) of
                    {error, notfound} ->
                        execute_search(Pid, Uri);
                    {ok, Doc} -> 
                        Doc
    end,
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Execute a search.
-spec nebula2_riak:execute_search(pid(),              %% Riak client pid.
                                  search_predicate()  %% URI.
                                 ) -> {ok, string()}.
execute_search(Pid, Uri) ->
    lager:debug("nebula2:execute_search: Search Uri: ~p", [Uri]),
    {ObjectName, ParentUri, _} = nebula2_utils:get_name_and_parent(Pid, Uri),
    lager:debug("execute_search: ObjectName: ~p ParentUri: ~p", [ObjectName, ParentUri]),
    Query = case ParentUri of
                    "" ->
                        "objectName: cdmi/";
                     _  ->
                        "objectName:" ++  ObjectName ++ 
                        " AND parentURI:" ++ ParentUri
                end,
    lager:debug("Query: ~p", [Query]),
    {ok, {search_results, Results, _, NumFound}} = riakc_pb_socket:search(Pid,
                                                                          list_to_binary(?CDMI_INDEX),
                                                                          list_to_binary(Query)),
    Response = case NumFound of
                    0 ->
                        {error, 404}; %% Return 404
                    1 ->
                        fetch(Pid, Results);
                    _ ->
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
    nebula2_riak:get(Pid, ObjectId).

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.