%% @author mmartin
%% @doc @todo Add description to nebula2_riak.

-module(nebula2_riak).
-compile([{parse_transform, lager_transform}]).

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
                      binary()               %% Data to store
                     ) -> {ok, object_oid()}.
put(Pid, ObjectName, Oid, Data) ->
    case search(Pid, ObjectName) of
        {error, 404} ->
            do_put(Pid, Oid, Data);
        {error, E} ->
            {error, E};
        {ok, _} ->
            {error, 409}
    end.

do_put(Pid, Oid, Data) ->
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            list_to_binary(Oid),
                            Data,
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
    search(Pid, "root/");
search(Pid, "/") ->
   search(Pid, "root/");
search(Pid, Uri) ->
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
    {Name, ParentUri} = nebula2_utils:get_name_and_parent(Uri),
    Query = case Uri of
                    "root/" ->
                        "metadata.nebula_objectName: root/";
                     _  ->
                        "metadata.nebula_objectName:" ++ 
                            Name ++ 
                            " AND metadata.nebula_parentURI:" ++ ParentUri
                end,
    {ok, {search_results, Results, _, NumFound}} = riakc_pb_socket:search(Pid,
                                                                          list_to_binary(?CDMI_INDEX),
                                                                          list_to_binary(Query)),
    lager:info("Search Uri: ~p", [Uri]),
    lager:info("Query: ~p", [Query]),
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
    [{<<?CDMI_INDEX>>, Results}] = Data,
    ObjectId = binary_to_list(proplists:get_value(<<"_yz_rk">>, Results)),
    nebula2_riak:get(Pid, ObjectId).
