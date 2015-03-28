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
         put/3,
         ping/1,
         search/2]).

%% @doc Get a value from riak by bucket type, bucket and key.
-spec nebula2_riak:get(pid(), object_oid()) -> {ok, json_value()}.
get(Pid, []) ->
    get(Pid, "/");
get(Pid, Oid) ->
    lager:info("Type: ~p Bucket: ~p Oid: ~p", [?BUCKET_TYPE, ?BUCKET_NAME, Oid]),
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
    lager:info("Get response is ~p", [Response]),
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
                      object_oid(),              %% ObjectID
                      binary()                   %% Data to store
                     ) -> {ok, object_oid()}.
put(Pid, Oid, Data) ->
    Object = riakc_obj:new({list_to_binary(?BUCKET_TYPE),
                            list_to_binary(?BUCKET_NAME)},
                            list_to_binary(Oid),
                            Data,
                            list_to_binary("application/json")),
    lager:debug("Riak Object: ~p", [Object]),
    Response = case riakc_pb_socket:put(Pid, Object) of
                    ok ->
                        {ok, Oid};
                    {error, Term} ->
                        {error, Term}
    end,
    {Response, Oid}.

%% @doc Search an index for objects.
-spec nebula2_riak:search(pid(),
                         search_predicate() %% Search predicate
                        ) -> {ok, string()}.
search(Pid, Path) ->
    Tokens = string:tokens(Path, "/"),
    Name = case string:right(Path, 1) of
                "/" -> lists:last(Tokens) ++ "/";
                _   -> lists:last(Tokens)
    end,
    Parent = "/" ++ string:join(lists:droplast(Tokens), "/") ++ "/",
    Predicate = "objectName:" ++ Name ++ " AND parentURI:\\" ++ Parent,
    lager:info("Searching ~p For: ~p", [?CDMI_INDEX, Predicate]),
    {ok, Results} = riakc_pb_socket:search(Pid,
                                           list_to_binary(?CDMI_INDEX),
                                           list_to_binary(Predicate)),
    lager:info("Search result: ~p", [Results]),
    Docs = Results#search_results.docs,
    lager:info("Search Doc: ~p", [Docs]),
    [{_, List}|_] = Docs,
    Dict = dict:from_list(List),
    Response = case dict:find(<<"objectID">>, Dict) of
                {ok, ObjectID} ->
                    {ok, binary_to_list(ObjectID)};
                {error, Term} ->
                    {error, Term}
    end,
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================
