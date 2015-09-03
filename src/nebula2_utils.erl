%% @author mmartin
%% @doc Various useful functions.

-module(nebula2_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
         beginswith/2,
         create_object/4,
         delete/2,
         delete_child_from_parent/4,
         extract_parentURI/1,
         get_object_name/2,
         get_object_oid/1,
         get_object_oid/2,
         get_parent_uri/1,
         get_time/0,
         handle_content_type/1,
         make_key/0,
         update_parent/4
        ]).

%% @doc Check if a string begins with a certain substring.
-spec nebula2_utils:beginswith(string(), string()) -> boolean().
beginswith(Str, Substr) ->
    ?LOG_ENTRY,
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% @doc Create a CDMI object
-spec nebula2_utils:create_object(map(), State, object_type(), list()) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
create_object(Body, State, ObjectType, DomainName) ->
    ?LOG_ENTRY,
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    ParentUri = nebula2_utils:get_parent_uri(Path),
    case nebula2_utils:get_object_oid(ParentUri, State) of
        {ok, ParentId} ->
            Path2 = path_to_list(Path),
            ObjectName = string:concat(lists:last(Path2), "/"),
            Data = sanitize_body([<<"objectID">>,
                                  <<"objectName">>,
                                  <<"parentID">>,
                                  <<"parentURI">>,
                                  <<"completionStatus">>],
                                 Body),
            Oid = nebula2_utils:make_key(),
            Location = list_to_binary(nebula2_app:cdmi_location()),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            NewMetadata = maps:from_list([{<<"cdmi_atime">>, Tstamp},
                                          {<<"cdmi_ctime">>, Tstamp},
                                          {<<"cdmi_mtime">>, Tstamp},
                                          {<<"cdmi_versions_count_provided">>, <<"0">>},
                                          {<<"nebula_data_location">>, [Location]},
                                          {<<"nebula_modified_by">>, <<"">>}
                                         ]),
            OldMetadata = maps:get(<<"metadata">>, Body, maps:new()),
            Metadata2 = maps:merge(OldMetadata, NewMetadata),
            CapabilitiesURI = case maps:get(<<"capabilitiesURI">>, Body, none) of
                                none ->   get_capability_uri(ObjectType);
                                Curi ->   Curi
                              end,
            Data2 = maps:merge(maps:from_list([{<<"objectType">>, list_to_binary(ObjectType)},
                                               {<<"objectID">>, list_to_binary(Oid)},
                                               {<<"objectName">>, list_to_binary(ObjectName)},
                                               {<<"parentID">>, ParentId},
                                               {<<"parentURI">>, list_to_binary(ParentUri)},
                                               {<<"metadata">>, Metadata2},
                                               {<<"capabilitiesURI">>, list_to_binary(CapabilitiesURI)},
                                               {<<"domainURI">>, list_to_binary(DomainName)},
                                               {<<"completionStatus">>, <<"Complete">>}]),
                               Data),
            {ok, Oid} = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
            ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            {true, Data2};
        {notfound, _} ->
            % lager:debug("create_object: Did not find parent"),
            pooler:return_member(riak_pool, Pid),
            false
    end.

%% @doc Delete an object and all objects underneath it.
-spec delete(map(), cdmi_state()) -> ok | {error, term()}.
delete(Data, State) ->
    {Pid, _} = State,
    Children = maps:get(<<"children">>, Data, []),
    handle_delete(Pid, Data, State, Children).

%% TODO: Make delete asynchronous
handle_delete(Pid, Data, _, []) ->
    nebula2_riak:delete(Pid, maps:get(<<"objecdID">>, Data));
handle_delete(Pid, Data, State, [Child | Tail]) ->
    ChildPath = maps:get(<<"objectName">>, Data) ++ Child,
    ChildData = nebula2_riak:search(ChildPath, State),
    GrandChildren = maps:get(<<"children">>, ChildData, []),
    handle_delete(Pid, ChildData, State, GrandChildren),
    handle_delete(Pid, Data, State, Tail),
    nebula2_riak:delete(Pid, maps:get(<<"objecdID">>, Data)).

%% @doc Delete a child from its parent
-spec delete_child_from_parent(object_oid(), string(), string(), pid()) -> ok | {error, term()}.
delete_child_from_parent(ParentId, Path, ObjectType, Pid) ->
    ?LOG_ENTRY,
    N = case length(string:tokens(Path, "/")) of
            0 ->
                "";
            _Other ->
                lists:last(string:tokens(Path, "/"))
        end,
    Name = case ObjectType of
               ?CONTENT_TYPE_CDMI_CAPABILITY ->
                   N ++ "/";
               ?CONTENT_TYPE_CDMI_CONTAINER ->
                   N ++ "/";
               ?CONTENT_TYPE_CDMI_DOMAIN ->
                   N ++ "/";
               _ -> 
                   N
           end,
    {ok, Parent} = nebula2_riak:get(Pid, ParentId),
    _X = maps:get(<<"children">>, Parent, ""),
    Children = maps:get(<<"children">>, Parent, ""),
    NewParent1 = case Children of
                     [] ->
                         maps:remove(<<"children">>, Parent);
                     _  ->
                         maps:put(<<"children">>, list:delete(Name, Children), Parent)
                 end,
    NewParent2 = case maps:get(<<"childrange">>, Parent, "") of
                    "0-0" ->
                        maps:remove(<<"childrange">>, Parent);
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         maps:put(<<"childrange">>, list_to_binary(lists:concat(["0-", Num - 1])), NewParent1)
                 end,
    nebula2_riak:update(Pid, ParentId, NewParent2).
%% @doc Extract the Parent URI from the path.
-spec extract_parentURI(list()) -> list().
extract_parentURI(Path) ->
    ?LOG_ENTRY,
    extract_parentURI(Path, "") ++ "/".
-spec extract_parentURI(list(), list()) -> list().
extract_parentURI([], Acc) ->
    Acc;
extract_parentURI([H|T], Acc) when is_binary(H) ->
    Acc2 = Acc ++ "/" ++ binary_to_list(H),
    extract_parentURI(T, Acc2);
extract_parentURI([H|T], Acc) ->
    Acc2 = Acc ++ "/" ++ H,
    extract_parentURI(T, Acc2).

%% @doc Get the object name.
-spec get_object_name(list(), string()) -> string().
get_object_name(Parts, Path) ->
    ?LOG_ENTRY,
    case Parts of
        [] ->
            "/";
        _ -> case string:right(Path, 1) of
                "/" ->
                    lists:last(Parts) ++ "/";
                _ ->
                    lists:last(Parts)
             end
    end.

%% @doc Get the object's oid.
-spec nebula2_utils:get_object_oid(map()) -> {ok, term()}|{notfound, string()}.
get_object_oid(State) ->
    ?LOG_ENTRY,
    {_, Path} = State,
    get_object_oid(Path, State).

%% @doc Get the object's oid.
-spec nebula2_utils:get_object_oid(string(), tuple()) -> {ok, term()}|{notfound, string()}.
get_object_oid(Path, State) ->
    ?LOG_ENTRY,
    Oid = case nebula2_riak:search(Path, State) of
            {error,_} -> 
                {notfound, ""};
            {ok, Data} ->
                % lager:debug("Data: ~p", [Data]),
                {ok, maps:get(<<"objectID">>, Data)}
          end,
    % lager:debug("get_object_oid: ~p", [Oid]),
    % lager:debug("get_object_oid: Path: ~p", [Path]),
    Oid.

%% @doc Construct the object's parent URI.
-spec get_parent_uri(list()) -> string().
get_parent_uri(Parts) ->
    ?LOG_ENTRY,
    ParentUri = case length(Parts) of
                    0 ->
                        "";     %% Root has no parent
                    1 ->
                        "/";
                    _ ->
                        nebula2_utils:extract_parentURI(lists:droplast(Parts))
                end,
    % lager:debug("Calculated ParentURI to be ~p", [ParentUri]),
    ParentUri.

%% @doc Return current time in ISO 8601:2004 extended representation.
-spec nebula2_utils:get_time() -> string().
get_time() ->
    ?LOG_ENTRY,
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:now_to_universal_time(erlang:now()),
    binary_to_list(iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.000000Z",
                  [Year, Month, Day, Hour, Minute, Second]))).

%% Get the content type for the request
-spec handle_content_type({pid(),map()}) -> string().
handle_content_type(State) ->
    ?LOG_ENTRY,
    {_, EnvMap} = State,
    case maps:get(<<"content-type">>, EnvMap, "") of
        "" ->
            Path = maps:get(<<"path">>, EnvMap),
            {ok, Data} = nebula2_riak:search(Path, State),
            binary_to_list(maps:get(<<"objectType">>, Data));
        CT ->
            binary_to_list(CT)
    end.

%% @doc Make a primary key for storing a new object.
-spec nebula2_utils:make_key() -> object_oid().
make_key() ->
    ?LOG_ENTRY,
    Uid = re:replace(uuid:to_string(uuid:uuid4()), "-", "", [global, {return, list}]),
    Temp = Uid ++ ?OID_SUFFIX ++ "0000",
    Crc = integer_to_list(crc16:crc16(Temp), 16),
    Uid ++ ?OID_SUFFIX ++ Crc.

%% @doc Update a parent object with a new child
-spec update_parent(object_oid(), string(), string(), pid()) -> ok | {error, term()}.
update_parent(Root, _, _, _) when Root == ""; Root == <<"">> ->
    %% Must be the root, since there is no parent.
    ?LOG_ENTRY,
    ok;
update_parent(ParentId, Path, ObjectType, Pid) ->
    ?LOG_ENTRY,
    % lager:debug("Entry nebula2_utils:update_parent: ~p ~p ~p ~p", [ParentId, Path, ObjectType, Pid]),
    N = case length(string:tokens(Path, "/")) of
            0 ->
                "";
            _Other ->
                lists:last(string:tokens(Path, "/"))
        end,
    % lager:debug("update_parent: N: ~p", [N]),
    % lager:debug("update_parent: child type: ~p", [ObjectType]),
    Name = case ObjectType of
               ?CONTENT_TYPE_CDMI_CAPABILITY ->
                   N ++ "/";
               ?CONTENT_TYPE_CDMI_CONTAINER ->
                   N ++ "/";
               ?CONTENT_TYPE_CDMI_DOMAIN ->
                   N ++ "/";
               _ -> 
                   N
           end,
    {ok, Parent} = nebula2_riak:get(Pid, ParentId),
    % lager:debug("update_parent got parent: ~p", [Parent]),
    % lager:debug("updating parent with child: ~p", [Name]),
    _X = maps:get(<<"children">>, Parent, ""),
    % lager:debug("capabilities update_parent: _X: ~p", [_X]),
    Children = case maps:get(<<"children">>, Parent, "") of
                     "" ->
                         [list_to_binary(Name)];
                     Ch ->
                         % lager:debug("capabilities update_parent: Ch: ~p", [Ch]),
                         lists:append(Ch, [list_to_binary(Name)])
                 end,
    % lager:debug("Children is now: ~p", [Children]),
    ChildRange = case maps:get(<<"childrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    % lager:debug("ChildRange is now: ~p", [ChildRange]),
    NewParent1 = maps:put(<<"children">>, Children, Parent),
    NewParent2 = maps:put(<<"childrange">>, list_to_binary(ChildRange), NewParent1),
    % lager:debug("NewParent2: ~p", [NewParent2]),
    % lager:debug("new parent: ~p", [NewParent2]),
    nebula2_riak:update(Pid, ParentId, NewParent2).

%% ====================================================================
%% Internal functions
%% ====================================================================
get_capability_uri(ObjectType) ->
    ?LOG_ENTRY,
    case ObjectType of
        ?CONTENT_TYPE_CDMI_CAPABILITY ->
            ?DOMAIN_SUMMARY_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_CONTAINER ->
            ?CONTAINER_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_DATAOBJECT ->
            ?DATAOBJECT_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_DOMAIN ->
            ?DOMAIN_CAPABILITY_URI
    end.

path_to_list(Path) ->
    path_to_list(Path, []).

path_to_list([], Acc) ->
    Acc;
path_to_list([H|T], Acc) when is_binary(H)->
    Acc2 = [Acc, binary_to_list(H)],
    path_to_list(T, Acc2);
path_to_list([H|T], Acc) ->
    Acc2 = [Acc, H],
    path_to_list(T, Acc2).

sanitize_body([], Body) ->
    ?LOG_ENTRY,
    Body;
sanitize_body([H|T], Body) ->
    ?LOG_ENTRY,
    sanitize_body(T, maps:remove(H, Body)).

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
%% @doc Test the beginswith/2 function.
beginswith_true_test() -> 
    ?assert(beginswith("abcdef", "abc")).
beginswith_false_test() -> 
    ?assertNot(beginswith("abcdef", "def")).

%% @doc Test the get_parent/2 function.
get_parent_root_test() -> 
    ?assert(get_parent(ok, "/") == {ok, "", ""}).
get_parent_object_test() ->
    Data =  "{\"objectID\":\"parent_oid\",}",
    Path = "/some/object",
    meck:new(nebula2_riak, [non_strict]),
    meck:expect(nebula2_riak, search, fun(ok, Path) -> {ok, Data} end),
    ?assert(get_parent(ok, Path) == {ok, "/some/", "parent_oid"}),
    meck:unload(nebula2_riak).
get_parent_notfound_test() ->
    Path = "/some/object",
    meck:new(nebula2_riak, [non_strict]),
    meck:expect(nebula2_riak, search, fun(ok, Path) -> {error, notfound} end),
    ?assert(get_parent(ok, Path) == {error, notfound, "/some/"}),
    meck:unload(nebula2_riak).
-endif.
