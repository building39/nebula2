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
		 check_base64/1,
         create_object/4,
         create_object/5,
         delete/1,
         delete_child_from_parent/3,
         extract_parentURI/1,
         generate_hash/2,
         get_object_name/2,
         get_object_oid/1,
         get_object_oid/2,
         get_parent_uri/1,
         get_time/0,
         handle_content_type/1,
         make_key/0,
         make_search_key/1,
         update_data_system_metadata/3,
         update_data_system_metadata/4,
         update_parent/4
        ]).

%% @doc Check if a string begins with a certain substring.
-spec nebula2_utils:beginswith(string(), string()) -> boolean().
beginswith(Str, Substr) ->
    lager:debug("Entry"),
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% Check base64 encoding
-spec nebula2_utils:check_base64(binary() | list()) -> boolean().
check_base64(Data) ->
	case maps:is_key(<<"valuetransferencoding">>, Data) of
		false ->
			true;
		true ->
			try base64:decode(binary_to_list(maps:get(<<"value">>, Data))) of
				_ -> 
					true
			catch
				_:_ ->
					false
			  end
	end.

%% @doc Create a CDMI object
-spec nebula2_utils:create_object(cowboy_req:req(), {pid(), map()}, object_type(), map()) -> {boolean(), cowboy_req:req(), {pid(), map()}}.
create_object(Req, State, ObjectType, Body) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
    case nebula2_utils:get_object_oid(nebula2_utils:get_parent_uri(Path), State) of
        {ok, ParentId} ->
            {ok, Parent} = nebula2_db:read(Pid, ParentId),
            DomainName = maps:get(<<"domainURI">>, Parent, ""),
            create_object(Req, State, ObjectType, DomainName, Parent, Body);
        {notfound, _} ->
            pooler:return_member(riak_pool, Pid),
            false
    end.

-spec nebula2_utils:create_object(cowboy_req:req(), {pid(), map()}, object_type(), map(), binary() | string()) -> {boolean(), cowboy_req:req(), {pid(), map()}}.
create_object(Req, State, ObjectType, Body, DomainName) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
    case nebula2_utils:get_object_oid(nebula2_utils:get_parent_uri(Path), State) of
        {ok, ParentId} ->
            {ok, Parent} = nebula2_db:read(Pid, ParentId),
            create_object(Req, State, ObjectType, DomainName, Parent, Body);
        {notfound, _} ->
            pooler:return_member(riak_pool, Pid),
            false
    end.

%% @doc Delete an object and all objects underneath it.
-spec delete(cdmi_state()) -> ok | {error, term()}.
delete(State) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Data = maps:get(<<"object_map">>, EnvMap),
    {Pid, _} = State,
    Children = maps:get(<<"children">>, Data, []),
    handle_delete(Pid, Data, State, Children).

%% @doc Delete a child from its parent
-spec delete_child_from_parent(pid(), object_oid(), string()) -> ok | {error, term()}.
delete_child_from_parent(Pid, ParentId, Name) ->
    lager:debug("Entry"),
    {ok, Parent} = nebula2_db:read(Pid, ParentId),
    Children = maps:get(<<"children">>, Parent, ""),
    NewParent1 = case Children of
                     [] ->
                         maps:remove(<<"children">>, Parent);
                     _  ->
                         NewChildren = lists:delete(Name, Children),
                         maps:put(<<"children">>, NewChildren, Parent)
                 end,
    NewParent2 = case maps:get(<<"childrenrange">>, Parent, "") of
                    "0-0" ->
                        maps:remove(<<"childrenrange">>, Parent);
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         maps:put(<<"childrenrange">>, list_to_binary(lists:concat(["0-", Num - 1])), NewParent1)
                 end,
    nebula2_db:update(Pid, ParentId, NewParent2).

%% @doc Extract the Parent URI from the path.
-spec extract_parentURI(list()) -> list().
extract_parentURI(Path) ->
    lager:debug("Entry"),
    extract_parentURI(Path, "") ++ "/".

%% @doc Generate hash.
-spec generate_hash(string(), string()) -> string().
generate_hash(Method, Data) when is_binary(Data)->
    Data2 = binary_to_list(Data),
    nebula2_utils:generate(Method, Data2);
generate_hash(Method, Data) ->
    M = list_to_atom(Method),
    string:to_lower(lists:flatten([[integer_to_list(N, 16) || <<N:4>> <= crypto:hash(M, Data)]])).
    
%% @doc Get the object name.
-spec get_object_name(list(), string()) -> string().
get_object_name(Parts, Path) ->
    lager:debug("Entry"),
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
    lager:debug("Entry"),
    {_, Path} = State,
    get_object_oid(Path, State).

%% @doc Get the object's oid.
-spec nebula2_utils:get_object_oid(string() | binary(), tuple()) -> {ok, term()}|{notfound, string()}.
get_object_oid(Path, State) when is_binary(Path) ->
    get_object_oid(binary_to_list(Path), State);
get_object_oid(Path, State) ->
    lager:debug("Entry"),
    RealPath = case beginswith(Path, "/capabilities/") of
                   true ->
                       Path;
                   false ->
                       {_, EnvMap} = State,
                       binary_to_list(maps:get(<<"domainURI">>, EnvMap, <<"">>)) ++ lists:nthtail(1, Path)
               end,
    Oid = case nebula2_db:search(RealPath, State) of
            {error,_} -> 
                {notfound, ""};
            {ok, Data} ->
                {ok, maps:get(<<"objectID">>, Data)}
          end,
    Oid.

%% @doc Construct the object's parent URI.
-spec get_parent_uri(list()) -> string().
get_parent_uri(Path) ->
    lager:debug("Entry"),
    Parts = string:tokens(Path, "/"),
    ParentUri = case length(Parts) of
                    0 ->
                        "";     %% Root has no parent
                    1 ->
                        "/";
                    _ ->
                        nebula2_utils:extract_parentURI(lists:droplast(Parts))
                end,
    lager:debug("Exit: ~p", [ParentUri]),
    list_to_binary(ParentUri).

%% @doc Return current time in ISO 8601:2004 extended representation.
-spec nebula2_utils:get_time() -> string().
get_time() ->
    lager:debug("Entry"),
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:now_to_universal_time(erlang:now()),
    binary_to_list(iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.000000Z",
                  [Year, Month, Day, Hour, Minute, Second]))).

%% Get the content type for the request
-spec handle_content_type({pid(),map()}) -> string().
handle_content_type(State) ->
    lager:debug("Entry"),
    {_, EnvMap} = State,
    case binary_to_list(maps:get(<<"content-type">>, EnvMap, <<"">>)) of
        "" ->
            Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
            {ok, Data} = nebula2_db:search(Path, State),
            binary_to_list(maps:get(<<"objectType">>, Data));
        CT ->
            binary_to_list(CT)
    end.

%% @doc Make a primary key for storing a new object.
-spec nebula2_utils:make_key() -> object_oid().
make_key() ->
    lager:debug("Entry"),
    Uid = re:replace(uuid:to_string(uuid:uuid4()), "-", "", [global, {return, list}]),
    Temp = Uid ++ ?OID_SUFFIX ++ "0000",
    Crc = integer_to_list(crc16:crc16(Temp), 16),
    Uid ++ ?OID_SUFFIX ++ Crc.

-spec make_search_key(map()) -> list().
make_search_key(Data) ->
    lager:debug("Entry"),
    lager:debug("Data: ~p", [Data]),
    ObjectName = binary_to_list(maps:get(<<"objectName">>, Data)),
    ParentUri = binary_to_list(maps:get(<<"parentURI">>, Data, <<"">>)),
    Path = binary_to_list(maps:get(<<"path">>, Data, <<"">>)),
    DomainUri = case beginswith(Path, "/cdmi_capabilities/") of
                    false ->
                        binary_to_list(maps:get(<<"domainURI">>, Data, <<"">>));
                    true ->
                        ""
                end,
    lager:debug("Path: ~p", [Path]),
    lager:debug("ObjectName: ~p", [ObjectName]),
    lager:debug("DomainUri: ~p", [DomainUri]),
    lager:debug("ParentUri: ~p", [ParentUri]),
    Key = DomainUri ++ ParentUri ++ ObjectName,
    Key2 = case string:left(Key, string:str(Key, "//")) of
               [] ->
                   lager:debug("Normal key: ~p", [Key]),
                   Key;
               Part1 ->
                   NewKey = Part1 ++ string:substr(Key, string:str(Key, "//") + 2),
                   lager:debug("Abnormal key: ~p ~p", [Part1, NewKey]),
                   NewKey
           end,
    lager:debug("Key: ~p", [Key2]),
    list_to_binary(Key2).

%% Update Metadata
-spec nebula2_utils:update_data_system_metadata(list(),map(), cdmi_state()) -> map().
update_data_system_metadata(CList, Data, State) ->
    lager:debug("Entry"),
    CapabilitiesURI = maps:get(<<"capabilitiesURI">>, Data, []),
    nebula2_utils:update_data_system_metadata(CList, Data, CapabilitiesURI, State).

-spec nebula2_utils:update_data_system_metadata(list(),map(), string(), cdmi_state()) -> map().
update_data_system_metadata(_CList, Data, [], _State) ->
    lager:debug("Entry"),
    Data;
update_data_system_metadata(CList, Data, CapabilitiesURI, State) when is_binary(CapabilitiesURI) ->
    update_data_system_metadata(CList, Data, binary_to_list(CapabilitiesURI), State);
update_data_system_metadata(CList, Data, CapabilitiesURI, State) ->
    lager:debug("Entry"),
    lager:debug("Cap URI: ~p", [CapabilitiesURI]),
    lager:debug("State: ~p",[State]),
    {ok, C1} = nebula2_db:search(CapabilitiesURI, State),
    Capabilities = maps:get(<<"capabilities">>, C1),
    CList2 = maps:to_list(maps:with(CList, Capabilities)),
    nebula2_capabilities:apply_metadata_capabilities(CList2, Data).

%% @doc Update a parent object with a new child
-spec update_parent(object_oid(), string(), string(), pid()) -> ok | {error, term()}.
update_parent(Root, _, _, _) when Root == ""; Root == <<"">> ->
    lager:debug("Entry"),
    %% Must be the root, since there is no parent.
    ok;
update_parent(ParentId, Path, ObjectType, Pid) ->
    lager:debug("Entry"),
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
    {ok, Parent} = nebula2_db:read(Pid, ParentId),
    lager:debug("Parent: ~p", [Parent]),
    Children = case maps:get(<<"children">>, Parent, "") of
                     "" ->
                         [list_to_binary(Name)];
                     Ch ->
                         lists:append(Ch, [list_to_binary(Name)])
                 end,
    ChildrenRange = case maps:get(<<"childrenrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    NewParent1 = maps:put(<<"children">>, Children, Parent),
    NewParent2 = maps:put(<<"childrenrange">>, list_to_binary(ChildrenRange), NewParent1),
    nebula2_db:update(Pid, ParentId, NewParent2).

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec nebula2_utils:create_object(cowboy_req:req(), {pid(), map()}, object_type(), list() | binary(), string(), binary()) ->
          {boolean(), cowboy_req:req(), {pid(), map()}}.
create_object(Req, State, ObjectType, DomainName, Parent, Body) when is_list(DomainName)->
    D = list_to_binary(DomainName),
    create_object(Req, State, ObjectType, D, Parent, Body);
create_object(Req, State, ObjectType, DomainName, Parent, Body) when is_binary(DomainName)->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    lager:debug("Body: ~p", [Body]),
	case check_base64(Body) of
		false ->
		   throw(badencoding);
		true ->
			true
	end,
    Path = binary_to_list(maps:get(<<"path">>, EnvMap)),
    ParentUri = nebula2_utils:get_parent_uri(Path),
    ParentId = maps:get(<<"objectID">>, Parent),
    ParentMetadata = maps:get(<<"metadata">>, Parent, maps:new()),
    Parts = string:tokens(Path, "/"),
    ObjectName = case ObjectType of
                     ?CONTENT_TYPE_CDMI_DATAOBJECT ->
                         lists:last(Parts);
                     _ ->
                         string:concat(lists:last(Parts), "/")
                 end,
    Data = sanitize_body([<<"objectID">>,
                          <<"objectName">>,
                          <<"parentID">>,
                          <<"parentURI">>,
                          <<"completionStatus">>],
                         Body),
    Oid = nebula2_utils:make_key(),
    Location = list_to_binary(application:get_env(nebula2, cdmi_location, ?DEFAULT_LOCATION)),
    Owner = maps:get(<<"auth_as">>, EnvMap, ""),
    CapabilitiesURI = case maps:get(<<"capabilitiesURI">>, Body, none) of
                        none ->   get_capability_uri(ObjectType);
                        Curi ->   Curi
                      end,
    NewMetadata = maps:from_list([
                                  {<<"nebula_data_location">>, [Location]},
                                  {<<"cdmi_owner">>, Owner}
                                 ]),
    OldMetadata = maps:get(<<"metadata">>, Body, maps:new()),
    Metadata2 = maps:merge(OldMetadata, NewMetadata),
    Metadata3 = maps:merge(ParentMetadata, Metadata2),
    Data2 = maps:merge(maps:from_list([{<<"objectType">>, list_to_binary(ObjectType)},
                                       {<<"objectID">>, list_to_binary(Oid)},
                                       {<<"objectName">>, list_to_binary(ObjectName)},
                                       {<<"parentID">>, ParentId},
                                       {<<"parentURI">>, ParentUri},
                                       {<<"capabilitiesURI">>, list_to_binary(CapabilitiesURI)},
                                       {<<"domainURI">>, DomainName},
                                       {<<"completionStatus">>, <<"Complete">>},
                                       {<<"metadata">>, Metadata3}]),
                      Data),
    Data3 = case maps:is_key(<<"value">>, Data2) of
                true ->
                    case maps:is_key(<<"valuetransferencoding">>, Data2) of
                        false ->
                            maps:put(<<"valuetransferencoding">>, <<"utf-8">>, Data2);
                        true ->
                            Data2
                     end;
                false ->
                    Data2
            end,
    Data4 = maps:put(<<"metadata">>, Metadata3, Data3),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_ctime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>,
             <<"cdmi_size">>
    ],
    Data5 = nebula2_utils:update_data_system_metadata(CList, Data4, CapabilitiesURI, State),
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data5),
    ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
    pooler:return_member(riak_pool, Pid),
    {true, Req, Data5}.

-spec extract_parentURI(list(), list()) -> list().
extract_parentURI([], Acc) ->
    Acc;
extract_parentURI([H|T], Acc) when is_binary(H) ->
    Acc2 = Acc ++ "/" ++ binary_to_list(H),
    extract_parentURI(T, Acc2);
extract_parentURI([H|T], Acc) ->
    Acc2 = Acc ++ "/" ++ H,
    extract_parentURI(T, Acc2).

get_capability_uri(ObjectType) ->
    lager:debug("Entry"),
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

%% TODO: Make delete asynchronous
handle_delete(Pid, Data, _, []) ->
    lager:debug("Entry"),
    case nebula2_db:delete(Pid, maps:get(<<"objectID">>, Data)) of
        ok ->
             ParentId = maps:get(<<"parentID">>, Data, []),
             ObjectName = maps:get(<<"objectName">>, Data),
             delete_child_from_parent(Pid, ParentId, ObjectName),
             ok;
        Other ->
            Other
    end;
handle_delete(Pid, Data, State, [Head | Tail]) ->
    lager:debug("Entry"),
    Child = binary_to_list(Head),
    ParentUri = binary_to_list(maps:get(<<"parentURI">>, Data, "")),
    ChildPath = ParentUri ++ binary_to_list(maps:get(<<"objectName">>, Data)) ++ Child,
    {ok, ChildData} = nebula2_db:search(ChildPath, State),
    GrandChildren = maps:get(<<"children">>, ChildData, []),
    handle_delete(Pid, ChildData, State, GrandChildren),
    handle_delete(Pid, Data, State, Tail),
    nebula2_db:delete(Pid, maps:get(<<"objectID">>, Data)).

sanitize_body([], Body) ->
    Body;
sanitize_body([H|T], Body) ->
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
