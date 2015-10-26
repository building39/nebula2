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
         delete_cache/1,
         delete_child_from_parent/3,
         extract_parentURI/1,
         generate_hash/2,
         get_cache/1,
         get_domain_hash/1,
         get_object_name/2,
         get_object_oid/1,
         get_object_oid/2,
         get_parent_uri/1,
         get_value/2,
         get_value/3,
         get_time/0,
         handle_content_type/1,
         make_key/0,
         make_search_key/1,
         put_value/3,
         set_cache/1,
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
            try base64:decode(binary_to_list(nebula2_utils:get_value(<<"value">>, Data))) of
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
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    case get_cache(Path) of
        {ok, Data} ->
            Data;
        _ ->
            case nebula2_utils:get_object_oid(nebula2_utils:get_parent_uri(Path), State) of
                {ok, ParentId} ->
                    {ok, Parent} = nebula2_db:read(Pid, ParentId),
                    DomainName = nebula2_utils:get_value(<<"domainURI">>, Parent, ""),
                    create_object(Req, State, ObjectType, DomainName, Parent, Body);
                {notfound, _} ->
                    pooler:return_member(riak_pool, Pid),
                    false
            end
    end.

-spec nebula2_utils:create_object(cowboy_req:req(), {pid(), map()}, object_type(), map(), binary() | string()) -> {boolean(), cowboy_req:req(), {pid(), map()}}.
create_object(Req, State, ObjectType, DomainName, Body) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    case nebula2_utils:get_object_oid(nebula2_utils:get_parent_uri(Path), State) of
        {ok, ParentId} ->
            {ok, Parent} = nebula2_db:read(Pid, ParentId),
            create_object(Req, State, ObjectType, DomainName, Parent, Body);
        {notfound, _} ->
            %% TODO: maybe leaking a pool connection here?
            pooler:return_member(riak_pool, Pid),
            false
    end.

%% @doc Delete an object and all objects underneath it.
-spec delete(cdmi_state()) -> ok | {error, term()}.
delete(State) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Data = nebula2_utils:get_value(<<"object_map">>, EnvMap),
    {Pid, _} = State,
    Children = nebula2_utils:get_value(<<"children">>, Data, []),
    handle_delete(Pid, Data, State, Children).

-spec delete_cache(object_oid()) -> {ok | error, deleted | notfound}.
delete_cache(Oid) ->
    lager:debug("Entry"),
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            SearchKey = newbula2_utils:make_search_key(Data),
            DSK = mcd:delete(?MEMCACHE, SearchKey),
            DOID = mcd:delete(?MEMCACHE, Oid),
        Response ->
            {error, notfound}
    end.

%% @doc Delete a child from its parent
-spec delete_child_from_parent(pid(), object_oid(), string()) -> ok | {error, term()}.
delete_child_from_parent(Pid, ParentId, Name) ->
    lager:debug("Entry"),
    {ok, Parent} = nebula2_db:read(Pid, ParentId),
    Children = nebula2_utils:get_value(<<"children">>, Parent, ""),
    NewParent1 = case Children of
                     [] ->
                         maps:remove(<<"children">>, Parent);
                     _  ->
                         NewChildren = lists:delete(Name, Children),
                         nebula2_utils:put_value(<<"children">>, NewChildren, Parent)
                 end,
    NewParent2 = case nebula2_utils:get_value(<<"childrenrange">>, Parent, "") of
                    "0-0" ->
                        maps:remove(<<"childrenrange">>, Parent);
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         nebula2_utils:put_value(<<"childrenrange">>, list_to_binary(lists:concat(["0-", Num - 1])), NewParent1)
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
                       get_domain_hash(<<"">>) ++ Path;
                   false ->
                       {_, EnvMap} = State,
                       get_domain_hash(nebula2_utils:get_value(<<"domainURI">>, EnvMap, <<"">>)) ++ Path
               end,
    Oid = case nebula2_db:search(RealPath, State) of
            {error,_} -> 
                {notfound, ""};
            {ok, Data} ->
                {ok, nebula2_utils:get_value(<<"objectID">>, Data)}
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

%% @doc Get a value from the data map.
-spec get_value(binary(), map()) -> binary().
get_value(Key, Map) ->
    lager:debug("Entry"),
    get_value(Key, Map, <<"">>).

%% @doc Get a value from the data map.
-spec get_value(binary(), map(), term()) -> binary().
get_value(Key, Map, Default) ->
    lager:debug("Entry"),
    case maps:is_key(<<"cdmi">>, Map) of
        true ->
            maps:get(Key, maps:get(<<"cdmi">>, Map), Default);
        false ->
            maps:get(Key, Map, Default)
    end.

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
    case binary_to_list(nebula2_utils:get_value(<<"content-type">>, EnvMap, <<"">>)) of
        "" ->
            Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
            {ok, Data} = nebula2_db:search(Path, State),
            binary_to_list(nebula2_utils:get_value(<<"objectType">>, Data));
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
    list_to_binary(Uid ++ ?OID_SUFFIX ++ Crc).

-spec make_search_key(map()) -> list().
make_search_key(Data) ->
    lager:debug("Entry"),
    lager:debug("Data: ~p", [Data]),
    ObjectName = binary_to_list(nebula2_utils:get_value(<<"objectName">>, Data)),
    ParentUri = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, Data, <<"">>)),
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, Data, <<"">>)),
    DomainUri = case beginswith(Path, "/cdmi_capabilities/") of
                    false ->
                        case beginswith(Path, "/cdmi_domains/") of
                            false ->
                                nebula2_utils:get_value(<<"domainURI">>, Data, <<"">>);
                            true ->
                                case Path == "/cdmi_domains/" of
                                    true ->     %% always belongs to /cdmi/system_domain
                                        <<"/cdmi_domains/system_domain/">>; 
                                    false ->    %% a domain always belongs to itself.
                                        list_to_binary(Path)
                                end
                        end;
                    true ->
                        lager:debug("Making key for capabilities"),
                        <<"">>
                end,
    lager:debug("Path: ~p", [Path]),
    lager:debug("ObjectName: ~p", [ObjectName]),
    lager:debug("DomainUri: ~p", [DomainUri]),
    lager:debug("ParentUri: ~p", [ParentUri]),
    Domain = get_domain_hash(DomainUri),
    lager:debug("Hashed domain: ~p", [Domain]),
    Key = Domain ++ ParentUri ++ ObjectName,
    lager:debug("Key: ~p", [Key]),
    list_to_binary(Key).

%% @doc Put a value to the data map.
-spec put_value(binary(), term(), map()) -> map().
put_value(Key, Value, Map) ->
    lager:debug("Entry"),
    case maps:is_key(<<"cdmi">>, Map) of
        true ->
            Data = maps:get(<<"cdmi">>, Map),
            Data2 = maps:put(Key, Value, Data),
            maps:put(<<"cdmi">>, Data2, Map);
        false ->
            maps:put(Key, Value, Map)
    end.
-spec set_cache(map()) -> {ok, map()}.
set_cache(Data) ->
    lager:debug("Entry"),
    SearchKey = nebula2_utils:make_search_key(Data),
    ObjectId = nebula2_utils:get_value(<<"objectID">>, Data),
    Soid = mcd:set(?MEMCACHE, ObjectId, Data, ?MEMCACHE_EXPIRY),
    SSK = mcd:set(?MEMCACHE, SearchKey, Data, ?MEMCACHE_EXPIRY),
    SSK.

%% Update Metadata
-spec nebula2_utils:update_data_system_metadata(list(),map(), cdmi_state()) -> map().
update_data_system_metadata(CList, Data, State) ->
    lager:debug("Entry"),
    CapabilitiesURI = nebula2_utils:get_value(<<"capabilitiesURI">>, Data, []),
    nebula2_utils:update_data_system_metadata(CList, Data, CapabilitiesURI, State).

-spec nebula2_utils:update_data_system_metadata(list(),map(), string(), cdmi_state()) -> map().
update_data_system_metadata(_CList, Data, [], _State) ->
    lager:debug("Entry"),
    Data;
update_data_system_metadata(CList, Data, CapabilitiesURI, State) when is_binary(CapabilitiesURI) ->
    update_data_system_metadata(CList, Data, binary_to_list(CapabilitiesURI), State);
update_data_system_metadata(CList, Data, CapabilitiesURI, State) ->
    lager:debug("Entry"),
    Domain = get_domain_hash(<<"">>),
    {ok, C1} = nebula2_db:search(Domain ++ CapabilitiesURI, State),
    Capabilities = nebula2_utils:get_value(<<"capabilities">>, C1),
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
    Children = case nebula2_utils:get_value(<<"children">>, Parent) of
                     <<"">> ->
                         [list_to_binary(Name)];
                     Ch ->
                         lists:append(Ch, [list_to_binary(Name)])
                 end,
    ChildrenRange = case nebula2_utils:get_value(<<"childrenrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    NewParent1 = nebula2_utils:put_value(<<"children">>, Children, Parent),
    NewParent2 = nebula2_utils:put_value(<<"childrenrange">>, list_to_binary(ChildrenRange), NewParent1),
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
	case check_base64(Body) of
		false ->
		   throw(badencoding);
		true ->
			true
	end,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    ParentUri = nebula2_utils:get_parent_uri(Path),
    ParentId = nebula2_utils:get_value(<<"objectID">>, Parent),
    ParentMetadata = nebula2_utils:get_value(<<"metadata">>, Parent, maps:new()),
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
    Owner = nebula2_utils:get_value(<<"auth_as">>, EnvMap, ""),
    CapabilitiesURI = case nebula2_utils:get_value(<<"capabilitiesURI">>, Body, none) of
                        none ->   get_capability_uri(ObjectType);
                        Curi ->   Curi
                      end,
    NewMetadata = maps:from_list([
                                  {<<"nebula_data_location">>, [Location]},
                                  {<<"cdmi_owner">>, Owner}
                                 ]),
    OldMetadata = nebula2_utils:get_value(<<"metadata">>, Body, maps:new()),
    Metadata2 = maps:merge(OldMetadata, NewMetadata),
    Metadata3 = maps:merge(ParentMetadata, Metadata2),
    CdmiData = maps:get(<<"cdmi">>, Data),
    Data2 = maps:merge(maps:from_list([{<<"objectType">>, list_to_binary(ObjectType)},
                                       {<<"objectID">>, Oid},
                                       {<<"objectName">>, list_to_binary(ObjectName)},
                                       {<<"parentID">>, ParentId},
                                       {<<"parentURI">>, ParentUri},
                                       {<<"capabilitiesURI">>, list_to_binary(CapabilitiesURI)},
                                       {<<"domainURI">>, DomainName},
                                       {<<"completionStatus">>, <<"Complete">>},
                                       {<<"metadata">>, Metadata3}]),
                      CdmiData),
    Data3 = case maps:is_key(<<"value">>, Data2) of
                true ->
                    case maps:is_key(<<"valuetransferencoding">>, Data2) of
                        false ->
                            nebula2_utils:put_value(<<"valuetransferencoding">>, <<"utf-8">>, Data2);
                        true ->
                            Data2
                     end;
                false ->
                    Data2
            end,
    Data4 = nebula2_utils:put_value(<<"metadata">>, Metadata3, Data3),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_ctime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>,
             <<"cdmi_size">>
    ],
    Data5 = nebula2_utils:update_data_system_metadata(CList, Data4, CapabilitiesURI, State),
    SearchKey = nebula2_utils:make_search_key(Data5),
    Data6 = maps:put(<<"cdmi">>, Data5, Data),      %% really needs to be maps:put, not nebula2_utils:put_value
    Data7 = maps:put(<<"sp">>, SearchKey, Data6),   %% Ditto
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data7),
    ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
    pooler:return_member(riak_pool, Pid),
    set_cache(Data7),
    {true, Req, Data6}.

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

-spec get_cache(binary() | list) -> {ok, map()} | {error, deleted | notfound}.
get_cache(Key) when is_list(Key) ->
    get_cache(list_to_binary(Key));
get_cache(Key) when is_binary(Key) ->
    lager:debug("Cache Key: ~p", [Key]),
    case mcd:get(?MEMCACHE, Key) of
        {ok, Data} ->
            lager:debug("Data: ~p", [Data]),
            {ok, Data};
        Response ->
            lager:debug("Response: ~p", [Response]),
            Response
    end.
%get_cache(_) -> 
%    {error, notfound}.

-spec get_domain_hash(binary() | list()) -> string().
get_domain_hash(Domain) when is_list(Domain) ->
    get_domain_hash(list_to_binary(Domain));
get_domain_hash(Domain) when is_binary(Domain) ->
    <<Mac:160/integer>> = crypto:hmac(sha, <<"domain">>, Domain),
    lists:flatten(io_lib:format("~40.16.0b", [Mac])).

%% TODO: Make delete asynchronous
handle_delete(Pid, Data, _, []) ->
    lager:debug("Entry"),
    case nebula2_db:delete(Pid, nebula2_utils:get_value(<<"objectID">>, Data)) of
        ok ->
             ParentId = nebula2_utils:get_value(<<"parentID">>, Data, []),
             ObjectName = nebula2_utils:get_value(<<"objectName">>, Data),
             delete_child_from_parent(Pid, ParentId, ObjectName),
             ok;
        Other ->
            Other
    end;
handle_delete(Pid, Data, State, [Head | Tail]) ->
    lager:debug("Entry"),
    Child = binary_to_list(Head),
    ParentUri = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, Data, "")),
    ChildPath = ParentUri ++ binary_to_list(nebula2_utils:get_value(<<"objectName">>, Data)) ++ Child,
    {ok, ChildData} = nebula2_db:search(ChildPath, State),
    GrandChildren = nebula2_utils:get_value(<<"children">>, ChildData, []),
    handle_delete(Pid, ChildData, State, GrandChildren),
    handle_delete(Pid, Data, State, Tail),
    nebula2_db:delete(Pid, nebula2_utils:get_value(<<"objectID">>, Data)).

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
