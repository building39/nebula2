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
         create_object/3,
         create_object/4,
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
         type_of/1,
         update_data_system_metadata/3,
         update_data_system_metadata/4,
         update_parent/4
        ]).

%% @doc Check if a string begins with a certain substring.
-spec beginswith(string(), string()) -> boolean().
beginswith(Str, Substr) ->
    ?nebMsg("Entry"),
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% Check base64 encoding
-spec check_base64(binary() | list()) -> boolean().
check_base64(Data) ->
    case maps:is_key(<<"valuetransferencoding">>, Data) of
        false ->
            true;
        true ->
            try base64:decode(binary_to_list(get_value(<<"value">>, Data))) of
                _ -> 
                    true
            catch
                _:_ ->
                    false
            end
    end.

%% @doc Create a CDMI object
-spec create_object({pid(), map()}, object_type(), map()) ->
          {boolean(), map()}.
create_object(State, ObjectType, Body) ->
    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(get_value(<<"path">>, EnvMap)),
    case get_cache(Path) of
        {ok, Data} ->
            Data;
        _ ->
            case get_object_oid(get_parent_uri(Path), State) of
                {ok, ParentId} ->
                    {ok, Parent} = nebula2_db:read(Pid, ParentId),
                    DomainName = get_value(<<"domainURI">>, Parent, ""),
                    create_object(State, ObjectType, DomainName, Parent, Body);
                {notfound, _} ->
                    pooler:return_member(riak_pool, Pid),
                    false
            end
    end.

-spec create_object({pid(), map()}, object_type(), list() | binary(), map()) ->
          {boolean(), map()}.
create_object(State, ObjectType, DomainName, Body) when is_list(DomainName)->
    create_object(State, ObjectType, list_to_binary(DomainName), Body);
create_object(State, ObjectType, DomainName, Body) when is_binary(DomainName)->
    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(get_value(<<"path">>, EnvMap)),
    case get_object_oid(get_parent_uri(Path), State) of
        {ok, ParentId} ->
            {ok, Parent} = nebula2_db:read(Pid, ParentId),
            create_object(State, ObjectType, DomainName, Parent, Body);
        {notfound, _} ->
            %% TODO: maybe leaking a pool connection here?
            pooler:return_member(riak_pool, Pid),
            false
    end.

%% @doc Delete an object and all objects underneath it.
-spec delete(cdmi_state()) -> ok | {error, term()}.
delete(State) ->
    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Data = get_value(<<"object_map">>, EnvMap),
    ?nebFmt("Data: ~p", [Data]),
    {Pid, _} = State,
    Children = get_value(<<"children">>, Data, []),
    Path = binary_to_list(get_value(<<"parentURI">>, Data)) ++ binary_to_list(get_value(<<"objectName">>, Data)),
    handle_delete(Pid, Data, State, list_to_binary(Path), Children).

-spec delete_cache(object_oid()) -> {ok | error, deleted | notfound}.
delete_cache(Oid) ->
    ?nebMsg("Entry"),
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            SearchKey = newbula2_utils:make_search_key(Data),
            mcd:delete(?MEMCACHE, SearchKey),
            mcd:delete(?MEMCACHE, Oid);
        _Response ->
            {error, notfound}
    end.

%% @doc Delete a child from its parent
-spec delete_child_from_parent(pid(), object_oid(), string()) -> ok | {error, term()}.
delete_child_from_parent(Pid, ParentId, Name) ->
    ?nebMsg("Entry"),
    {ok, Parent} = nebula2_db:read(Pid, ParentId),
    Children = get_value(<<"children">>, Parent, ""),
    ?nebFmt("Children: ~p", [Children]),
    ?nebFmt("Name: ~p", [Name]),
    NewParent1 = case Children of
                     [] ->
                         maps:remove(<<"children">>, Parent);
                     _  ->
                         NewChildren = lists:delete(Name, Children),
                         put_value(<<"children">>, NewChildren, Parent)
                 end,
    NewParent2 = case get_value(<<"childrenrange">>, Parent, "") of
                    "0-0" ->
                        maps:remove(<<"childrenrange">>, Parent);
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         put_value(<<"childrenrange">>, list_to_binary(lists:concat(["0-", Num - 1])), NewParent1)
                 end,
    ?nebFmt("NewParent2: ~p", [NewParent2]),
    nebula2_db:update(Pid, ParentId, NewParent2).

%% @doc Extract the Parent URI from the path.
-spec extract_parentURI(list()) -> list().
extract_parentURI(Path) ->
    ?nebMsg("Entry"),
    extract_parentURI(Path, "") ++ "/".

%% @doc Generate hash.
-spec generate_hash(string(), string()) -> string().
generate_hash(Method, Data) when is_binary(Data)->
    Data2 = binary_to_list(Data),
    generate_hash(Method, Data2);
generate_hash(Method, Data) ->
    M = list_to_atom(Method),
    string:to_lower(lists:flatten([[integer_to_list(N, 16) || <<N:4>> <= crypto:hash(M, Data)]])).
    
%% @doc Get the object name.
-spec get_object_name(list(), string()) -> string().
get_object_name(Parts, Path) ->
    ?nebMsg("Entry"),
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
-spec get_object_oid(map()) -> {ok, term()}|{notfound, string()}.
get_object_oid(State) ->
    ?nebMsg("Entry"),
    {_, Path} = State,
    get_object_oid(Path, State).

%% @doc Get the object's oid.
-spec get_object_oid(string() | binary(), tuple()) -> {ok, term()}|{notfound, string()}.
get_object_oid(Path, State) when is_binary(Path) ->
    get_object_oid(binary_to_list(Path), State);
get_object_oid(Path, State) ->
    ?nebMsg("Entry"),
    RealPath = case beginswith(Path, "/capabilities/") of
                   true ->
                       get_domain_hash(<<"">>) ++ Path;
                   false ->
                       {_, EnvMap} = State,
                       get_domain_hash(get_value(<<"domainURI">>, EnvMap, <<"">>)) ++ Path
               end,
    Oid = case nebula2_db:search(RealPath, State) of
            {error,_} -> 
                {notfound, ""};
            {ok, Data} ->
                {ok, get_value(<<"objectID">>, Data)}
          end,
    Oid.

%% @doc Construct the object's parent URI.
-spec get_parent_uri(list()) -> string().
get_parent_uri(Path) ->
    ?nebMsg("Entry"),
    Parts = string:tokens(Path, "/"),
    ParentUri = case length(Parts) of
                    0 ->
                        "";     %% Root has no parent
                    1 ->
                        "/";
                    _ ->
                        extract_parentURI(lists:droplast(Parts))
                end,
    ?nebFmt("Exit: ~p", [ParentUri]),
    list_to_binary(ParentUri).

%% @doc Get a value from the data map.
-spec get_value(binary(), map()) -> binary().
get_value(Key, Map) ->
    ?nebMsg("Entry"),
    get_value(Key, Map, <<"">>).

%% @doc Get a value from the data map.
-spec get_value(binary(), map(), term()) -> binary().
get_value(Key, Map, Default) ->
    ?nebMsg("Entry"),
    case maps:is_key(<<"cdmi">>, Map) of
        true ->
            maps:get(Key, maps:get(<<"cdmi">>, Map), Default);
        false ->
            maps:get(Key, Map, Default)
    end.

%% @doc Return current time in ISO 8601:2004 extended representation.
-spec get_time() -> string().
get_time() ->
    ?nebMsg("Entry"),
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:now_to_universal_time(erlang:now()),
    binary_to_list(iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.000000Z",
                  [Year, Month, Day, Hour, Minute, Second]))).

%% Get the content type for the request
-spec handle_content_type({pid(),map()}) -> string().
handle_content_type(State) ->
    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    case binary_to_list(get_value(<<"content-type">>, EnvMap, <<"">>)) of
        "" ->
            Path = binary_to_list(get_value(<<"path">>, EnvMap)),
            {ok, Data} = nebula2_db:search(Path, State),
            binary_to_list(get_value(<<"objectType">>, Data));
        CT ->
            binary_to_list(CT)
    end.

%% @doc Make a primary key for storing a new object.
-spec make_key() -> object_oid().
make_key() ->
    ?nebMsg("Entry"),
    Uid = re:replace(uuid:to_string(uuid:uuid4()), "-", "", [global, {return, list}]),
    Temp = Uid ++ ?OID_SUFFIX ++ "0000",
    Crc = integer_to_list(crc16:crc16(Temp), 16),
    list_to_binary(Uid ++ ?OID_SUFFIX ++ Crc).

-spec make_search_key(map()) -> list().
make_search_key(Data) ->
    ?nebMsg("Entry"),
    ?nebFmt("Data: ~p", [Data]),
    ObjectName = binary_to_list(get_value(<<"objectName">>, Data)),
    ParentUri = binary_to_list(get_value(<<"parentURI">>, Data, <<"">>)),
    Path = binary_to_list(get_value(<<"path">>, Data, <<"">>)),
    DomainUri = case beginswith(Path, "/cdmi_capabilities/") of
                    false ->
                        case beginswith(Path, "/cdmi_domains/") of
                            false ->
                                get_value(<<"domainURI">>, Data, <<"">>);
                            true ->
                                case Path == "/cdmi_domains/" of
                                    true ->     %% always belongs to /cdmi/system_domain
                                        <<"/cdmi_domains/system_domain/">>; 
                                    false ->
                                        case beginswith(Path, "/cdmi_domains/") of
                                            true ->
                                                get_domain_from_path(Path);
                                            false -> %% a domain always belongs to itself.
                                                list_to_binary(Path)
                                        end
                                end
                        end;
                    true ->
                        ?nebMsg("Making key for capabilities"),
                        <<"">>
                end,
    DomainHash = get_domain_hash(DomainUri),
    Key = DomainHash ++ ParentUri ++ ObjectName,
    list_to_binary(Key).

%% @doc Put a value to the data map.
-spec put_value(binary(), term(), map()) -> map().
put_value(Key, Value, Map) ->
    ?nebMsg("Entry"),
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
    ?nebMsg("Entry"),
    SearchKey = make_search_key(Data),
    ObjectId = get_value(<<"objectID">>, Data),
    mcd:set(?MEMCACHE, ObjectId, Data, ?MEMCACHE_EXPIRY),
    mcd:set(?MEMCACHE, SearchKey, Data, ?MEMCACHE_EXPIRY).

-spec type_of(term()) -> boolean().
type_of(X) ->
    if
        is_atom(X) ->
            <<"atom">>;
        is_binary(X) ->
            <<"binary">>;
        is_bitstring(X) ->
            <<"bitstring">>;
        is_boolean(X) ->
            <<"boolean">>;
        is_float(X) ->
            <<"float">>;
        is_function(X) ->
            <<"function">>;
        is_integer(X) ->
            <<"integer">>;
        is_list(X) ->
            <<"list">>;
        is_number(X) ->
            <<"number">>;
        is_pid(X) ->
            <<"pid">>;
        is_port(X) ->
            <<"port">>;
        is_reference(X) ->
            <<"reference">>;
        is_tuple(X) ->
            <<"tuple">>;
        true ->
            <<"I have no idea what it is">>
    end.

%% Update Metadata
-spec update_data_system_metadata(list(),map(), cdmi_state()) -> map().
update_data_system_metadata(CList, Data, State) ->
    ?nebMsg("Entry"),
    CapabilitiesURI = get_value(<<"capabilitiesURI">>, Data, []),
    update_data_system_metadata(CList, Data, CapabilitiesURI, State).

-spec update_data_system_metadata(list(),map(), string(), cdmi_state()) -> map().
update_data_system_metadata(_CList, Data, [], _State) ->
    ?nebMsg("Entry"),
    Data;
update_data_system_metadata(CList, Data, CapabilitiesURI, State) when is_binary(CapabilitiesURI) ->
    update_data_system_metadata(CList, Data, binary_to_list(CapabilitiesURI), State);
update_data_system_metadata(CList, Data, CapabilitiesURI, State) ->
    ?nebMsg("Entry"),
    Domain = get_domain_hash(<<"">>),
    {ok, C1} = nebula2_db:search(Domain ++ CapabilitiesURI, State),
    Capabilities = get_value(<<"capabilities">>, C1),
    CList2 = maps:to_list(maps:with(CList, Capabilities)),
    nebula2_capabilities:apply_metadata_capabilities(CList2, Data).

%% @doc Update a parent object with a new child
-spec update_parent(object_oid(), string(), string(), pid()) -> ok | {error, term()}.
update_parent(Root, _, _, _) when Root == ""; Root == <<"">> ->
    ?nebMsg("Entry"),
    %% Must be the root, since there is no parent.
    ok;
update_parent(ParentId, Path, ObjectType, Pid) ->
    ?nebMsg("Entry"),
    ?nebFmt("Path: ~p", [Path]),
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
    Children = case get_value(<<"children">>, Parent) of
                     <<"">> ->
                         [list_to_binary(Name)];
                     Ch ->
                         lists:append(Ch, [list_to_binary(Name)])
                 end,
    ChildrenRange = case get_value(<<"childrenrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    NewParent1 = put_value(<<"children">>, Children, Parent),
    NewParent2 = put_value(<<"childrenrange">>, list_to_binary(ChildrenRange), NewParent1),
    nebula2_db:update(Pid, ParentId, NewParent2).

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec create_object({pid(), map()}, object_type(), list() | binary(), string(), binary()) ->
          {boolean(), cowboy_req:req(), {pid(), map()}}.
create_object(State, ObjectType, DomainName, Parent, Body) when is_list(DomainName)->
    D = list_to_binary(DomainName),
    create_object(State, ObjectType, D, Parent, Body);
create_object(State, ObjectType, DomainName, Parent, Body) when is_binary(DomainName)->
    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
	case check_base64(Body) of
		false ->
		   throw(badencoding);
		true ->
			true
	end,
    Path = binary_to_list(get_value(<<"path">>, EnvMap)),
    ParentUri = get_parent_uri(Path),
    ParentId = get_value(<<"objectID">>, Parent),
    ParentMetadata = get_value(<<"metadata">>, Parent, maps:new()),
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
    Oid = make_key(),
    Location = list_to_binary(application:get_env(nebula2, cdmi_location, ?DEFAULT_LOCATION)),
    Owner = get_value(<<"auth_as">>, EnvMap, ""),
    CapabilitiesURI = case get_value(<<"capabilitiesURI">>, Body, none) of
                        none ->   get_capability_uri(ObjectType);
                        Curi ->   Curi
                      end,
    NewMetadata = maps:from_list([
                                  {<<"nebula_data_location">>, [Location]},
                                  {<<"cdmi_owner">>, Owner}
                                 ]),
    OldMetadata = get_value(<<"metadata">>, Body, maps:new()),
    Metadata2 = maps:merge(OldMetadata, NewMetadata),
    Metadata3 = maps:merge(ParentMetadata, Metadata2),
    Data2 = maps:merge(maps:from_list([{<<"objectType">>, list_to_binary(ObjectType)},
                                       {<<"objectID">>, Oid},
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
                            put_value(<<"valuetransferencoding">>, <<"utf-8">>, Data2);
                        true ->
                            Data2
                     end;
                false ->
                    Data2
            end,
    Data4 = put_value(<<"metadata">>, Metadata3, Data3),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_ctime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>,
             <<"cdmi_size">>
    ],
    Data5 = update_data_system_metadata(CList, Data4, CapabilitiesURI, State),
    SearchKey = make_search_key(Data5),
    Data6 = maps:put(<<"cdmi">>, Data5, Data),      %% really needs to be maps:put, not nebula2_utils:put_value
    Data7 = maps:put(<<"sp">>, SearchKey, Data6),   %% Ditto
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data7),
    ok = update_parent(ParentId, ObjectName, ObjectType, Pid),
    pooler:return_member(riak_pool, Pid),
    set_cache(Data7),
    {true, Data6}.

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
    ?nebMsg("Entry"),
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
    ?nebFmt("Cache Key: ~p", [Key]),
    case mcd:get(?MEMCACHE, Key) of
        {ok, Data} ->
            {ok, Data};
        Response ->
            Response
    end.
%get_cache(_) -> 
%    {error, notfound}.

get_domain_from_path(Path) ->
    ?nebMsg("Entry"),
    Parts = string:tokens(Path, "/"),
    DomainParts = lists:takewhile(fun(X) -> true /= beginswith(X, "cdmi_domain_") end, Parts),
    "/" ++ string:join(DomainParts, "/") ++ "/".
  
-spec get_domain_hash(binary() | list()) -> string().
get_domain_hash(Domain) when is_list(Domain) ->
    get_domain_hash(list_to_binary(Domain));
get_domain_hash(Domain) when is_binary(Domain) ->
    <<Mac:160/integer>> = crypto:hmac(sha, <<"domain">>, Domain),
    lists:flatten(io_lib:format("~40.16.0b", [Mac])).

%% TODO: Make delete asynchronous
handle_delete(Pid, Data, _, _, []) ->
    ?nebMsg("Entry"),
    Oid = get_value(<<"objectID">>, Data),
    case nebula2_db:delete(Pid, Oid) of
        ok ->
             ParentId = get_value(<<"parentID">>, Data, []),
             ObjectName = get_value(<<"objectName">>, Data),
             delete_child_from_parent(Pid, ParentId, ObjectName),
             ok;
        Other ->
            ?nebFmt("Delete failed for object: ~p. Reason: ~p", [Oid, Other]),
            Other
    end;
handle_delete(Pid, Data, State, Path, [Child | Tail]) ->
    ?nebMsg("Entry"),
    ChildPath = binary_to_list(Path) ++ binary_to_list(Child),
    KeyMap = maps:from_list([{<<"objectName">>, Child},
                             {<<"path">>, list_to_binary(ChildPath)},
                             {<<"parentURI">>, Path}]),
    NewPath = make_search_key(KeyMap),
    {ok, ChildData} = nebula2_db:search(NewPath, State),
    GrandChildren = get_value(<<"children">>, ChildData, []),
    handle_delete(Pid, ChildData, State, list_to_binary(ChildPath), GrandChildren),
    handle_delete(Pid, Data, State, Path, Tail).

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
%% get_parent_root_test() -> 
%%     ?assert(get_parent(ok, "/") == {ok, "", ""}).
%% get_parent_object_test() ->
%%     Data =  "{\"objectID\":\"parent_oid\",}",
%%     meck:new(nebula2_riak, [non_strict]),
%%     meck:expect(nebula2_riak, search, fun(ok, "/some/object") -> {ok, Data} end),
%%     ?assert(get_parent(ok, "/some/object") == {ok, "/some/", "parent_oid"}),
%%     meck:unload(nebula2_riak).
%% get_parent_notfound_test() ->
%%     meck:new(nebula2_riak, [non_strict]),
%%     meck:expect(nebula2_riak, search, fun(ok, "/some/object") -> {error, notfound} end),
%%     ?assert(get_parent(ok, "/some/object") == {error, notfound, "/some/"}),
%%     meck:unload(nebula2_riak).
-endif.
