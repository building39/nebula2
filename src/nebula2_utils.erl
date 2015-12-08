%% @author mmartin
%% @doc Various useful functions.

-module(nebula2_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
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
         get_object_name/1,
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
%    ?nebMsg("Entry"),
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% Check base64 encoding
-spec check_base64(map()) -> boolean().
check_base64(Data) when is_map(Data) ->
    case maps:get(<<"valuetransferencoding">>, Data, <<"">>) of
        <<"base64">> ->
            try base64:decode(binary_to_list(get_value(<<"value">>, Data))) of
                _ -> 
                    true
            catch
                _:_ ->
                    false
            end;
        _ ->
            true
    end.

%% @doc Create a CDMI object
-spec create_object(cdmi_state(), object_type(), map()) ->
          {boolean(), map()} | false.
create_object(State, ObjectType, Body) when is_binary(ObjectType), is_map(Body) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Path = get_value(<<"path">>, EnvMap),
    case get_object_oid(get_parent_uri(Path), State) of
        {ok, ParentId} ->
            {ok, Parent} = nebula2_db:read(Pid, ParentId),
            DomainName = get_value(<<"domainURI">>, Parent, ""),
            create_object(State, ObjectType, DomainName, Parent, Body);
        {notfound, _} ->
            pooler:return_member(riak_pool, Pid),
            false
    end.

-spec create_object(cdmi_state(), object_type(), binary(), map()) ->
          {boolean(), map()} | false.
create_object(State, ObjectType, DomainName, Body) when is_binary(ObjectType), is_binary(DomainName), is_map(Body) ->
%    ?nebMsg("Entry"),
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
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Data = get_value(<<"object_map">>, EnvMap),
    {Pid, _} = State,
    Children = get_value(<<"children">>, Data, []),
    Path = binary_to_list(get_value(<<"parentURI">>, Data)) ++ binary_to_list(get_value(<<"objectName">>, Data)),
    handle_delete(Data, State, list_to_binary(Path), Children).

-spec delete_cache(object_oid()) -> {ok | error, deleted | notfound}.
delete_cache(Oid) when is_binary(Oid) ->
%    ?nebMsg("Entry"),
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            SearchKey = nebula2_utils:make_search_key(Data),
            mcd:delete(?MEMCACHE, SearchKey),
            mcd:delete(?MEMCACHE, Oid);
        _Response ->
            {error, notfound}
    end.

%% @doc Delete a child from its parent
-spec delete_child_from_parent(pid(), object_oid(), string()) -> {ok, map()}  | {error, term()}.
delete_child_from_parent(Pid, ParentId, Name) when is_pid(Pid), is_binary(ParentId), is_binary(Name) ->
%    ?nebMsg("Entry"),
    {ok, Parent} = nebula2_db:read(Pid, ParentId),
    Children = get_value(<<"children">>, Parent, ""),
    NewParent1 = case Children of
                     [] ->
                         maps:remove(<<"children">>, Parent);
                     _Children  ->
                         NewChildren = lists:delete(Name, Children),
                         if
                             length(NewChildren) == 0 ->
                                 maps:remove(<<"children">>, Parent);
                             true ->
                                 put_value(<<"children">>, NewChildren, Parent)
                         end
                 end,
    NewParent2 = case get_value(<<"childrenrange">>, NewParent1, "") of
                     "" ->
                         NewParent1;
                    <<"0-0">> ->
                        maps:remove(<<"childrenrange">>, NewParent1);
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         put_value(<<"childrenrange">>, list_to_binary(lists:concat(["0-", Num - 1])), NewParent1)
                 end,
    nebula2_db:update(Pid, ParentId, NewParent2).

%% @doc Extract the Parent URI from the path.
-spec extract_parentURI(list()) -> list().
extract_parentURI(Path) ->
%    ?nebMsg("Entry"),
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
-spec get_object_name(string()) -> string().
get_object_name(Path) when is_list(Path) ->
%    ?nebMsg("Entry"),
    Parts = string:tokens(Path, "/"),
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
-spec get_object_oid(string() | binary(), cdmi_state()) -> {ok, term()}|{notfound, string()}.
get_object_oid(Path, State) when is_binary(Path), is_tuple(State) ->
    get_object_oid(binary_to_list(Path), State);
get_object_oid(Path, State) when is_list(Path), is_tuple(State) ->
%   ?nebMsg("Entry"),
    RealPath = case beginswith(Path, "/cdmi_capabilities/") of
                   true ->
                       get_domain_hash(<<"">>) ++ Path;
                   false ->
                       {_, EnvMap} = State,
                       get_domain_hash(get_value(<<"domainURI">>, EnvMap, <<"">>)) ++ Path
               end,
    case nebula2_db:search(RealPath, State) of
        {error,_} ->
            {notfound, ""};
        {ok, Data} ->
            {ok, get_value(<<"objectID">>, Data)}
    end.

%% @doc Construct the object's parent URI.
-spec get_parent_uri(list() | binary()) -> binary().
get_parent_uri(Path) when is_list(Path) ->
    get_parent_uri(list_to_binary(Path));
get_parent_uri(Path) when is_binary(Path) ->
%    ?nebMsg("Entry"),
    PathString = binary_to_list(Path),
    Parts = string:tokens(PathString, "/"),
    ParentUri = case length(Parts) of
                    0 ->
                        "";     %% Root has no parent
                    1 ->
                        "/";
                    _ ->
                        extract_parentURI(lists:droplast(Parts))
                end,
    list_to_binary(ParentUri).

%% @doc Get a value from the data map.
-spec get_value(binary(), map()) -> binary().
get_value(Key, Map) when is_binary(Key), is_map(Map) ->
%    ?nebMsg("Entry"),
    get_value(Key, Map, <<"">>).

%% @doc Get a value from the data map.
-spec get_value(binary(), map(), term()) -> binary().
get_value(Key, Map, Default) when is_binary(Key), is_map(Map) ->
%    ?nebMsg("Entry"),
    case maps:is_key(<<"cdmi">>, Map) of
        true ->
            maps:get(Key, maps:get(<<"cdmi">>, Map), Default);
        false ->
            maps:get(Key, Map, Default)
    end.

%% @doc Return current time in ISO 8601:2004 extended representation.
-spec get_time() -> string().
-ifdef(TEST).
get_time() ->
    "1970-01-01T00:00:00.000000Z".
-else.
get_time() ->
%    ?nebMsg("Entry"),
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:now_to_universal_time(erlang:now()),
    binary_to_list(iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.000000Z",
                  [Year, Month, Day, Hour, Minute, Second]))).
-endif.

%% Get the content type for the request
-spec handle_content_type(cdmi_state()) -> string().
handle_content_type(State) when is_tuple(State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    case get_value(<<"content-type">>, EnvMap, <<"">>) of
        <<"">> ->
            Path = get_value(<<"path">>, EnvMap),
            {ok, Data} = nebula2_db:search(Path, State),
            get_value(<<"objectType">>, Data);
        CT ->
            CT
    end.

%% @doc Make a primary key for storing a new object.
-spec make_key() -> object_oid().
make_key() ->
%    ?nebMsg("Entry"),
    Uid = re:replace(uuid:to_string(uuid:uuid4()), "-", "", [global, {return, list}]),
    Temp = Uid ++ ?OID_SUFFIX ++ "0000",
    Crc = integer_to_list(crc16:crc16(Temp), 16),
    list_to_binary(string:to_lower(Uid ++ ?OID_SUFFIX ++ Crc)).

-spec make_search_key(cdmi_state()) -> list().
make_search_key(Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
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
                                        get_domain_from_path(Path)
                                end
                        end;
                    true ->
                        <<"">>
                end,
    DomainHash = get_domain_hash(DomainUri),
    DomainHash ++ ParentUri ++ ObjectName.

%% @doc Put a value to the data map.
-spec put_value(binary(), term(), map()) -> map().
put_value(Key, Value, Map) when is_binary(Key), is_map(Map) ->
%    ?nebMsg("Entry"),
    case maps:is_key(<<"cdmi">>, Map) of
        true ->
            Data = maps:get(<<"cdmi">>, Map),
            Data2 = maps:put(Key, Value, Data),
            maps:put(<<"cdmi">>, Data2, Map);
        false ->
            maps:put(Key, Value, Map)
    end.
-spec set_cache(map()) -> {ok, map()}.
set_cache(Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
    SearchKey = make_search_key(Data),
    ObjectId = get_value(<<"objectID">>, Data),
    mcd:set(?MEMCACHE, ObjectId, Data, ?MEMCACHE_EXPIRY),
    mcd:set(?MEMCACHE, SearchKey, Data, ?MEMCACHE_EXPIRY).

-spec type_of(term()) -> boolean().
type_of(X) ->
    if
        is_boolean(X) ->        %% This test must come before is_atom/1
            <<"boolean">>;
        is_atom(X) ->
            <<"atom">>;
        is_binary(X) ->
            <<"binary">>;
        is_bitstring(X) ->
            <<"bitstring">>;
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
-spec update_data_system_metadata(list(), map(), cdmi_state()) -> map().
update_data_system_metadata(CList, Data, State) when is_list(CList), is_map(Data), is_tuple(State) ->
%    ?nebMsg("Entry"),
    CapabilitiesURI = get_value(<<"capabilitiesURI">>, Data, []),
    update_data_system_metadata(CList, Data, CapabilitiesURI, State).

-spec update_data_system_metadata(list(), map(), binary() | string(), cdmi_state()) -> map().
update_data_system_metadata(_CList, Data, CapabilitiesURI, _State) when is_list(_CList), 
                                                                        is_map(Data),
                                                                        CapabilitiesURI=="",
                                                                        is_tuple(_State) ->
%    ?nebMsg("Entry"),
    Data;
update_data_system_metadata(CList, Data, CapabilitiesURI, State) when is_list(CList), 
                                                                        is_map(Data),
                                                                        is_binary(CapabilitiesURI),
                                                                        is_tuple(State) ->
    update_data_system_metadata(CList, Data, binary_to_list(CapabilitiesURI), State);
update_data_system_metadata(CList, Data, CapabilitiesURI, State) when is_list(CList), 
                                                                        is_map(Data),
                                                                        is_list(CapabilitiesURI),
                                                                        is_tuple(State)->
%    ?nebMsg("Entry"),
    Domain = get_domain_hash(<<"">>),
    {ok, C1} = nebula2_db:search(Domain ++ CapabilitiesURI, State),
    Capabilities = get_value(<<"capabilities">>, C1),
    CList2 = maps:to_list(maps:with(CList, Capabilities)),
    nebula2_capabilities:apply_metadata_capabilities(CList2, Data).

%% @doc Update a parent object with a new child
-spec update_parent(object_oid(), string(), object_type(), pid()) -> {ok, map()} | {error, term()}.
update_parent(ParentOid, _, _, _) when ParentOid == <<"">> ->
%    ?nebMsg("Entry"),
    %% Must be the root, since there is no parent.
    {ok, <<"">>};
update_parent(ParentOid, Path, ObjectType, Pid) when is_binary(ParentOid), is_list(Path), is_binary(ObjectType), is_pid(Pid) ->
%    ?nebMsg("Entry"),
    N = lists:last(string:tokens(Path, "/")),
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
    {ok, Parent} = nebula2_db:read(Pid, ParentOid),
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
    nebula2_db:update(Pid, ParentOid, NewParent2).

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec create_object({pid(), map()}, object_type(), list() | binary(), string(), binary()) ->
          {boolean(), map()}.
create_object(State, ObjectType, DomainName, Parent, Body) when is_list(DomainName)->
    D = list_to_binary(DomainName),
    create_object(State, ObjectType, D, Parent, Body);
create_object(State, ObjectType, DomainName, Parent, Body) when is_binary(DomainName)->
%    ?nebMsg("Entry"),
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
    Data2 = maps:merge(maps:from_list([{<<"objectType">>, ObjectType},
                                       {<<"objectID">>, Oid},
                                       {<<"objectName">>, list_to_binary(ObjectName)},
                                       {<<"parentID">>, ParentId},
                                       {<<"parentURI">>, ParentUri},
                                       {<<"capabilitiesURI">>, CapabilitiesURI},
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
    Data6 = maps:from_list([{<<"cdmi">>, Data5}, {<<"sp">>, list_to_binary(SearchKey)}]),
    {ok, Oid} = nebula2_db:create(Pid, Oid, Data6),
    {ok, _} = update_parent(ParentId, ObjectName, ObjectType, Pid),
    pooler:return_member(riak_pool, Pid),
    set_cache(Data6),
    {true, Data6}.

-spec extract_parentURI(list(), list()) -> list().
extract_parentURI([], Acc) ->
    Acc;
%% extract_parentURI([H|T], Acc) when is_binary(H) ->
%%     Acc2 = Acc ++ "/" ++ binary_to_list(H),
%%     extract_parentURI(T, Acc2);
extract_parentURI([H|T], Acc) ->
    Acc2 = Acc ++ "/" ++ H,
    extract_parentURI(T, Acc2).

-spec get_capability_uri(binary()) -> binary().
get_capability_uri(ObjectType) when is_binary(ObjectType) ->
%    ?nebMsg("Entry"),
    case ObjectType of
        ?CONTENT_TYPE_CDMI_CAPABILITY ->
            ?DOMAIN_SUMMARY_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_CONTAINER ->
            ?CONTAINER_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_DATAOBJECT ->
            ?DATAOBJECT_CAPABILITY_URI;
        ?CONTENT_TYPE_CDMI_DOMAIN ->
            ?DOMAIN_CAPABILITY_URI;
        _Other ->
            <<"unknown">>
    end.

-spec get_cache(binary() | list()) -> {ok, map()} | {error, deleted | notfound}.
get_cache(Key) when is_list(Key) ->
    get_cache(list_to_binary(Key));
get_cache(Key) when is_binary(Key) ->
    case mcd:get(?MEMCACHE, Key) of
        {ok, Data} ->
            {ok, Data};
        Response ->
            Response
    end.

-spec get_domain_from_path(list()) -> list().
get_domain_from_path(Path) when is_list(Path) ->
%    ?nebMsg("Entry"),
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
-spec handle_delete(map(), cdmi_state(), list(), list()) -> ok | {error, term()}.
handle_delete(Data, State, _, []) ->
%    ?nebMsg("Entry"),
    {Pid, _} = State,
    Oid = get_value(<<"objectID">>, Data),
    case nebula2_db:delete(Pid, Oid) of
        ok ->
            ParentId = get_value(<<"parentID">>, Data, []),
            ObjectName = get_value(<<"objectName">>, Data),
            delete_child_from_parent(Pid, ParentId, ObjectName),
            ok;
        Other ->
%            ?nebFmt("Delete failed for object: ~p. Reason: ~p", [Oid, Other]),
            Other
    end;
handle_delete(Data, State, Path, [Child | Tail]) ->
%    ?nebMsg("Entry"),
    ChildPath = binary_to_list(Path) ++ binary_to_list(Child),
    KeyMap = maps:from_list([{<<"objectName">>, Child},
                             {<<"path">>, list_to_binary(ChildPath)},
                             {<<"parentURI">>, Path}]),
    NewPath = make_search_key(KeyMap),
    {ok, ChildData} = nebula2_db:search(NewPath, State),
    GrandChildren = get_value(<<"children">>, ChildData, []),
    handle_delete(ChildData, State, list_to_binary(ChildPath), GrandChildren),
    handle_delete(Data, State, Path, Tail).

sanitize_body([], Body) when is_map(Body) ->
    Body;
sanitize_body([H|T], Body) when is_map(Body)->
    sanitize_body(T, maps:remove(H, Body)).

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
%% @doc Test the beginswith/2 function.

nebula2_utils_test_() ->
    {foreach,
        fun() ->
            meck:new(nebula2_db, [non_strict]),
            meck:new(pooler, [non_strict]),
            meck:new(mcd, [non_strict]),
            meck:new(uuid)
        end,
        fun(_) ->
            meck:unload(nebula2_db),
            meck:unload(pooler),
            meck:unload(mcd),
            meck:unload(uuid)
        end,
        [
            {"Test beginswith/2",
                fun() ->
                    ?assert(beginswith("abcdef", "abc")),
                    ?assertNot(beginswith("abcdef", "def"))
                end
            },
            {"Test check_base64/1",
                fun() ->
                    EncodedData = "dGhpcyBpcyBzb21lIGRhdGEK",
                    Map = maps:from_list(([{<<"value">>, list_to_binary(EncodedData)},
                                           {<<"valuetransferencoding">>, <<"base64">>}])),
                    ?assert(check_base64(Map)),
                    Map2 = maps:from_list(([{<<"value">>, <<"unencoded data">>},
                                            {<<"valuetransferencoding">>, <<"base64">>}])),
                    ?assertNot(check_base64(Map2)),
                    ?assert(check_base64(maps:new())),
                    ?assertException(error, function_clause, check_base64(not_a_map))
                end
            },
            {"Test create_object/3",
                fun () ->
                    Body = jsx:decode(<<"{\"metadata\": {\"my_metadata\": \"junk\"}}">>, [return_maps]),
                    EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"domainURI">>, <<"/new_container/">>}]),
                    Pid = self(),
                    State = {Pid, EnvMap},
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestNewObjectCDMI = maps:from_list([{<<"cdmi">>, TestNewObject},
                                                        {<<"sp">>, ?TestCreateContainerSearchPath}
                                                       ]),
                    TestRootMap = jsx:decode(?TestRootObject, [return_maps]),
                    TestSystemCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Capabilities = maps:get(<<"capabilities">>, TestSystemCapabilities),
                    Capabilities2 = maps:remove(<<"cdmi_acount">>, Capabilities),
                    Capabilities3 = maps:remove(<<"cdmi_mcount">>, Capabilities2),
                    TestSystemCapabilities2 = maps:from_list([{<<"capabilities">>, Capabilities3}]),
                    meck:expect(uuid, uuid4, [], ?TestUuid4),
                    meck:expect(uuid, to_string, [?TestUuid4], ?TestUidString),
                    meck:expect(mcd, set, 4, ['_']),
                    meck:expect(pooler, return_member, ['_', '_'], '_'),
                    meck:sequence(nebula2_db, create, 3, [{ok, ?TestUid}]),
                    meck:sequence(nebula2_db, read,   2, [{ok, TestRootMap}
                                                         ]),
                    meck:sequence(nebula2_db, search, 2, [{ok, TestRootMap},
                                                          {ok, TestSystemCapabilities2},
                                                          {error, '_'}
                                                         ]),
                    meck:sequence(nebula2_db, update, 3, [{ok, TestRootMap}]),
                    {true, NewObject} = create_object(State, ?CONTENT_TYPE_CDMI_CONTAINER, Body),
                    ?assertMatch(TestNewObjectCDMI, NewObject),
                    ?assertMatch(false, create_object(State, ?CONTENT_TYPE_CDMI_CONTAINER, Body)),
                    ?assertException(error, function_clause, create_object(State, not_a_binary, Body)),
                    ?assertException(error, function_clause, create_object(State, ?CONTENT_TYPE_CDMI_CONTAINER, not_a_map)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(mcd)),
                    ?assert(meck:validate(pooler)),
                    ?assert(meck:validate(uuid))
                end
            },
            {"Test create_object/4",
                fun () ->
                    Body = jsx:decode(<<"{\"metadata\": {\"my_metadata\": \"junk\"}}">>, [return_maps]),
                    EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                            {<<"auth_as">>, <<"MickeyMouse">>},
                                            {<<"domainURI">>, <<"/new_container/">>}]),
                    Pid = self(),
                    State = {Pid, EnvMap},
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestNewObjectCDMI = maps:from_list([{<<"cdmi">>, TestNewObject},
                                                        {<<"sp">>, ?TestCreateContainerSearchPath}
                                                        ]),
                    TestRootMap = jsx:decode(?TestRootObject, [return_maps]),
                    TestSystemCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Capabilities = maps:get(<<"capabilities">>, TestSystemCapabilities),
                    Capabilities2 = maps:remove(<<"cdmi_acount">>, Capabilities),
                    Capabilities3 = maps:remove(<<"cdmi_mcount">>, Capabilities2),
                    TestSystemCapabilities2 = maps:from_list([{<<"capabilities">>, Capabilities3}]),
                    meck:expect(uuid, uuid4, [], ?TestUuid4),
                    meck:expect(uuid, to_string, [?TestUuid4], ?TestUidString),
                    meck:expect(mcd, set, 4, ['_']),
                    meck:expect(pooler, return_member, ['_', '_'], '_'),
                    meck:sequence(nebula2_db, create, 3, [{ok, ?TestUid}]),
                    meck:sequence(nebula2_db, read,   2, [{ok, TestRootMap}
                                                         ]),
                    meck:sequence(nebula2_db, search, 2, [{ok, TestRootMap},
                                                          {ok, TestSystemCapabilities2},
                                                          {error, '_'}
                                                         ]),
                    meck:sequence(nebula2_db, update, 3, [{ok, TestRootMap}]),
                    {true, NewObject} = create_object(State,
                                                      ?CONTENT_TYPE_CDMI_CONTAINER,
                                                      <<"/cdmi_domains/system_domain/">>,
                                                      Body),
                    ?assertMatch(TestNewObjectCDMI, NewObject),
                    ?assertMatch(TestNewObjectCDMI, NewObject),
                    ?assertMatch(false, create_object(State,
                                                      ?CONTENT_TYPE_CDMI_CONTAINER,
                                                      <<"/cdmi_domains/system_domain/">>,
                                                      Body)),
                    ?assertException(error, function_clause, create_object(State,
                                                                           not_a_binary,
                                                                           Body)),
                    ?assertException(error, function_clause, create_object(State,
                                                                           ?CONTENT_TYPE_CDMI_CONTAINER,
                                                                           not_a_map)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(mcd)),
                    ?assert(meck:validate(pooler)),
                    ?assert(meck:validate(uuid))
                end
            },
            {"Test delete/1",
                fun () ->
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestRootMap = jsx:decode(?TestRootObject, [return_maps]),
                    EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"domainURI">>, <<"/new_container/">>},
                                             {<<"object_map">>, TestNewObject}
                                            ]),
                    Pid = self(),
                    State = {Pid, EnvMap},
                    Map = maps:new(),
                    meck:sequence(nebula2_db, delete, 2, [ok, {error, notfound}]),
                    meck:sequence(nebula2_db, read,   2, [{ok, TestRootMap}]),
                    meck:sequence(nebula2_db, update, 3, [{ok, Map}]),
                    ?assertMatch(ok, delete(State)),
                    ?assert(meck:validate(nebula2_db))
                end
            },
            {"Test delete_cache/1",
                fun () ->
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    meck:sequence(mcd, delete, 2, [{ok, deleted}]),
                    meck:sequence(mcd, get,    2, [{ok, TestNewObject},
                                                   {error, notfound}
                                                  ]),
                    ?assertMatch({ok, deleted}, delete_cache(?TestOid)),
                    ?assertMatch({error, notfound}, delete_cache(?TestOid)),
                    ?assertException(error, function_clause, delete_cache(not_a_binary)),
                    meck:validate(mcd)
                end
            },
            {"Test delete_child_from_parent/3",
                fun () ->
                    Pid = self(),
                    Child = <<"a_child">>,
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    ParentId = maps:get(<<"objectID">>, TestNewObject),
                    WithChild = maps:put(<<"children">>, [Child], maps:put(<<"childrenrange">>, <<"0-0">>, TestNewObject)),
                    meck:sequence(nebula2_db, read, 2, [{ok, WithChild},
                                                        {ok, TestNewObject}
                                                        ]),
                    meck:expect(nebula2_db, update, [Pid, ParentId, TestNewObject], {ok, TestNewObject}),
                    ?assertMatch({ok, TestNewObject}, delete_child_from_parent(Pid, ParentId, Child)),
                    ?assertMatch({ok, TestNewObject}, delete_child_from_parent(Pid, ParentId, Child)),
                    meck:validate(nebula2_db)
                end
            },
            {"Test extract_parentURI/1",
                fun () ->
                    ?assertMatch("/parent/child/", extract_parentURI(["parent", "child"])),
                    ?assertException(error, function_clause, extract_parentURI(not_a_list))
                end
            },
            {"Test generate_hash/2",
                fun () ->
                    Data = "this is some data",
                    BinaryData = list_to_binary(Data),
                    Algo = "sha256",
                    Hash = "dff90087e2a95f1c093cf40e7be6ef4e998e21b4ea38d0b494ea2fdb2576fcfe",
                    ?assertMatch(Hash, generate_hash(Algo, Data)),
                    ?assertMatch(Hash, generate_hash(Algo, BinaryData))
                end
            },
            {"Test get_cache/1",
                fun () ->
                    KeyList = "a key",
                    KeyBinary = <<"a binary key">>,
                    Data = "test data",
                    meck:sequence(mcd, get, 2, [{ok, Data},
                                                {ok, Data},
                                                {error, notfound}]),
                    ?assertMatch({ok, Data}, get_cache(KeyList)),
                    ?assertMatch({ok, Data}, get_cache(KeyBinary)),
                    ?assertMatch({error, notfound}, get_cache(KeyList)),
                    meck:validate(mcd)
                end
            },
            {"Test get_capability_uri/1",
                fun () ->
                    ?assertMatch(?DOMAIN_SUMMARY_CAPABILITY_URI, get_capability_uri(?CONTENT_TYPE_CDMI_CAPABILITY)),
                    ?assertMatch(?CONTAINER_CAPABILITY_URI, get_capability_uri(?CONTENT_TYPE_CDMI_CONTAINER)),
                    ?assertMatch(?DATAOBJECT_CAPABILITY_URI, get_capability_uri(?CONTENT_TYPE_CDMI_DATAOBJECT)),
                    ?assertMatch(?DOMAIN_CAPABILITY_URI, get_capability_uri(?CONTENT_TYPE_CDMI_DOMAIN)),
                    ?assertMatch(<<"unknown">>, get_capability_uri(<<"bogus capability">>)),
                    ?assertException(error, function_clause, get_capability_uri(not_a_binary))
                end
            },
            {"Test get_domain_from_path/1",
                fun () ->
                    Path = "/cdmi_domains/user_domain/sub_domain/cdmi_domain_summary/daily/",
                    ?assertMatch("/cdmi_domains/user_domain/sub_domain/", get_domain_from_path(Path)),
                    ?assertException(error, function_clause, get_domain_from_path(not_a_list))
                end
            },
            {"Test get_domain_hash/1",
                fun () ->
                    ?assertMatch(?TestSystemDomainHash, get_domain_hash(?SYSTEM_DOMAIN_URI)),
                    ?assertException(error, function_clause, get_domain_hash(not_a_list_or_binary))
                end
            },
            {"Test get_object_name/1",
                fun () ->
                    ContainerPath = "/cdmi_domains/user_domain/sub_domain/cdmi_domain_summary/daily/",
                    ObjectPath = ContainerPath ++ "dataobject",
                    ?assertMatch("daily/", get_object_name(ContainerPath)),
                    ?assertMatch("dataobject", get_object_name(ObjectPath)),
                    ?assertMatch("/", get_object_name("")),
                    ?assertException(error, function_clause, get_object_name(not_a_list))
                end
            },
            {"Test get_object_oid/1",
                fun () ->
                    Path = "/container/object",
                    Capabilities = "/cdmi_capabilities/container/",
                    Pid = self(),
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                            {<<"auth_as">>, <<"MickeyMouse">>},
                                            {<<"domainURI">>, <<"/new_container/">>}]),
                    State = {Pid, EnvMap},
                    TestRootMap = jsx:decode(?TestRootObject, [return_maps]),
                    Oid = maps:get(<<"objectID">>, TestRootMap),
                    meck:sequence(nebula2_db, search, 2, [{ok, TestRootMap}]),
                    ?assertMatch({ok, Oid}, get_object_oid(Path, State)),
                    ?assertMatch({ok, Oid}, get_object_oid(Capabilities, State)),
                    ?assertException(error, function_clause, get_object_oid(not_a_list, State)),
                    ?assertException(error, function_clause, get_object_oid(Path, not_a_tuple))
                end
            },
            {"Test get_parent_uri/1",
                fun () ->
                    RootPath = "/",
                    FirstLevelPath = "/container/",
                    SecondLevelPath = "/container/object",
                    ?assertMatch(<<"">>, get_parent_uri(RootPath)),
                    ?assertMatch(<<"/">>, get_parent_uri(FirstLevelPath)),
                    ?assertMatch(<<"/container/">>, get_parent_uri(SecondLevelPath)),
                    ?assertMatch(<<"">>, get_parent_uri(list_to_binary(RootPath))),
                    ?assertMatch(<<"/">>, get_parent_uri(list_to_binary(FirstLevelPath))),
                    ?assertMatch(<<"/container/">>, get_parent_uri(list_to_binary(SecondLevelPath))),
                    ?assertException(error, function_clause, get_parent_uri(not_a_list_or_binary))
                end
            },
            {"Test get_time/0",
                fun () ->
                    ?assertMatch("1970-01-01T00:00:00.000000Z", get_time())
                end
            },
            {"Test get_value/2",
                fun () ->
                    TestMap = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                                {<<"sp">>, ?TestCreateContainerSearchPath}
                                                ]),
                    ?assertMatch(<<"Complete">>, get_value(<<"completionStatus">>, TestMap)),
                    ?assertMatch(<<"Complete">>, get_value(<<"completionStatus">>, TestMapCDMI)),
                    ?assertMatch(<<"">>, get_value(<<"missing_key">>, TestMap)),
                    ?assertMatch(<<"">>, get_value(<<"missing_key">>, TestMapCDMI)),
                    ?assertException(error, function_clause, get_value(not_binary, TestMap)),
                    ?assertException(error, function_clause, get_value(<<"key">>, not_a_map))
                end
            },
            {"Test get_value/3",
                fun () ->
                    TestMap = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                                {<<"sp">>, ?TestCreateContainerSearchPath}
                                                ]),
                    ?assertMatch(<<"Complete">>, get_value(<<"completionStatus">>, TestMap, <<"default">>)),
                    ?assertMatch(<<"Complete">>, get_value(<<"completionStatus">>, TestMapCDMI, <<"default">>)),
                    ?assertMatch(<<"default">>, get_value(<<"missing_key">>, TestMap, <<"default">>)),
                    ?assertMatch(<<"default">>, get_value(<<"missing_key">>, TestMapCDMI, <<"default">>)),
                    ?assertException(error, function_clause, get_value(not_binary, TestMap, <<"default">>)),
                    ?assertException(error, function_clause, get_value(<<"key">>, not_a_map, <<"default">>))
                end
            },
            {"Test handle_content_type/1",
                fun () ->
                    Path = "/container/object",
                    Pid = self(),
                    EnvMap = maps:from_list([{<<"auth_as">>, <<"MickeyMouse">>},
                                            {<<"domainURI">>, <<"/new_container/">>},
                                            {<<"path">>, list_to_binary(Path)}
                                            ]),
                    State = {Pid, EnvMap},
                    TestMap = jsx:decode(?TestCreateContainer, [return_maps]),
                    ContentType = maps:get(<<"objectType">>, TestMap),
                    meck:sequence(nebula2_db, search, 2, [{ok, TestMap}]),
                    ?assertMatch(ContentType, handle_content_type(State)),
                    EnvMap2 = maps:put(<<"content-type">>, <<"application/cdmi-container">>, EnvMap),
                    State2 = {Pid, EnvMap2},
                    ?assertMatch(ContentType, handle_content_type(State2)),
                    ?assertException(error, function_clause, handle_content_type(not_a_tuple)),
                    meck:validate(nebula2_db)
                end
            },
            {"Test handle_delete/5",
                fun () ->
                    Child = <<"a_child">>,
                    TestNewObject  = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestContainer = maps:put(<<"children">>, [Child], maps:put(<<"childrenrange">>, <<"0-0">>, TestNewObject)),
                    Path = binary_to_list(maps:get(<<"parentURI">>, TestContainer)) ++ binary_to_list(maps:get(<<"objectName">>, TestContainer)),
                    EnvMap = maps:from_list([{<<"path">>, <<"/new_container/">>},
                                            {<<"auth_as">>, <<"MickeyMouse">>},
                                            {<<"domainURI">>, <<"/new_container/">>},
                                            {<<"object_map">>, TestNewObject}
                                            ]),
                    ChildObject = maps:from_list([{<<"objectName">>, Child},
                                                {<<"objectID">>, <<"id">>},
                                                {<<"parentID">>, maps:get(<<"objectID">>, TestNewObject)},
                                                {<<"parentName">>, list_to_binary(Path)}
                                                ]),
                    Pid = self(),
                    State = {Pid, EnvMap},
                    TestRootMap = jsx:decode(?TestRootObject, [return_maps]),
                    meck:sequence(nebula2_db, delete, 2, [ok, ok, {error, notfound}]),
                    meck:sequence(nebula2_db, search, 2, [{ok, ChildObject},
                                                        {ok, TestNewObject}
                                                        ]),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestContainer},
                                                        {ok, TestRootMap}
                                                    ]),
                    meck:sequence(nebula2_db, update, 3, [{ok, TestNewObject}]),
                    ?assertMatch(ok, handle_delete(TestContainer, State, list_to_binary(Path), maps:get(<<"children">>, TestContainer))),
                    ?assertMatch({error, notfound}, handle_delete(TestContainer, State, list_to_binary(Path), maps:get(<<"children">>, TestContainer))),
                    ?assert(meck:validate(nebula2_db))
                end
            },
            {"Test make_key/0",
                fun () ->
                    meck:expect(uuid, uuid4, [], ?TestUuid4),
                    meck:expect(uuid, to_string, [?TestUuid4], ?TestUidString),
                    ?assertMatch(?TestUid, make_key()),
                    ?assert(meck:validate(uuid))
                end
            },
            {"Test make_search_key/1",
                fun () ->
                    Map = maps:from_list([{<<"path">>, <<"/">>},
                                        {<<"objectName">>, <<"/">>},
                                        {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                        ]),
                    SearchKey = "c8c17baf9a68a8dbc75b818b24269ebca06b0f31/",
                    ?assertMatch(SearchKey, make_search_key(Map)),
                    Map2a = maps:put(<<"objectName">>, <<"/cdmi_domains/">>, Map),
                    Map2b = maps:put(<<"path">>, <<"/cdmi_domains/">>, Map2a),
                    SearchKey2 = "c8c17baf9a68a8dbc75b818b24269ebca06b0f31/cdmi_domains/",
                    ?assertMatch(SearchKey2, make_search_key(Map2b)),
                    Map3a = maps:put(<<"objectName">>, <<"/cdmi_domains/system_domain/">>, Map),
                    Map3b = maps:put(<<"path">>, <<"/cdmi_domains/system_domain/">>, Map3a),
                    SearchKey3 = "c8c17baf9a68a8dbc75b818b24269ebca06b0f31/cdmi_domains/system_domain/",
                    ?assertMatch(SearchKey3, make_search_key(Map3b)),
                    Map4a = maps:put(<<"objectName">>, <<"/cdmi_domains/Fuzzcat/">>, Map),
                    Map4b = maps:put(<<"path">>, <<"/cdmi_domains/Fuzzcat/">>, Map4a),
                    Map4c = maps:put(<<"domainURI">>, <<"/cdmi_domains/Fuzzcat/">>, Map4b),
                    SearchKey4 = "e2f450d94cb7f21e3596f8b953b3ec2791343482/cdmi_domains/Fuzzcat/",
                    ?assertMatch(SearchKey4, make_search_key(Map4c)),
                    Map5a = maps:put(<<"objectName">>, <<"/cdmi_capabilities/">>, Map),
                    Map5b = maps:put(<<"path">>, <<"/cdmi_capabilities/">>, Map5a),
                    Map5c = maps:put(<<"domainURI">>, <<"/cdmi_capabilities/">>, Map5b),
                    SearchKey5 = "e1c36ee8b6b76553d8977eb4737df5b996b418bd/cdmi_capabilities/",
                    ?assertMatch(SearchKey5, make_search_key(Map5c)),
                    ?assertException(error, function_clause, make_search_key(not_a_map))
                end
            },
            {"Test put_value/3",
                fun () ->
                    TestMap = jsx:decode(?TestCreateContainer, [return_maps]),
                    TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                                {<<"sp">>, ?TestCreateContainerSearchPath}
                                                ]),
                    TestCDMI = put_value(<<"new key">>, <<"new value">>, TestMapCDMI),
                    ?assert(maps:is_key(<<"new key">>, maps:get(<<"cdmi">>, TestCDMI))),
                    ?assertMatch(<<"new value">>, maps:get(<<"new key">>, maps:get(<<"cdmi">>, TestCDMI))),
                    TestNonCDMI = put_value(<<"new key">>, <<"new value">>, TestMap),
                    ?assert(maps:is_key(<<"new key">>, TestNonCDMI)),
                    ?assertMatch(<<"new value">>, maps:get(<<"new key">>, TestNonCDMI)),
                    ?assertException(error, function_clause, put_value(not_a_binary, <<"">>, TestMap)),
                    ?assertException(error, function_clause, put_value(<<"">>, <<"">>, not_a_map))
                end
            },
            {"Test sanitize_body/2",
                fun () ->
                    Body = maps:from_list([{<<"objectID">>, <<"val1">>},
                                           {<<"parentID">>, <<"val2">>},
                                           {<<"parentURI">>, <<"val3">>},
                                           {<<"completionStatus">>, <<"val4">>}
                                          ]),
                    Data = sanitize_body([<<"objectID">>,
                                          <<"objectName">>,
                                          <<"parentID">>,
                                          <<"parentURI">>,
                                          <<"completionStatus">>],
                                          Body),
                    ?assertMatch(0, maps:size(Data)),
                    ?assertException(error, function_clause, sanitize_body([], not_a_map))
                end
            },
            {"Test set_cache/1",
                fun () ->
                    TestMap = jsx:decode(?TestCreateContainer, [return_maps]),
                    meck:sequence(mcd, set, 4, [{ok, TestMap}, {ok, TestMap}]),
                    ?assertMatch({ok, TestMap}, set_cache(TestMap)),
                    ?assertException(error, function_clause, set_cache(not_a_map)),
                    meck:validate(mcd)
                end
            },
            {"Test type_of/1",
                fun () ->
                    ?assertMatch(<<"atom">>, type_of(atom)),
                    ?assertMatch(<<"binary">>, type_of(<<"binary">>)),
                    ?assertMatch(<<"binary">>, type_of(<<1:8>>)),
                    ?assertMatch(<<"bitstring">>, type_of(<<1:1>>)),
                    ?assertMatch(<<"boolean">>, type_of(true)),
                    ?assertMatch(<<"boolean">>, type_of(false)),
                    ?assertMatch(<<"float">>, type_of(1.1)),
                    ?assertMatch(<<"function">>, type_of(fun() -> ok end)),
                    ?assertMatch(<<"integer">>, type_of(1)),
                    ?assertMatch(<<"list">>, type_of([])),
                    ?assertMatch(<<"pid">>, type_of(self())),
                    ?assertMatch(<<"tuple">>, type_of({tuple}))
                end
            },
            {"Test update_data_system_metadata/4 when no data to update",
                fun () ->
                    Data = maps:from_list([{<<"key">>, <<"value">>}]),
                    ?assertMatch(Data, update_data_system_metadata([], Data, [], {}))
                end
            },
            {"Test update_data_system_metadata/4 contract",
                fun () ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, update_data_system_metadata(notalist, Map, "", {})),
                    ?assertException(error, function_clause, update_data_system_metadata([], notamap, <<"">>, {})),
                    ?assertException(error, function_clause, update_data_system_metadata([], Map, notalistorbinary, {})),
                    ?assertException(error, function_clause, update_data_system_metadata([], Map, "", notatuple))
                end
            },
            {"Test update_parent/4 when object is root",
                fun () ->
                    Pid = self(),
                    ?assertMatch({ok, <<"">>}, update_parent(<<"">>, "/path", ?CONTENT_TYPE_CDMI_CONTAINER, Pid))
                end
            },
            {"Test update_parent/4",
                fun () ->
                    Pid = self(),
                    Parent = jsx:decode(?TestRootObject, [return_maps]),
                    Data = maps:from_list(([{<<"objectName">>, <<"achild">>},
                                            {<<"objectID">>, <<"oid">>},
                                            {<<"objectType">>, ?CONTENT_TYPE_CDMI_DATAOBJECT},
                                            {<<"parentID">>, maps:get(<<"objectID">>, Parent)},
                                            {<<"parentURI">>, <<"/">>},
                                            {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>}
                                            ])),
                    Path = "/achild",
                    ParentId = maps:get(<<"parentID">>, Data),
                    Children = maps:get(<<"children">>, Parent),
                    NewChildren = lists:append([maps:get(<<"objectName">>, Data)], Children),
                    UpdatedParent = maps:put(<<"children">>, NewChildren, Parent),
                    UpdatedParent2 = maps:put(<<"childrenrange">>, <<"0-10">>, UpdatedParent),
                    ObjectType = maps:get(<<"objectType">>, Data),
                    Oid = maps:get(<<"objectID">>, Data),
                    meck:expect(nebula2_db, read, [Pid, ParentId], {ok, Parent}),
                    meck:sequence(nebula2_db, update, 3, [{ok, UpdatedParent2}]),
                    ?assertException(error, function_clause, update_parent(not_a_binary, Path, ObjectType, Pid)),
                    ?assertException(error, function_clause, update_parent(Oid, not_a_list, ObjectType, Pid)),
                    ?assertException(error, function_clause, update_parent(Oid, Path, not_a_binary, Pid)),
                    ?assertException(error, function_clause, update_parent(Oid, Path, ObjectType, not_a_pid)),
                    meck:validate(nebula2_db)
                end
            }
        ]
    }.

-endif.
