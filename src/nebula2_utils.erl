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
         extract_parentURI/1,
         get_capabilities_uri/2,
         get_object_name/2,
         get_object_oid/2,
         get_parent_uri/1,
         get_time/0,
         make_key/0,
         update_parent/4
        ]).

%% @doc Check if a string begins with a certain substring.
-spec nebula2_utils:beginswith(string(), string()) -> boolean.
beginswith(Str, Substr) ->
    lager:debug("nebula2_utils:beginswith(~p, ~p)", [Str, Substr]),
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% @doc Extract the Parent URI from the path.
-spec extract_parentURI(list()) -> list().
extract_parentURI(Path) ->
    lager:debug("Path: ~p", [Path]),
    extract_parentURI(Path, "") ++ "/".
-spec extract_parentURI(list(), list()) -> list().
extract_parentURI([], Acc) ->
    lager:debug("Final: ~p", [Acc]),
    Acc;
extract_parentURI([H|T], Acc) ->
    lager:debug("Head: ~p~nTail: ~p~nAcc: ~p", [H, T, Acc]),
    Acc2 = Acc ++ "/" ++ H,
    lager:debug("Acc2: ~p", [Acc2]),
    extract_parentURI(T, Acc2).

%% @doc Get the capabilities URI for the request.
%% @todo Flesh this out after authentication is done.
-spec nebula2_utils:get_capabilities_uri(pid(), string()) -> string().
get_capabilities_uri(_Pid, ObjectName) ->
    case ObjectName of
        "/" ->
            ?PERMANENT_CONTAINER_CAPABILITY_URI;
        "/cdmi_domains/" ->
            ?PERMANENT_CONTAINER_CAPABILITY_URI;
        "/cdmi_domains/system_domain/" ->
            ?DOMAIN_CAPABILITY_URI;
        "/cdmi_domains/cdmi_domain_members/" ->
            ?CONTAINER_CAPABILITY_URI;
        "/cdmi_domains/cdmi_domain_summary/" ->
            ?DOMAIN_SUMMARY_CAPABILITY_URI;
        "/system_configuration/" ->
            ?CONTAINER_CAPABILITY_URI;
        "/system_configuration/environment_variables/" ->
            ?CONTAINER_CAPABILITY_URI
    end.

%% @doc Get the object name.
-spec get_object_name(list(), string()) -> string().
get_object_name(Parts, Path) ->
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

%% @doc Get the object's parent oid.
-spec nebula2_utils:get_object_oid(string(), map()) -> {{ok|notfound, string()}}.
get_object_oid(Path, State) ->
    case nebula2_riak:search(Path, State) of
        {error,_} -> 
            {notfound, ""};
        {ok, Data} ->
            {ok, maps:get(<<"objectID">>, maps:from_list(jsx:decode(list_to_binary(Data))))}
    end.

-spec get_parent_uri(list()) -> string().
get_parent_uri(Parts) ->
    case length(Parts) of
        0 ->
            "";     %% Root has no parent
        1 ->
            "/";
        _ ->
            nebula2_utils:extract_parentURI(lists:droplast(Parts))
    end.

%% @doc Return current time in ISO 8601:2004 extended representation.
-spec nebula2_utils:get_time() -> string().
get_time() ->
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:now_to_universal_time(erlang:now()),
    binary_to_list(iolist_to_binary(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w.000000Z",
                  [Year, Month, Day, Hour, Minute, Second]))).

%% @doc Make a primary key for storing a new object.
-spec nebula2_utils:make_key() -> object_oid().
make_key() ->
    lager:debug("Function make_key"),
    Uid = re:replace(uuid:to_string(uuid:uuid4()), "-", "", [global, {return, list}]),
    Temp = Uid ++ ?OID_SUFFIX ++ "0000",
    Crc = integer_to_list(crc16:crc16(Temp), 16),
    Uid ++ ?OID_SUFFIX ++ Crc.

update_parent(Root, _, _, _) when Root == ""; Root == <<"">> ->
    %% Must be the root, since there is no parent.
        lager:debug("update_parent: root: Nothing to update"),
    ok;
update_parent(ParentId, Path, ObjectType, Pid) ->
    lager:debug("update_parent: ~p ~p ~p ~p", [ParentId, Path, ObjectType, Pid]),
    N = case length(string:tokens(Path, "/")) of
            0 ->
                "";
            1 ->
                "";
            _Other ->
                lists:last(string:tokens(Path, "/"))
        end,
    lager:debug("update_parent: N: ~p", [N]),
    lager:debug("update_parent: child type: ~p", [ObjectType]),
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
    {ok, Parent} = nebula2_riak:get_mapped(Pid, ParentId),
    lager:debug("update_parent got parent: ~p", [Parent]),
    lager:debug("updating parent with child: ~p", [Name]),
    X = maps:get(<<"children">>, Parent, ""),
    lager:debug("capabilities update_parent: X: ~p", [X]),
    Children = case maps:get(<<"children">>, Parent, "") of
                     "" ->
                         [list_to_binary(Name)];
                     Ch ->
                         lager:debug("capabilities update_parent: Ch: ~p", [Ch]),
                         lists:append(Ch, [list_to_binary(Name)])
                 end,
    lager:debug("Children is now: ~p", [Children]),
    ChildRange = case maps:get(<<"childrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    lager:debug("ChildRange is now: ~p", [ChildRange]),
    NewParent1 = maps:put(<<"children">>, Children, Parent),
    NewParent2 = maps:put(<<"childrange">>, list_to_binary(ChildRange), NewParent1),
    O = maps:to_list(NewParent2),
    lager:debug("NewParent2: ~p", [NewParent2]),
    lager:debug("To Encode: ~p", [O]),
    lager:debug("new parent: ~p", [NewParent2]),
    {ok, _Oid} = nebula2_riak:update(Pid, ParentId, NewParent2),
    ok.
%% ====================================================================
%% Internal functions
%% ====================================================================

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
