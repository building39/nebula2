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
         get_capabilities_uri/2,
         get_domain_uri/2,
         get_name_and_parent/2,
         get_parent/2,
         get_parent_oid/2,
         get_time/0,
         make_key/0
        ]).

%% @doc Check if a string begins with a certain substring.
-spec nebula2_utils:beginswith(string(), string()) -> boolean.
beginswith(Str, Substr) ->
    lager:info("nebula2_utils:beginswith(~p, ~p)", [Str, Substr]),
    case string:left(Str, string:len(Substr)) of
        Substr -> true;
        _ -> false
    end.

%% @doc Get the capabilities URI for the request.
%% @todo Flesh this out after authentication is done.
-spec nebula2_utils:get_capabilities_uri(pid(), string()) -> string().
get_capabilities_uri(_Pid, ObjectName) ->
    lager:debug("nebula2_utils:get_capabilities: objectname: ~p", [ObjectName]),
    case ObjectName of
        "cdmi/" ->
            "/cdmi_capabilities/container/permanent";
        "cdmi/cdmi_domains/" ->
            "/cdmi_capabilities/container/permanent";
        "cdmi/system_configuration/" ->
            "/cdmi_capabilities/container/permanent"
    end.

%% @doc Get the domain URI for the request.
%% @todo Flesh this out after authentication is done.
-spec nebula2_utils:get_domain_uri(pid(), string()) -> string().
get_domain_uri(_Pid, ObjectName) ->
    case ObjectName of
        "cdmi/" ->
            "/cdmi_domains/system_domain";
        "cdmi/cdmi_domains/" ->
            "/cdmi_domains/system_domain";
        "cdmi/system_configuration/" ->
            "/cdmi_domains/system_domain"
    end.

%% @doc Get the object name and parent URI.
-spec nebula2_utils:get_name_and_parent(pid(), string()) -> {string(), string()}.
get_name_and_parent(Pid, Uri) ->
    Tokens = string:tokens(Uri, "/"),
    Name = case string:right(Uri, 1) of
            "/" -> lists:last(Tokens) ++ "/";
            _   -> lists:last(Tokens)
           end,
    {ok, ParentUri, ParentId} = get_parent(Pid, Name),
    {Name, ParentUri, ParentId}.

%% @doc Get the object name and parent URI.
-spec nebula2_utils:get_parent(pid(), string()) -> {{error, notfound, string()}|{ok, string(), string()}}.
get_parent(_Pid, "cdmi/") ->        %% Root has no parent
    {ok, "", ""};
get_parent(Pid, ObjectName) ->
    Tokens = string:tokens(ObjectName, "/"),
    ParentUri = case string:join(lists:droplast(Tokens), "/") of
                    [] -> "cdmi/";
                    PU -> PU ++ "/"
                end,
    lager:debug("get_parent found ParentUri=~p", [ParentUri]),
    case get_parent_oid(Pid, ParentUri) of
        {ok, ParentId} ->
            {ok, ParentUri, binary_to_list(ParentId)};
        {notfound, ""}->
            {error, notfound, ParentUri}
    end.

%% @doc Get the object's parent oid.
-spec nebula2_utils:get_parent_oid(pid(), string()) -> {{ok|notfound, string()}}.
get_parent_oid(Pid, ParentUri) ->
    case nebula2_riak:search(Pid, ParentUri) of
        {error,_} -> 
            {notfound, ""};
        {ok, Data} ->
            {ok, maps:get(<<"objectID">>, maps:from_list(jsx:decode(list_to_binary(Data))))}
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
    ?assert(get_parent(ok, "cdmi/") == {ok, "", ""}).
get_parent_object_test() ->
    Data =  "{\"objectID\":\"parent_oid\",}",
    Path = "cdmi/some/object",
    meck:new(nebula2_riak, [non_strict]),
    meck:expect(nebula2_riak, search, fun(ok, Path) -> {ok, Data} end),
    ?assert(get_parent(ok, Path) == {ok, "cdmi/some/", "parent_oid"}),
    meck:unload(nebula2_riak).
get_parent_notfound_test() ->
    Path = "cdmi/some/object",
    meck:new(nebula2_riak, [non_strict]),
    meck:expect(nebula2_riak, search, fun(ok, Path) -> {error, notfound} end),
    ?assert(get_parent(ok, Path) == {error, notfound, "cdmi/some/"}),
    meck:unload(nebula2_riak).
-endif.
