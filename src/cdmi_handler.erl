-module(cdmi_handler).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").
  
-export([init/3]).
-export([allowed_methods/2,
         allow_missing_post/2,
         content_types_accepted/2,
         content_types_provided/2,
         delete_completed/2,
         expires/2,
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_domain/2,
         from_cdmi_object/2,
         forbidden/2,
         generate_etag/2,
         is_authorized/2,
         is_conflict/2,
         known_methods/2,
         last_modified/2,
         malformed_request/2,
         moved_permanently/2,
         moved_temporarily/2,
         multiple_choices/2,
         previously_existed/2,
         resource_exists/2,
         rest_init/2,
         service_available/2,
         to_cdmi_capability/2,
         to_cdmi_container/2,
         to_cdmi_domain/2,
         to_cdmi_object/2]).

init(_, _Req, _Opts) ->
    lager:debug("initing..."),
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _State) ->
    lager:debug("Entry cdmi_handler:rest_init: Req: ~p", [Req]),
    PoolMember = pooler:take_member(riak_pool),
    {Method, Req2} = cowboy_req:method(Req),
    {Url, Req4} = cowboy_req:url(Req2),
    {HostUrl, Req5} = cowboy_req:host_url(Req4),
    {ContentType, Req6} = cowboy_req:header(<<"content-type">>, Req5),
    {ReqPath, Req7} = cowboy_req:path(Req6),
    {AcceptType, Req8} = cowboy_req:header(<<"accept">>, Req7),
    Map = maps:new(),
    Url_S = binary_to_list(Url),
    HostUrl_S = binary_to_list(HostUrl),
    Map2 = maps:put(<<"method">>, Method, Map),
    Map3 = maps:put(<<"url">>, Url_S, Map2),
    Map4 = maps:put(<<"hosturl">>, HostUrl_S, Map3),
    P = string:tokens(binary_to_list(ReqPath), "/"),
    Path = string:sub_string(Url_S, string:len(HostUrl_S)+1+string:len(lists:nth(1, P))+1),
    Map5 = maps:put(<<"path">>, Path, Map4),
    Map6 = maps:put(<<"domainURI">>, map_domain_uri(HostUrl_S), Map5),
    Parts = string:tokens(Path, "/"),
    lager:debug("Parts: ~p", [Parts]),
    ParentURI = nebula2_utils:get_parent_uri(Parts),
    ObjectName = nebula2_utils:get_object_name(Parts, Path),
    Map7 = maps:put(<<"parentURI">>, ParentURI, Map6),
    Map8 = maps:put(<<"objectName">>, ObjectName, Map7),
    Map9 = maps:put(<<"content-type">>, ContentType, Map8),
    Map10 = maps:put(<<"accept">>, AcceptType, Map9),
    lager:debug("rest_init: EnvMap: ~p", [Map10]),
    {ok, Req8, {PoolMember, Map10}}.

allowed_methods(Req, State) ->
    lager:debug("Entry allowed_methods"),
    {[<<"GET">>, <<"PUT">>, <<"POST">>, <<"HEAD">>, <<"DELETE">>], Req, State}.

allow_missing_post(Req, State) ->
    lager:debug("Entry allow_missing_post"),
    {true, Req, State}.

content_types_accepted(Req, State) ->
    lager:debug("Entry content_types_accepted"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, from_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, from_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'}, from_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'}, from_cdmi_object}
     ], Req, State}.

content_types_provided(Req, State) ->
    lager:debug("Entry content_types_provided"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, to_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, to_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'}, to_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'}, to_cdmi_object}
     ], Req, State}.

delete_completed(Req, State) ->
    lager:debug("Entry delete_completed"),
    {true, Req, State}.

expires(Req, State) ->
    lager:debug("Entry expires"),
    {undefined, Req, State}.

forbidden(Req, State) ->
%% TODO: Check ACLs here
    lager:debug("Entry forbidden"),
    {false, Req, State}.
%%    {true, Req, State}.
    
%% content types accepted
from_cdmi_capability(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry from_cdmi_capability...~p", [Pid]),
    Path = maps:get(<<"path">>, EnvMap),
    lager:debug("URI: ~p", [Path]),
%%    Body = maps:get(<<"body">>, EnvMap),
%%    lager:debug("Body: ~p", [Body]),
    Response = nebula2_capabilities:new_capability(Req, State),
    lager:debug("Response from_cdmi_capability: ~p", [Response]),
    {true, Req, State}.

from_cdmi_container(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry from_cdmi_container"),
    lager:debug("exists? ~p", [maps:get(<<"exists">>, EnvMap)]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    Response = case maps:get(<<"exists">>, EnvMap) of
                    true ->
                        ObjectId = maps:get(<<"objectID">>, maps:get(<<"object_map">>, EnvMap)),
                        BodyMap = jsx:decode(Body, [return_maps]),
                        nebula2_containers:update_container(Pid, ObjectId, BodyMap);
                    false ->
                        nebula2_containers:new_container(Req2, State)
               end,
    lager:debug("Entry from_cdmi_container: ~p", [Response]),
    {true, Req2, State}.

from_cdmi_domain(Req, State) ->
    Response = nebula2_domains:new_domain(Req, State),
    lager:debug("Entry from_cdmi_domain: ~p", [Response]),
    {true, Req, State}.

from_cdmi_object(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry from_cdmi_object...~p", [Pid]),
    Path = maps:get(<<"path">>, EnvMap),
    lager:debug("Get URI: ~p", [Path]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number3\"}">>, Req, State}.

generate_etag(Req, State) ->
    lager:debug("Entry generate_etag"),
    {undefined, Req, State}.

is_authorized(Req, State) ->
    lager:debug("Entry is_authorized"),
    {AuthString, Req2} = cowboy_req:header(<<"authorization">>, Req),
    is_authorized_handler(AuthString, Req2, State).

is_authorized_handler(undefined, Req, State) ->
    lager:debug("Entry is_authorized_handler"),
    {{false, "Basic realm=\"default\""}, Req, State};
is_authorized_handler(AuthString, Req, State) ->
    lager:debug("Entry cdmi_handler:is_authorized_handler2 AuthString: ~p", [AuthString]),
    AuthString2 = binary_to_list(AuthString),
    [AuthMethod, Auth] = string:tokens(AuthString2, " "),
    {Authenticated, UserId} = case string:to_lower(AuthMethod) of
                            "basic" ->
                                basic(Auth, State);
                             _Other ->
                                 lager:error("Unknown AuthMethod: ~p", [AuthMethod]),
                                 {false, undefined}
                         end,
    if
        Authenticated==false ->
            {{false, "Basic realm=\"default\""}, Req, State};
        true ->
            {Pid, EnvMap} = State,
            NewEnvMap = maps:put(<<"auth_as">>, list_to_binary(UserId), EnvMap),
            {true, Req, {Pid, NewEnvMap}}
    end.

is_conflict(Req, State) ->
    lager:debug("Entry is_conflict"),
    {false, Req, State}.
%%    {_Pid, EnvMap} = State,
%%    lager:debug("Entry is_conflict: ~p", [EnvMap]),
%%    Method = maps:get(<<"method">>, EnvMap),
%%    Exists = maps:get(<<"exists">>, EnvMap),
%%    Conflicts = handle_is_conflict(Method, Exists),
%%    {Conflicts, Req, State}.
%%handle_is_conflict(<<"PUT">>, Exists) ->
%%    lager:debug("handle_is_conflict: PUT exists is ~p", [Exists]),
%%    Exists;
%%handle_is_conflict(Method, Exists) ->
%%        lager:debug("handle_is_conflict:catchall Method ~p exists is ~p", [Method, Exists]),
%%    false.

known_methods(Req, State) ->
    lager:debug("Entry known_methods"),
    {[<<"GET">>, <<"HEAD">>, <<"POST">>, <<"PUT">>, <<"PATCH">>, <<"DELETE">>, <<"OPTIONS">>], Req, State}.

last_modified(Req, State) ->
    lager:debug("Entry last_modified"),
    {undefined, Req, State}.

%% Malformed request.
%% There must be an X-CDMI-Specification-Version header, and it
%% must request version 1.1
malformed_request(Req, State) ->
    lager:debug("Enter malformed_request"),
    CDMIVersion = cowboy_req:header(<<?VERSION_HEADER>>, Req, error),
    Valid = case CDMIVersion of
        {error, _} ->
            true;
        {BinaryVersion, _} ->
            Version = binary_to_list(BinaryVersion),
            L = re:replace(Version, "\\s+", "", [global,{return,list}]),
            CDMIVersions = string:tokens(L, ","),
            not lists:member(?CDMI_VERSION, CDMIVersions)
    end,
    {Valid, Req, State}.

%% Has the resource moved, permanently?
%% If the request asks for a cdmi-container or cdmi-domain, and
%% the URL does not end with a slash, it has moved permanently.
moved_permanently(Req, State) ->
    {_Pid, EnvMap} = State,
    lager:debug("-------------------------------------------------------------------------------------------------"),
    lager:debug("Entry moved_permanently"),
    lager:debug("EnvMap: ~p", [EnvMap]),
    Moved = maps:get(<<"moved_permanently">>, EnvMap, false),
    lager:debug("Entry moved permanently, moved: ~p", [Moved]),
    lager:debug("Leaving moved_permanently"),
    lager:debug("-------------------------------------------------------------------------------------------------"),
    {Moved, Req, State}.

%% Has the resource has moved, temporarily?
moved_temporarily(Req, State) ->
%    {_Pid, _Moved} = State,
    lager:debug("Entry moved temporarily, method"),
    {false, Req, State}.

multiple_choices(Req, State) ->
    lager:debug("Entry multiple_choices"),
    lager:debug("Req: ~p", [Req]),
    lager:debug("State: ~p", [State]),
    {false, Req, State}.

%% Did the resource exist once upon a time?
%% For non-CDMI object types that lack a trailing slash,
%% does that resource exist with a trailing slash?
previously_existed(Req, State) ->
    lager:debug("-------------------------------------------------------------------------------------------------"),
    lager:debug("Entry previously_existed"),
    {_, EnvMap2} = State,
    Moved = maps:get(<<"moved_permanently">>, EnvMap2, false),
    R = case Moved of
            false ->
                false;
            _ ->
                true
        end,
    lager:debug("Leaving previously_existed: ~p ~p ~p", [R, Req, State]),
    lager:debug("-------------------------------------------------------------------------------------------------"),
    {R, Req, State}.

%% Does the resource exist?
resource_exists(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry resource_exists state: ~p", [EnvMap]),
    ParentURI = maps:get(<<"parentURI">>, EnvMap),
    {Response, NewState} = resource_exists_handler(ParentURI, State),
    {_, NewEnvMap} = NewState,
    lager:debug("Response: ~p", [Response]),
    lager:debug("NewEnvMap: ~p", [NewEnvMap]),
    NewEnvMap2 = maps:put(<<"exists">>, Response, NewEnvMap),
    lager:debug("Exit resource_exists. response: ~p", [Response]),
    {Response, Req, {Pid, NewEnvMap2}}.

resource_exists_handler("/cdmi_objectid/", State) ->
    lager:debug("Entry resource_exists_handler"),
    {Pid, EnvMap} = State,
    Oid = maps:get(<<"objectName">>, EnvMap),
    case nebula2_riak:get(Pid, Oid) of
                   {error, _Status} ->
                       {false, State};
                   {ok, Data} ->
                       {true, {Pid, maps:put(<<"object_map">>, Data, EnvMap)}}
               end;
resource_exists_handler(_ParentURI, State) ->
    lager:debug("Entry resource_exists_handler"),
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    case nebula2_riak:search(Path, State) of
        {error, _Status} ->
            {false, State};
        {ok, Data} ->
            {true, {Pid, maps:put(<<"object_map">>, Data, EnvMap)}}
    end.
    
%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, {error_no_members, _}) ->
    lager:debug("Entry service_available"),
    {false, Req, undefined};
service_available(Req, State) ->
    lager:debug("Entry service_available"),
    {Pid, _EnvMap} = State,
    Available = case nebula2_riak:ping(Pid) of
        true -> true;
        _ -> false
    end,
    {Available, Req, State}.

%% content types provided
to_cdmi_capability(Req, State) ->
    lager:debug("Entry to_cdmi_capability"),
    to_cdmi_object(Req, State).

to_cdmi_container(Req, State) ->
    lager:debug("Entry to_cdmi_container"),
    to_cdmi_object(Req, State).

to_cdmi_domain(Req, State) ->
    lager:debug("Entry to_cdmi_domain"),
    to_cdmi_object(Req, State).

to_cdmi_object(Req, State) ->
    lager:debug("Entry to_cdmi_object"),
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    Response = to_cdmi_object_handler(Req, State, Path, maps:get(<<"parentURI">>, EnvMap)),
    pooler:return_member(riak_pool, Pid),
    lager:debug("Response: ~p", [Response]),
    Response.

-spec to_cdmi_object_handler(map(), tuple(), string(), string()) -> {map(), term(), pid()} | {notfound, term(), pid()}.
to_cdmi_object_handler(Req, State, _, "/cdmi_objectid/") ->
    lager:debug("Entry to_cdmi_object_handler"),
    {Pid, EnvMap} = State,
    Oid = maps:get(<<"objectName">>, EnvMap),
    case nebula2_riak:get(Pid, Oid) of
        {ok, Map} ->
            Data = list_to_binary(jsx:encode(Map)),
            {Data, Req, State};
        {error, Status} -> 
            {notfound, cowboy_req:reply(Status, Req, [{<<"content-type">>, <<"text/plain">>}]), State}
    end;
to_cdmi_object_handler(Req, State, Path, _) ->
    lager:debug("Entry to_cdmi_object_handler"),
    lager:debug("Req: ~p", [Req]),
    Response = nebula2_riak:search(Path, State),
    lager:debug("Response: ~p", [Response]),
    case nebula2_riak:search(Path, State) of
        {ok, Map} ->
            Data = jsx:encode(Map),
            {Data, Req, State};
        {error, Status} ->
            lager:debug("notfound: Status: ~p", [Status]),
            {notfound, cowboy_req:reply(Status, Req, [{<<"content-type">>, <<"text/plain">>}]), State}
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Basic Authorization
-spec basic(string(), tuple()) -> {true, nonempty_string} | {false, string()}.
basic(Auth, State) ->
    lager:debug("Entry cdmi_handler:basic"),
    {_, EnvMap} = State,
    [UserId, Password] = string:tokens(base64:decode_to_string(Auth), ":"),
    DomainUri = maps:get(<<"domainURI">>, EnvMap) ++ "/cdmi_domain_members/" ++ UserId,
    Result = case nebula2_riak:search(DomainUri, State) of
                 {ok, Json} ->
                     {true, Json};
                 {error, _Status} ->
                     false
             end,
    case Result of
        false ->
            {false, ""};
        {true, Data} ->
            Value = maps:get(<<"value">>, Data),
            VMap = jsx:decode(Value, [return_maps]),
            Creds = binary_to_list(maps:get(<<"cdmi_member_credentials">>, VMap)),
            basic_auth_handler(Creds, UserId, Password)
    end.

-spec basic_auth_handler(list(), nonempty_string(), nonempty_string()) -> {true|false, nonempty_string()}.
basic_auth_handler(Creds, UserId, Password) ->
    lager:debug("Entry cdmi_handler:basic_auth_handler"),
    <<Mac:160/integer>> = crypto:hmac(sha, UserId, Password),
    case Creds == lists:flatten(io_lib:format("~40.16.0b", [Mac])) of
        true ->
            {true, UserId};
        false ->
            {false, "Basic realm=\"default\""}
    end.

%% @doc Map the domain URI
-spec map_domain_uri(string()) -> string().
map_domain_uri(_HostUrl) ->
    %% @todo Do something useful here.
    lager:debug("Entry cdmi_handler:map_domain_uri"),
    "/cdmi_domains/system_domain/".

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
