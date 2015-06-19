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
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_domain/2,
         from_cdmi_object/2,
         forbidden/2,
         is_authorized/2,
         is_conflict/2,
         known_methods/2,
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
    PoolMember = pooler:take_member(riak_pool),
    {Method, Req2} = cowboy_req:method(Req),
    {ok, Body, Req3} = cowboy_req:body(Req2),
    {Url, Req4} = cowboy_req:url(Req3),
    {HostUrl, Req5} = cowboy_req:host_url(Req4),
    {ContentType, Req6} = cowboy_req:header(<<"content-type">>, Req5),
    {ReqPath, Req7} = cowboy_req:path(Req6),
    lager:debug("cdmi_handler: rest_init: Body: ~p", [Body]),
    Map = case Body of
               <<>> ->
                   maps:new();
               _Other ->
                   M = maps:new(),
                   maps:put(<<"body">>, jsx:decode(Body, [return_maps]), M)
           end,
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
    lager:info("Body: ~p", [Body]),
    {ok, Req7, {PoolMember, Map9}}.

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

forbidden(Req, State) ->
%% TODO: Check ACLs here
    lager:debug("Entry forbidden"),
    {false, Req, State}.
%%    {true, Req, State}.
    
from_cdmi_capability(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry from_cdmi_capability...~p", [Pid]),
    Path = maps:get(<<"path">>, EnvMap),
    lager:debug("URI: ~p", [Path]),
    Body = maps:get(<<"body">>, EnvMap),
    lager:debug("Body: ~p", [Body]),
    Response = nebula2_capabilities:new_capability(Req, State),
    lager:debug("Response from_cdmi_capability: ~p", [Response]),
    {true, Req, State}.

from_cdmi_container(Req, State) ->
    {_Pid, EnvMap} = State,
    Body = maps:get(<<"body">>, EnvMap),
    lager:debug("from_cdmi_container Body: ~p", [Body]),
    Response = nebula2_containers:new_container(Req, State),
    lager:debug("Entry from_cdmi_container: ~p", [Response]),
    {true, Req, State}.

from_cdmi_domain(Req, State) ->
    {_Pid, EnvMap} = State,
    Body = maps:get(<<"body">>, EnvMap),
    lager:debug("Entry from_cdmi_domain Body: ~p", [Body]),
    {<<"{\"jsondoc\": \"domain\"}">>, Req, State}.

from_cdmi_object(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("from_cdmi_object...~p", [Pid]),
    Body = maps:get(<<"body">>, EnvMap),
    Path = maps:get(<<"path">>, EnvMap),
%%    Uri = string:substr(binary_to_list(Path), 6),
    
    lager:debug("Get URI: ~p", [Path]),
    lager:debug("Get Body: ~p", [Body]),
%%    Json = nebula2_riak:get(Pid, Uri),
%%    lager:debug("Got Json: ~p", [Json]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number3\"}">>, Req, State}.

is_authorized(Req, State) ->
%% TODO: Check credentials here
    lager:debug("Entry is_authorized"),
    {AuthString, Req2} = cowboy_req:header(<<"authorization">>, Req),
    is_authorized_handler(AuthString, Req2, State).

is_authorized_handler(undefined, Req, State) ->
    {{false, "Basic realm=\"default\""}, Req, State};
is_authorized_handler(AuthString, Req, State) ->
    lager:debug("is_authorized_handler2 AuthString: ~p", [AuthString]),
    AuthString2 = binary_to_list(AuthString),
    [AuthMethod, Auth] = string:tokens(AuthString2, " "),
    [Userid, _Password] = case string:to_lower(AuthMethod) of
                            "basic" ->
                                basic(Auth);
                             _Other ->
                                 {error, "Unknown AuthMethod: " ++ AuthMethod}
                         end,
    {Pid, EnvMap} = State,
    lager:debug("is_authorized: Userid: ~p EnvMap: ~p", [Userid, EnvMap]),
    NewEnvMap = maps:put(<<"auth_as">>, list_to_binary(Userid), EnvMap),
    lager:debug("is_authorized: NewEnvMap: ~p", [NewEnvMap]),
    {true, Req, {Pid, NewEnvMap}}.

is_conflict(Req, State) ->
    {_Pid, EnvMap} = State,
    lager:debug("Entry is_conflict: ~p", [EnvMap]),
    Method = maps:get(<<"method">>, EnvMap),
    Exists = maps:get(<<"exists">>, EnvMap),
    Conflicts = handle_is_conflict(Method, Exists),
    {Conflicts, Req, State}
.
handle_is_conflict(<<"PUT">>, Exists) ->
    lager:debug("handle_is_conflict: PUT exists is ~p", [Exists]),
    Exists;
handle_is_conflict(Method, Exists) ->
        lager:debug("handle_is_conflict:catchall Method ~p exists is ~p", [Method, Exists]),
    false.

known_methods(Req, State) ->
    lager:debug("Entry known_methods"),
    {[<<"GET">>, <<"HEAD">>, <<"POST">>, <<"PUT">>, <<"PATCH">>, <<"DELETE">>, <<"OPTIONS">>], Req, State}.

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
    Method = maps:get(<<"method">>, EnvMap),
    lager:debug("Entry moved permanently, method: ~p", [Method]),
    lager:debug("Leaving moved_permanently"),
    {false, Req, State}.
%    {Moved, Req, State}.

%% Has the resource has moved, temporarily?
moved_temporarily(Req, State) ->
%    {_Pid, _Moved} = State,
    lager:debug("Entry moved temporarily, method"),
    {false, Req, State}.

multiple_choices(Req, State) ->
    lager:debug("Entry multiple_choices"),
    {false, Req, State}.

%% Did the resource exist once upon a time?
%% For non-CDMI object types that lack a trailing slash,
%% does that resource exist with a trailing slash?
previously_existed(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("-------------------------------------------------------------------------------------------------"),
    lager:debug("Entry previously_existed"),
    Path = maps:get(<<"path">>, EnvMap),
    {BinaryAcceptHeader, _} = cowboy_req:header(<<?ACCEPT_HEADER>>, Req, error),
    Response = needs_a_slash(Path,
                             State,
                             binary_to_list(BinaryAcceptHeader)),
    lager:debug("previously_existed Response: ~p", [Response]),
    State2 = {Pid, Response},
    R = case Response of
            {true, _} -> true;
            false     -> false
        end,
    lager:debug("Leaving previously_existed: ~p ~p ~p", [R, Req, State2]),
    {R, Req, State2}.

%% Does the resource exist?
resource_exists(Req, State) ->
    {Pid, EnvMap} = State,
    lager:debug("Entry resource_exists state: ~p", [EnvMap]),
    Method = maps:get(<<"method">>, EnvMap),
    Response = resource_exists_handler(Method, State),
    NewEnvMap = maps:put(<<"exists">>, Response, EnvMap),
    {Response, Req, {Pid, NewEnvMap}}.

resource_exists_handler(<<"GET">>, State) ->
    {_, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    case nebula2_riak:search(Path, State) of
                   {error, _Status} ->
                       false;
                   _Other ->
                       true
               end;
resource_exists_handler(<<"PUT">>, State) ->
    {_, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    case nebula2_riak:search(Path, State) of
                   {error, _Status} ->
                       false;
                   _Other ->
                       true
               end.
    
%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, {error_no_members, _}) ->
    {false, Req, undefined};
service_available(Req, State) ->
    {Pid, _EnvMap} = State,
    Available = case nebula2_riak:ping(Pid) of
        true -> true;
        _ -> false
    end,
    {Available, Req, State}.

to_cdmi_capability(Req, State) ->
    to_cdmi_object(Req, State).

to_cdmi_container(Req, State) ->
    lager:debug("Entry to_cdmi_container"),
    to_cdmi_object(Req, State).

to_cdmi_domain(Req, State) ->
    to_cdmi_object(Req, State).

to_cdmi_object(Req, State) ->
    lager:debug("Entry to_cdmi_object"),
    {Pid, EnvMap} = State,
    Path = maps:get(<<"path">>, EnvMap),
    Response = case nebula2_riak:search(Path, State) of
                   {ok, Json}            -> {list_to_binary(Json), Req, Pid};
                   {error, Status} -> 
                       {"Not Found", cowboy_req:reply(Status, Req, [{<<"content-type">>, <<"text/plain">>}]), Pid}
               end,
    pooler:return_member(riak_pool, Pid),
    lager:debug("to_cdmi_object: Response: ~p", [Response]),
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% Basic Authorization
basic(Auth) ->
    string:tokens(base64:decode_to_string(Auth), ":").

%% @doc Map the domain URI
-spec map_domain_uri(string()) -> string().
map_domain_uri(_HostUrl) ->
    %% @todo Do something useful here.
    "/cdmi_domains/system_domain/".

%% @doc Does the URI need a trailing slash?
needs_a_slash(Path, State, ?CONTENT_TYPE_CDMI_CONTAINER) ->
    needs_a_slash(Path, State);
needs_a_slash(Path, State, ?CONTENT_TYPE_CDMI_DOMAIN) ->
    needs_a_slash(Path, State);
needs_a_slash(_Path, _State, _Other) ->
    false.
    
needs_a_slash(Path, State) ->
    lager:debug("needs_a_slash: Path ~", [Path]),
    End = string:right(Path, 1),
    case End of
        "/" -> 
            false;
         _  ->
            Path2 = Path ++ "/",
            Uri = string:substr(Path2, 6),
            lager:debug("cdmi_handler:needs_a_slash: Search Uri is ~p", [Uri]),
            case nebula2_riak:search(Path2, State) of
                {ok, _Json} ->
                     lager:debug("added a slash - true"),
                     {true, Uri};
                {error, _Status} ->
                     lager:debug("added a slash - false"),
                     false
            end
    end.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.