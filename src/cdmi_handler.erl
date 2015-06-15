-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

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
    {ok, Req, {PoolMember, [{<<"envmap">>, maps:new()}]}}.

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
    {Pid, _Opts} = State,
    lager:debug("Entry from_cdmi_capability...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, _} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    Response = nebula2_capabilities:new_capability(Req, State),
    lager:debug("Response from_cdmi_capability: ~p", [Response]),
    {true, Req, State}.

from_cdmi_container(Req, State) ->
    {ok, Body, _} = cowboy_req:body(Req),
    lager:debug("from_cdmi_container Body: ~p", [Body]),
    Response = nebula2_containers:new_container(Req, State),
    lager:debug("Entry from_cdmi_container: ~p", [Response]),
    {true, Req, State}.

from_cdmi_domain(Req, State) ->
    lager:debug("Entry from_cdmi_domain"),
    {<<"{\"jsondoc\": \"domain\"}">>, Req, State}.

from_cdmi_object(Req, State) ->
    {Pid, _Opts} = State,
    lager:debug("from_cdmi_object...~p", [Pid]),
    {Path, _} = cowboy_req:path(Req),
%%    Uri = string:substr(binary_to_list(Path), 6),
    
    lager:debug("Get URI: ~p", [Path]),
%%    Json = nebula2_riak:get(Pid, Uri),
%%    lager:debug("Got Json: ~p", [Json]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number3\"}">>, Req, State}.

is_authorized(Req, State) ->
%% TODO: Check credentials here
    lager:debug("Entry is_authorized"),
    {AuthString, Req2} = cowboy_req:header(<<"authorization">>, Req),
    AuthString2 = binary_to_list(AuthString),
    [AuthMethod, Auth] = string:tokens(AuthString2, " "),
    [Userid, _Password] = case string:to_lower(AuthMethod) of
                            "basic" ->
                                basic(Auth);
                             _Other ->
                                 {error, "Unknown AuthMethod: " ++ AuthMethod}
                         end,
    {Pid, Opts} = State,
    {<<"envmap">>, EnvMap} = lists:keyfind(<<"envmap">>, 1, Opts),
    lager:debug("is_authorized: Userid: ~p EnvMap: ~p", [Userid, EnvMap]),
    NewEnvMap = maps:put(<<"auth_as">>, list_to_binary(Userid), EnvMap),
    lager:debug("is_authorized: NewEnvMap: ~p", [NewEnvMap]),
    NewOpts = lists:keyreplace(<<"envmap">>, 1, Opts, {<<"envmap">>, NewEnvMap}),
    lager:debug("is_authorized: NewOpts: ~p", [NewOpts]),
    NewState = {Pid, NewOpts},
    {true, Req2, NewState}.

is_conflict(Req, State) ->
    {_, Opts} = State,
    lager:debug("Entry is_conflict: ~p", [Opts]),
    {Method, _} = cowboy_req:method(Req),
    Conflicts = handle_is_conflict(Method, lists:member(resource_exists, Opts)),
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
    lager:debug("Entery malformed_request"),
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
    {_Pid, Moved} = State,
    {Method, _} = cowboy_req:method(Req),
    lager:debug("Entry moved permanently, method: ~p", [Method]),
    {_Pid, Moved} = State,
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
%% For non-CDMIobject types that lack a trailing slash,
%% does that resource exist with a trailing slash?
previously_existed(Req, State) ->
    {Pid, _Opts} = State,
    lager:debug("-------------------------------------------------------------------------------------------------"),
    lager:debug("Entry previously_existed"),
    {Path, _} = cowboy_req:path(Req),
    {BinaryAcceptHeader, _} = cowboy_req:header(<<?ACCEPT_HEADER>>, Req, error),
    Response = needs_a_slash(binary_to_list(Path),
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
    {Pid, Opts} = State,
    lager:debug("Entry resource_exists state: ~p", [Opts]),
    {Path, Req2} = cowboy_req:path(Req),
    Uri = "cdmi" ++ string:substr(binary_to_list(Path), 6),
    lager:debug("cdmi_handler:resource_exists: Search Uri is ~p", [Uri]),
    {Response, Opts2} = case nebula2_riak:search(Pid, Uri) of
                            {ok, _Json}      ->
                                lager:debug("resource DOES exist"),
                                {true, lists:append(Opts, [resource_exists])};
                            {error, _Status} ->
                                lager:debug("resource does NOT exist"),
                                {false, lists:append(Opts, [resource_noexist])}
                        end,
    State2 = {Pid, Opts2},
    {Response, Req2, State2}.
    
%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, {error_no_members, _}) ->
    {false, Req, undefined};
service_available(Req, State) ->
    {Pid, _opts} = State,
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
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path(Req),
    Uri = string:substr(binary_to_list(Path), 6),
    lager:debug("cdmi_handler:to_cdmi_object: Search Uri is ~p", [Uri]),
    Response = case nebula2_riak:search(Pid, Uri) of
                   {ok, Json}            -> {list_to_binary(Json), Req, Pid};
                   {error, Status} -> 
                       lager:error("Get error: Status: ~p URI: ~p", [Status, Uri]),
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

%% @doc Does the URI need a trailing slash?
needs_a_slash(Path, State, ?CONTENT_TYPE_CDMI_CONTAINER) ->
    needs_a_slash(Path, State);
needs_a_slash(Path, State, ?CONTENT_TYPE_CDMI_DOMAIN) ->
    needs_a_slash(Path, State);
needs_a_slash(_Path, _State, _Other) ->
    false.
    
needs_a_slash(Path, State) ->
    {Pid, _Opts} = State,
    lager:debug("needs_a_slash: Path ~", [Path]),
    End = string:right(Path, 1),
    case End of
        "/" -> 
            false;
         _  ->
            Path2 = Path ++ "/",
            Uri = string:substr(Path2, 6),
            lager:debug("cdmi_handler:needs_a_slash: Search Uri is ~p", [Uri]),
            case nebula2_riak:search(Pid, Uri) of
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