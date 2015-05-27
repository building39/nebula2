-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

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
    {ok, Req, {PoolMember, []}}.

allowed_methods(Req, State) ->
    lager:info("Entry allowed_methods"),
    {[<<"GET">>, <<"PUT">>, <<"POST">>, <<"HEAD">>, <<"DELETE">>], Req, State}.

allow_missing_post(Req, State) ->
    lager:info("Entry allow_missing_post"),
    {true, Req, State}.

content_types_accepted(Req, State) ->
    lager:info("Entry content_types_accepted"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, from_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, from_cdmi_container},
      {{<<"application">>, <<"cdmi-object">>, '*'}, from_cdmi_object}
     ], Req, State}.

content_types_provided(Req, State) ->
    lager:info("Entry content_types_provided"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, to_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, to_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'}, to_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'}, to_cdmi_object}
     ], Req, State}.

delete_completed(Req, State) ->
    lager:info("Entry delete_completed"),
    {true, Req, State}.

forbidden(Req, State) ->
%% TODO: Check ACLs here
    lager:info("Entry forbidden"),
    {false, Req, State}.
%%    {true, Req, State}.
    
from_cdmi_capability(Req, State) ->
    {Pid, _Opts} = State,
    lager:debug("Entry from_cdmi_capability...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req2, State}.

from_cdmi_container(Req, State) ->
    Response = nebula2_containers:new_container(Req, State),
    lager:info("Entry from_cdmi_container: ~p", [Response]),
%    Response = case nebula2_riak:put(Pid, Oid, Name, Data) of
%                   {ok, _Oid} ->
%                        RespBody = jsx:encode(maps:to_list(Eterm2)),
%                        Req3 = cowboy_req:set_resp_body(RespBody, Req2)
    {true, Req, State}.

from_cdmi_domain(Req, State) ->
    lager:info("Entry from_cdmi_domain"),
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
    lager:info("Entry is_authorized"),
    {true, Req, State}.
%%    {{false, "You suck!!!"}, Req, State}.

is_conflict(Req, State) ->
    lager:info("Entry is_conflict"),
    {false, Req, State}.
%%    {{false, "You suck!!!"}, Req, State}.

known_methods(Req, State) ->
    lager:info("Entry known_methods"),
    {[<<"GET">>, <<"HEAD">>, <<"POST">>, <<"PUT">>, <<"PATCH">>, <<"DELETE">>, <<"OPTIONS">>], Req, State}.

%% Malformed request.
%% There must be an X-CDMI-Specification-Version header, and it
%% must request version 1.1
malformed_request(Req, State) ->
    lager:info("Entery malformed_request"),
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
    lager:info("Entry moved permanently, method: ~p", [Method]),
    {_Pid, Moved} = State,
    lager:info("Leaving moved_permanently"),
    {false, Req, State}.
%    {Moved, Req, State}.

%% Has the resource has moved, temporarily?
moved_temporarily(Req, State) ->
%    {_Pid, _Moved} = State,
    lager:info("Entry moved temporarily, method"),
    {false, Req, State}.

multiple_choices(Req, State) ->
    lager:info("Entry multiple_choices"),
    {false, Req, State}.

%% Did the resource exist once upon a time?
%% For non-CDMIobject types that lack a trailing slash,
%% does that resource exist with a trailing slash?
previously_existed(Req, State) ->
    {Pid, _Opts} = State,
    lager:info("-------------------------------------------------------------------------------------------------"),
    lager:info("Entry previously_existed"),
    {Path, _} = cowboy_req:path(Req),
    {BinaryAcceptHeader, _} = cowboy_req:header(<<?ACCEPT_HEADER>>, Req, error),
    Response = needs_a_slash(binary_to_list(Path),
                             State,
                             binary_to_list(BinaryAcceptHeader)),
    lager:info("previously_existed Response: ~p", [Response]),
    State2 = {Pid, Response},
    R = case Response of
            {true, _} -> true;
            false     -> false
        end,
    lager:info("Leaving previously_existed: ~p ~p ~p", [R, Req, State2]),
    {R, Req, State2}.

%% Does the resource exist?
resource_exists(Req, State) ->
    {Pid, _Opts} = State,
    lager:info("Entry resource_exists"),
    {Path, Req2} = cowboy_req:path(Req),
    Uri = string:substr(binary_to_list(Path), 6),
    {Method, _} = cowboy_req:method(Req),
    Response = case nebula2_riak:search(Pid, Uri) of
                   {ok, _Json}      ->
                       lager:info("resource DOES exist"),
                       true;
                   {error, _Status} ->
                       lager:info("resource does NOT exist"),
                       pooler:return_member(riak_pool, Pid),
                       false
%                       case Method of
%                            <<"PUT">> -> true;
%                                   _ -> false
%                       end
               end,
    
    lager:info("response: ~p", [Response]),
    lager:info("request method: ~p", [Method]),
    lager:info("about to exit resource_exists"),
    {Response, Req2, State}.
    
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
    lager:info("Entry to_cdmi_container"),
    to_cdmi_object(Req, State).

to_cdmi_domain(Req, State) ->
    to_cdmi_object(Req, State).

to_cdmi_object(Req, State) ->
    lager:info("Entry to_cdmi_object"),
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path(Req),
    Uri = string:substr(binary_to_list(Path), 6),
    Response = case nebula2_riak:search(Pid, Uri) of
                   {ok, Json}            -> {list_to_binary(Json), Req, Pid};
                   {error, Status} -> 
                       lager:error("Get error: Status: ~p URI: ~p", [Status, Uri]),
                       {"Not Found", cowboy_req:reply(Status, Req, [{<<"content-type">>, <<"text/plain">>}]), Pid}
               end,
    pooler:return_member(riak_pool, Pid),
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @doc Does the URI need a trailing slash?
needs_a_slash(Path, State, "application/cdmi-container") ->
    needs_a_slash(Path, State);
needs_a_slash(Path, State, "application/cdmi-domain") ->
    needs_a_slash(Path, State);
needs_a_slash(_Path, _State, _Other) ->
    false.
    
needs_a_slash(Path, State) ->
    {Pid, _Opts} = State,
    lager:info("needs_a_slash: Path ~", [Path]),
    End = string:right(Path, 1),
    case End of
        "/" -> 
            false;
         _  ->
            Path2 = Path ++ "/",
            Uri = string:substr(Path2, 6),
            case nebula2_riak:search(Pid, Uri) of
                {ok, _Json} ->
                     lager:info("added a slash - true"),
                     {true, Uri};
                {error, _Status} ->
                     lager:info("added a slash - false"),
                     false
            end
    end.
