-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

-include("nebula.hrl").
  
-export([init/3]).
-export([content_types_accepted/2,
         content_types_provided/2,
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_domain/2,
         from_cdmi_object/2,
         forbidden/2,
         is_authorized/2,
         malformed_request/2,
         moved_permanently/2,
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

content_types_accepted(Req, State) ->
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, from_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, from_cdmi_container},
      {{<<"application">>, <<"cdmi-object">>, '*'}, from_cdmi_object}
     ], Req, State}.

content_types_provided(Req, State) ->
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, to_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, to_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'}, to_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'}, to_cdmi_object}
     ], Req, State}.

forbidden(Req, State) ->
%% TODO: Check ACLs here
    {false, Req, State}.
%%    {true, Req, State}.
    
from_cdmi_capability(Req, State) ->
    {Pid, _Opts} = State,
    lager:debug("from_cdmi_capability...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req2, State}.

from_cdmi_container(Req, State) ->
    {Pid, _Opts} = State,
    lager:debug("from_cdmi_container...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req2, State}.

from_cdmi_domain(Req, State) ->
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
    {<<"{\"jsondoc\": \"number1\"}">>, Req, State}.

is_authorized(Req, State) ->
%% TODO: Check credentials here
    {true, Req, State}.
%%    {{false, "You suck!!!"}, Req, State}.

%% Malformed request.
%% There must be an X-CDMI-Specification-Version header, and it
%% must request version 1.1
malformed_request(Req, State) ->
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

%% Has the resource has moved, permanently?
%% If the request asks for a cdmi-container or cdmi-domain, and
%% the URL does not end with a slash, it has moved permanently.
moved_permanently(Req, State) ->
    {_Pid, Moved} = State,
    {Moved, Req, State}.

%% Did the resource exist once upon a time?
%% For non-CDMIobject types that lack a trailing slash,
%% does that resource exist with a trailing slash?
previously_existed(Req, State) ->
    {Pid, _Opts} = State,
    lager:info("previously_existed"),
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
    {R, Req, State2}.

%% Does the resource exist?
resource_exists(Req, State) ->
    {Pid, _Opts} = State,
    lager:info("resource_exists"),
    {Path, _} = cowboy_req:path(Req),
    Uri = string:substr(binary_to_list(Path), 6),
    Response = case nebula2_riak:search(Pid, Uri) of
                   {ok, _Json}      ->
                       true;
                   {error, _Status} ->
                       pooler:return_member(riak_pool, Pid),
                       false
               end,
    {Response, Req, State}.
    
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
    to_cdmi_object(Req, State).

to_cdmi_domain(Req, State) ->
    to_cdmi_object(Req, State).

to_cdmi_object(Req, State) ->
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
