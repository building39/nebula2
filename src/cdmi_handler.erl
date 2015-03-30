-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

-include("nebula.hrl").
  
-export([init/3]).
-export([content_types_accepted/2,
         content_types_provided/2,
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_object/2,
         forbidden/2,
         is_authorized/2,
         malformed_request/2,
         resource_exists/2,
         rest_init/2,
         service_available/2,
         to_cdmi_capability/2,
         to_cdmi_container/2,
         to_cdmi_object/2]).

init(_, _Req, _Opts) ->
    lager:debug("initing..."),
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _State) ->
    PoolMember = pooler:take_member(riak_pool),
    {ok, Req, PoolMember}.

content_types_accepted(Req, Pid) ->
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, from_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, from_cdmi_container},
      {{<<"application">>, <<"cdmi-object">>, '*'}, from_cdmi_object}
     ], Req, Pid}.

content_types_provided(Req, Pid) ->
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, to_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'}, to_cdmi_container},
      {{<<"application">>, <<"cdmi-object">>, '*'}, to_cdmi_object}
     ], Req, Pid}.

forbidden(Req, Pid) ->
%% TODO: Check ACLs here
    {false, Req, Pid}.
%%    {true, Req, Pid}.
    
from_cdmi_capability(Req, Pid) ->
    lager:debug("from_cdmi_capability...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
%%    Uri = string:substr(binary_to_list(Path), 6),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req2, Pid}.

from_cdmi_container(Req, Pid) ->
    lager:debug("from_cdmi_container...~p", [Pid]),
    {Path, _} = cowboy_req:path_info(Req),
%%    Uri = string:substr(binary_to_list(Path), 6),
    lager:debug("URI: ~p", [Path]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req2, Pid}.

from_cdmi_object(Req, Pid) ->
    lager:debug("from_cdmi_object...~p", [Pid]),
    {Path, _} = cowboy_req:path(Req),
%%    Uri = string:substr(binary_to_list(Path), 6),
    
    lager:debug("Get URI: ~p", [Path]),
%%    Json = nebula2_riak:get(Pid, Uri),
%%    lager:debug("Got Json: ~p", [Json]),
    pooler:return_member(riak_pool, Pid),
    {<<"{\"jsondoc\": \"number1\"}">>, Req, Pid}.

is_authorized(Req, Pid) ->
%% TODO: Check credentials here
    {true, Req, Pid}.
%%    {{false, "You suck!!!"}, Req, Pid}.

%% Malformed request.
%% There must be an X-CDMI-Specification-Version header, and it
%% must request version 1.1
malformed_request(Req, Pid) ->
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
    {Valid, Req, Pid}.

%% Does the resource exist?
resource_exists(Req, Pid) ->
    {Path, _} = cowboy_req:path(Req),
    Uri = string:substr(binary_to_list(Path), 6),
    Response = case nebula2_riak:search(Pid, Uri) of
                   {ok, _Json}      ->
                       true;
                   {error, _Status} ->
                       pooler:return_member(riak_pool, Pid),
                       false
               end,
    {Response, Req, Pid}.
    
%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, error_no_members) ->
    {false, Req, undefined};
service_available(Req, Pid) ->
    Available = case nebula2_riak:ping(Pid) of
        true -> true;
        _ -> false
    end,
    {Available, Req, Pid}.

to_cdmi_capability(Req, Pid) ->
    to_cdmi_object(Req, Pid).

to_cdmi_container(Req, Pid) ->
    to_cdmi_object(Req, Pid).

to_cdmi_object(Req, Pid) ->
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
