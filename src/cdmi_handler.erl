%% @author mmartin
%% @doc
%% Server callbacks.
%% @end

-module(cdmi_handler).

-ifdef(TESTX).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include("nebula.hrl").
  
-export([init/3]).
-export([allowed_methods/2,
         allow_missing_post/2,
         content_types_accepted/2,
         content_types_provided/2,
         delete_completed/2,
         delete_resource/2,
         expires/2,
         from_cdmi_capability/2,
         from_cdmi_container/2,
         from_cdmi_domain/2,
         from_cdmi_object/2,
         from_multipart_mixed/2,
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

%% @doc
%% Module initialization.
%% @end
init(_, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

%% @doc
%% Initialize the REST interface.
%% @end
-spec rest_init(cowboy_req:req(), cdmi_state()) -> {ok, cowboy_req:req(), cdmi_state()}.
rest_init(Req, _State) ->
%    ?nebMsg("Entry"),
    PoolMember          = get_pooler(),
    {Method, Req2}      = cowboy_req:method(Req),
    {Url, Req3}         = cowboy_req:url(Req2),
    {HostUrl, Req4}     = cowboy_req:host_url(Req3),
    {ContentType, Req6} = cowboy_req:header(<<"content-type">>, Req4),
    {ReqPath, Req7}     = cowboy_req:path(Req6),
    {AcceptType, Req8}  = cowboy_req:header(<<"accept">>, Req7),
    {Qs, Req9}          = cowboy_req:qs(Req8),
    Url_S      = binary_to_list(Url),
    HostUrl_S  = binary_to_list(HostUrl),
    P          = string:tokens(binary_to_list(ReqPath), "/"),
    Path       = string:sub_string(Url_S, string:len(HostUrl_S)+1+string:len(lists:nth(1, P))+1),
    Path2      = lists:nth(1, string:tokens(Path, "?")),
    ParentURI  = nebula2_utils:get_parent_uri(Path),
    ObjectName = nebula2_utils:get_object_name(Path),
    SysDomainHash = nebula2_utils:get_domain_hash(?SYSTEM_DOMAIN_URI),
    Map   = maps:new(),
    Map2 = case nebula2_db:search(SysDomainHash ++ ?SYSTEM_DOMAIN_URI, {PoolMember, Map}) of
                {ok, Root} ->
                    Map1  = nebula2_utils:put_value(<<"root_oid">>, nebula2_utils:get_value(<<"objectID">>, Root), Map),
                    nebula2_utils:put_value(<<"method">>, Method, Map1);
                _ ->
                    %% This case should only happen on bootstrap.
                    nebula2_utils:put_value(<<"method">>, Method, Map)
           end,
    Map3  = nebula2_utils:put_value(<<"url">>, Url, Map2),
    Map4  = nebula2_utils:put_value(<<"hosturl">>, HostUrl, Map3),
    Map5  = nebula2_utils:put_value(<<"path">>, list_to_binary(Path2), Map4), %% strip out query string if present
    Map6  = nebula2_utils:put_value(<<"domainURI">>, list_to_binary(map_domain_uri(PoolMember, HostUrl_S)), Map5),
    DomainUri = case nebula2_utils:beginswith(Path, "/cdmi_capabilities/") of
                    true ->
                        <<"">>;
                    false ->
                        nebula2_utils:get_value(<<"domainURI">>, Map6)
                end,
    Map7  = nebula2_utils:put_value(<<"parentURI">>, ParentURI, Map6),
    Map8  = nebula2_utils:put_value(<<"objectName">>, list_to_binary(ObjectName), Map7),
    Map9  = nebula2_utils:put_value(<<"content-type">>, ContentType, Map8),
    Map10 = nebula2_utils:put_value(<<"accept">>, AcceptType, Map9),
    Map11 = nebula2_utils:put_value(<<"qs">>, Qs, Map10),
    NoDomain = nebula2_utils:get_domain_hash(<<"">>),
    SysCapKey = NoDomain ++ "/cdmi_capabilities/",
    SysCap = case nebula2_db:search(SysCapKey, {PoolMember, Map11}) of 
                {ok, SystemCapabilities} ->
                    Cap = nebula2_utils:get_value(<<"capabilities">>, SystemCapabilities, maps:new()),
                    Cap;
                _ ->
                    maps:new()
            end,
    Map12 = nebula2_utils:put_value(<<"system_capabilities">>, SysCap, Map11),
    DomainHash =  nebula2_utils:get_domain_hash(DomainUri),
    SystemDomainHash =  nebula2_utils:get_domain_hash(<<"/cdmi_domains/system_domain/">>),
    Parent = if
                 Path == "/cdmi_capabilities/" ->
                     get_parent(ParentURI, SystemDomainHash, {PoolMember, Map12});
                 true ->
                     get_parent(ParentURI, DomainHash, {PoolMember, Map12})
             end,
    Map13 = nebula2_utils:put_value(<<"is_envmap">>, true, Map12),
    FinalMap = case Parent of
                    {ok, FData} ->
                        ParentID  = nebula2_utils:get_value(<<"objectID">>, FData),
                        Map14     = nebula2_utils:put_value(<<"parentID">>, ParentID, Map13),
                        KeyExists = maps:is_key(<<"capabilitiesURI">>, FData),
                        if 
                            KeyExists == true ->
                                ParentCapabilitiesURI = nebula2_utils:get_value(<<"capabilitiesURI">>, FData),
                                ParentCapabilities    = nebula2_db:search(NoDomain ++ ParentCapabilitiesURI,
                                                                          {PoolMember, Map14}),
                                case ParentCapabilities of
                                    {ok, CapData} ->
                                        CapMap = nebula2_utils:get_value(<<"capabilities">>, CapData, maps:new()),
                                        nebula2_utils:put_value(<<"parent_capabilities">>, CapMap, Map14);
                                    {error, _} ->
                                        Map14
                                end;
                            true ->
                                Map13
                        end;
                   {error, _} ->
                       Map13
               end,
%    ?nebFmt("Exit: ~p", [FinalMap]),
    {ok, Req9, {PoolMember, FinalMap}}.

%% @doc
%% Return a list of allowed methods.
%% @end
-spec allowed_methods(cowboy_req:req(), cdmi_state()) -> {list(), cowboy_req:req(), cdmi_state()}.
allowed_methods(Req, State) ->
%    ?nebMsg("Entry"),
    {[<<"GET">>, <<"PUT">>, <<"POST">>, <<"HEAD">>, <<"DELETE">>], Req, State}.

%% @doc
%% Allow `POST' on an object that does not exist.
%% @end
-spec allow_missing_post(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
allow_missing_post(Req, State) ->
%    ?nebMsg("Entry"),
    {true, Req, State}.

%% @doc
%% Return a list of acceptable content types.
%% @end
-spec content_types_accepted(cowboy_req:req(), cdmi_state()) -> {list(), cowboy_req:req(), cdmi_state()}.
content_types_accepted(Req, State) ->
%    ?nebMsg("Entry"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, from_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'},  from_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'},     from_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'},     from_cdmi_object},
	  {{<<"multipart">>, <<"mixed">>, '*'},             from_multipart_mixed}
     ], Req, State}.

%% @doc
%% Return a list of content types that this server provides.
%% @end
-spec content_types_provided(cowboy_req:req(), cdmi_state()) -> {list(), cowboy_req:req(), cdmi_state()}.
content_types_provided(Req, State) ->
%    ?nebMsg("Entry"),
    {[{{<<"application">>, <<"cdmi-capability">>, '*'}, to_cdmi_capability},
      {{<<"application">>, <<"cdmi-container">>, '*'},  to_cdmi_container},
      {{<<"application">>, <<"cdmi-domain">>, '*'},     to_cdmi_domain},
      {{<<"application">>, <<"cdmi-object">>, '*'},     to_cdmi_object}
     ], Req, State}.

%% @doc
%% Return true if the `DELETE' method has completed.
%% @end
-spec delete_completed(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
delete_completed(Req, State) ->
    %% TODO: make this handle large deletes, like a container with lots of objects.
%    ?nebMsg("Entry"),
    {true, Req, State}.
%% @doc
%% DELETE a resource.
%% @end
-spec delete_resource(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
delete_resource(Req, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    try
        case nebula2_utils:beginswith(Path, "/cdmi_domains/") of
            true ->
                case nebula2_domains:delete_domain(State)of
                    ok ->
                        {true, Req, State};
                    _O ->
                        ?nebFmt("domain delete failed: ~p", [_O]),
                        bail(403, <<"Cannot delete this domain\n">>, Req, State)
                    end;
            false ->
                case nebula2_utils:delete(State) of
                    ok ->
                        {true, Req, State};
                    _ ->
                        bail(400, <<"Delete failed\n">>, Req, State)
                end
        end
    catch
        throw:badjson ->
            bail(400, <<"bad json\n">>, Req, State);
        throw:forbidden ->
            bail(403, <<"Cannot delete this domain\n">>, Req, State)
    end.

%% @doc
%% Return the date of expiration of the resource.
%% This date will be sent as the value of the expires header. Not yet implemented.
%% @end
-spec expires(cowboy_req:req(), cdmi_state()) -> {undefined, cowboy_req:req(), cdmi_state()}.
expires(Req, State) ->
%    ?nebMsg("Entry"),
    {undefined, Req, State}.

%% @doc
%% Check ACLs. Not yet implemented.
%% @end
-spec forbidden(cowboy_req:req(), cdmi_state()) -> {false, cowboy_req:req(), cdmi_state()}.
forbidden(Req, State) ->
%% TODO: Check ACLs here
%    ?nebMsg("Entry"),
    {false, Req, State}.
    
%% @doc
%% Process a capability object request from the client.
%% @end
-spec from_cdmi_capability(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
from_cdmi_capability(Req, State) ->
%    ?nebMsg("Entry"),
    {_Pid, EnvMap} = State,
    try
        case nebula2_utils:get_value(<<"exists">>, EnvMap) of
            <<"true">> ->
                ObjectMap = nebula2_utils:get_value(<<"object_map">>, EnvMap),
                ObjectId = nebula2_utils:get_value(<<"objectID">>, ObjectMap),
                nebula2_capabilities:update_capability(Req, State, ObjectId);
            <<"false">> ->
                nebula2_capabilities:new_capability(Req, State)
        end
    catch
        throw:badjson ->
            bail(400, <<"bad json\n">>, Req, State);
        throw:badencoding ->
            bail(400, <<"bad encoding\n">>, Req, State)
    end.

%% @doc
%% Process a container object request from the client.
%% @end
-spec from_cdmi_container(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
from_cdmi_container(Req, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    ObjectName = binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)),
    LastChar = string:substr(ObjectName, string:len(ObjectName)),
    try
        case LastChar of
            "/" ->
                Response = case nebula2_utils:get_value(<<"exists">>, EnvMap) of
                                <<"true">> ->
                                    ObjectMap = nebula2_utils:get_value(<<"object_map">>, EnvMap),
                                    ObjectId = nebula2_utils:get_value(<<"objectID">>, nebula2_utils:get_value(<<"object_map">>, EnvMap)),
                                    ?nebFmt("object id: ~p", [ObjectId]),
                                    nebula2_containers:update_container(Req, State, ObjectId);
                                <<"false">> ->
                                    nebula2_containers:new_container(Req, State)
                           end,
                Response;
            _ ->
                bail(400, <<"container name must end with '/'\n">>, Req, State)
        end
    catch
        throw:badjson ->
            bail(400, <<"bad json\n">>, Req, State);
        throw:badencoding ->
            bail(400, <<"bad encoding\n">>, Req, State)
    end.

%% @doc
%% Process a domain object request from the client.
%% @end
-spec from_cdmi_domain(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
from_cdmi_domain(Req, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    case string:substr(Path, length(Path)) of
        "/" ->
            nebula2_domains:new_domain(Req, State),
            {true, Req, State};
        _  ->
            bail(400, <<"Bad Request\n">>, Req, State)
    end.

%% @doc
%% Process a data object request from the client.
%% @end
-spec from_cdmi_object(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
from_cdmi_object(Req, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    ObjectName = binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)),
    LastChar = string:substr(ObjectName, string:len(ObjectName)),
    try
       case LastChar of
           "/" ->
               lager:error("data object name must not end with '/'"),
               bail(400, <<"data object name must not end with '/'\n">>, Req, State);
           _ ->
                {ok, Body, Req2} = cowboy_req:body(Req),
                Body2 = try jsx:decode(Body, [return_maps]) of
                            NewBody ->
                                NewBody
                        catch
                            error:badarg ->
                                throw(badjson)
                        end,
                case nebula2_utils:get_value(<<"exists">>, EnvMap) of
                    <<"true">> ->
                        ObjectId = nebula2_utils:get_value(<<"objectID">>, nebula2_utils:get_value(<<"object_map">>, EnvMap)),
                        nebula2_dataobjects:update_dataobject(Req2, State, ObjectId, Body2);
                    <<"false">> ->
                        nebula2_dataobjects:new_dataobject(Req2, State, Body2)
                end
        end
    catch
        throw:badjson ->
            lager:error("bad json"),
            bail(400, <<"bad json\n">>, Req, State);
        throw:badencoding ->
            lager:error("bad encoding"),
            bail(400, <<"bad encoding\n">>, Req, State)
    end.

%% @doc
%% Process a multipart mixed object request from the client.
%% @end
-spec from_multipart_mixed(cowboy_req:req(), cdmi_state()) -> {true | halt, cowboy_req:req(), cdmi_state()}.
from_multipart_mixed(Req, State) ->
%    ?nebMsg("Enter"),
    {_, EnvMap} = State,
    SysCap = nebula2_utils:get_value(<<"system_capabilities">>, EnvMap, maps:new()),
    case nebula2_utils:get_value(<<"cdmi_multipart_mime">>, SysCap, <<"false">>) of
        <<"true">> ->
            from_multipart_mixed(Req, State, ok);
        _ ->
            lager:error("multipart not supported"),
            bail(501, <<"multipart not supported">>, Req, State)
    end.

from_multipart_mixed(Req, State, ok) ->
%    ?nebMsg("Enter"),
    {_, EnvMap} = State,
    ObjectName = binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)),
    LastChar = string:substr(ObjectName, string:len(ObjectName)),
    try
       case LastChar of
           "/" ->
                lager:error("data object name must not end with '/'"),
                bail(400, <<"data object name must not end with '/'\n">>, Req, State);
            _ ->
                {Req2, [BodyPart1, BodyPart2]} = multipart(Req, []),
                Body = try jsx:decode(BodyPart1, [return_maps]) of
                           NewBody ->
                               NewBody
                       catch
                           error:badarg ->
                               throw(badjson)
                       end,
                NewBody2 = nebula2_utils:put_value(<<"value">>, BodyPart2, Body),
                case nebula2_utils:get_value(<<"exists">>, EnvMap) of
                    <<"true">> ->
                        ObjectId = nebula2_utils:get_value(<<"objectID">>, nebula2_utils:get_value(<<"object_map">>, EnvMap)),
                        nebula2_dataobjects:update_dataobject(Req, State, ObjectId, NewBody2);
                    <<"false">> ->
                        
                        nebula2_dataobjects:new_dataobject(Req, State, NewBody2)
            end
        end
    catch
        throw:badjson ->
            lager:error("bad json"),
            bail(400, <<"bad json\n">>, Req, State);
        throw:badencoding ->
            lager:error("bad encoding"),
            bail(400, <<"bad encoding\n">>, Req, State)
    end.

%% @doc
%% Generate an ETag. Not yet implemented.
%% @end
-spec generate_etag(cowboy_req:req(), cdmi_state()) -> {undefined, cowboy_req:req(), cdmi_state()}.
generate_etag(Req, State) ->
%    ?nebMsg("Entry"),
    {undefined, Req, State}.

%% @doc
%% Check if the client is authorized.
%% @end
-spec is_authorized(cowboy_req:req(), cdmi_state()) -> {true | {false, string()}, cowboy_req:req(), cdmi_state()}.
is_authorized(Req, State) ->
%    ?nebMsg("Entry"),
    {AuthString, Req2} = cowboy_req:header(<<"authorization">>, Req),
    is_authorized_handler(AuthString, Req2, State).

%% @doc
%% Return whether the put action results in a conflict.
%% A `409 Conflict' response will be sent if this function returns true. Not yet implemented.
%% @end
-spec is_conflict(cowboy_req:req(), cdmi_state()) -> {false, cowboy_req:req(), cdmi_state()}.
is_conflict(Req, State) ->
%    ?nebMsg("Entry"),
    {false, Req, State}.
%%    {_Pid, EnvMap} = State,
%%    % ?nebFmt("Entry is_conflict: ~p", [EnvMap]),
%%    Method = nebula2_utils:get_value(<<"method">>, EnvMap),
%%    Exists = nebula2_utils:get_value(<<"exists">>, EnvMap),
%%    Conflicts = handle_is_conflict(Method, Exists),
%%    {Conflicts, Req, State}.
%%handle_is_conflict(<<"PUT">>, Exists) ->
%%    % ?nebFmt("handle_is_conflict: PUT exists is ~p", [Exists]),
%%    Exists;
%%handle_is_conflict(Method, Exists) ->
%%        % ?nebFmt("handle_is_conflict:catchall Method ~p exists is ~p", [Method, Exists]),
%%    false.

%% @doc
%% Return a list of methods that this server is willing to handle.
%% @end
-spec known_methods(cowboy_req:req(), cdmi_state()) -> {list(), cowboy_req:req(), cdmi_state()}.
known_methods(Req, State) ->
%    ?nebMsg("Entry"),
    {[<<"GET">>, <<"HEAD">>, <<"POST">>, <<"PUT">>, <<"PATCH">>, <<"DELETE">>, <<"OPTIONS">>], Req, State}.

%% @doc
%% Return the date of last modification of the resource.
%% This date will be used to test against the if-modified-since and if-unmodified-since headers
%% and sent as the last-modified header in the response of `GET' and `HEAD' requests.
%% Not yet implemented.
%% @end
-spec last_modified(cowboy_req:req(), cdmi_state()) -> {undefined, cowboy_req:req(), cdmi_state()}.
last_modified(Req, State) ->
%    ?nebMsg("Entry"),
    {undefined, Req, State}.

%% @doc
%% Check for well formed request.
%% There must be an `X-CDMI-Specification-Version' header, and it
%% must request version 1.1
%% @end
-spec malformed_request(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
malformed_request(Req, State) ->
%    ?nebMsg("Entry"),
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

%% @doc
%% Check to see if the resource has been permanently moved.
%% If the request asks for a cdmi-container or cdmi-domain, and
%% the URL does not end with a slash, it has moved permanently.
%% @end
-spec moved_permanently(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
moved_permanently(Req, State) ->
%    ?nebMsg("Entry"),
    {_Pid, EnvMap} = State,
    Moved = nebula2_utils:get_value(<<"moved_permanently">>, EnvMap, false),
    {Moved, Req, State}.

%% @doc
%% Check to see if the resource has been temporarily moved.
%% Not yet implemented.
%% @end
-spec moved_temporarily(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
moved_temporarily(Req, State) ->
%    ?nebMsg("Entry"),
    {false, Req, State}.

%% @doc
%% Return whether there are multiple representations of the resource.
%% Probably won't be implemented.
%% @end
-spec multiple_choices(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
multiple_choices(Req, State) ->
%    ?nebMsg("Entry"),
    {false, Req, State}.

%% @doc
%% Check to see if the resource has ever existed.
%% For non-CDMI object types or CDMI containers that lack a trailing slash,
%% does that resource exist with a trailing slash?
%% @end
-spec previously_existed(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
previously_existed(Req, State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    case [lists:last(binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)))] of
        "/" ->
            {false, Req, State};
        _Other ->
            Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)) ++ "/",
            ObjectName = binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)) ++ "/",
            EnvMap2 = maps:update(<<"objectName">>, list_to_binary(ObjectName), EnvMap),
            EnvMap3 = maps:update(<<"path">>, list_to_binary(Path), EnvMap2),
            State2 = {Pid, EnvMap3},
            {Response, Req3, State3} = resource_exists(Req, State2),
            {Pid, EnvMap4} = State3,
            EnvMap5 = case Response of 
                        true ->
                            nebula2_utils:put_value(<<"moved_permanently">>, {true, Path}, EnvMap4);
                        false ->
                            nebula2_utils:put_value(<<"moved_permanently">>, false, EnvMap4)
                      end,
            {Response, Req3, {Pid, EnvMap5}}
    end.

%% @doc
%% Check to see if the resource exists.
%% @end
-spec resource_exists(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
resource_exists(Req, State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
%    ?nebFmt("EnvMap: ~p", [EnvMap]),
    ParentURI = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, EnvMap)),
    {Response, NewReq, NewState} = resource_exists_handler(ParentURI, Req, State),
    {_, NewEnvMap} = NewState,
    NewEnvMap2 = nebula2_utils:put_value(<<"exists">>, Response, NewEnvMap),
    {Response, NewReq, {Pid, NewEnvMap2}}.

%% @doc
%% Check to see if the service is currently available.
%% If `pooler' reports that it has no members, a `503 Service Unavailable' response will be returned.
%% @end
-spec service_available(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
service_available(Req, {error_no_members, _}) ->
%    ?nebMsg("<--------------------- Request Start ---------------------------->"),
    {false, Req, undefined};
service_available(Req, State) ->
%    ?nebMsg("<--------------------- Request Start ---------------------------->"),
    {Pid, _EnvMap} = State,
    Available = case nebula2_db:available(Pid) of
        true -> true;
        _ -> false
    end,
    {Available, Req, State}.

%% @doc
%% Return a capability object.
%% @end
-spec to_cdmi_capability(cowboy_req:req(), cdmi_state()) -> {json_value() | notfound, cowboy_req:req(), cdmi_state()}.
to_cdmi_capability(Req, State) ->
%    ?nebMsg("Entry"),
    to_cdmi_object(Req, State).

%% @doc
%% Return a container object.
%% @end
-spec to_cdmi_container(cowboy_req:req(), cdmi_state()) -> {json_value() | notfound, cowboy_req:req(), cdmi_state()}.
to_cdmi_container(Req, State) ->
%    ?nebMsg("Entry"),
    to_cdmi_object(Req, State).

%% @doc
%% Return a domain object.
%% @end
-spec to_cdmi_domain(cowboy_req:req(), cdmi_state()) -> {json_value() | notfound, cowboy_req:req(), cdmi_state()}.
to_cdmi_domain(Req, State) ->
%    ?nebMsg("Entry"),
    to_cdmi_object(Req, State).

%% @doc
%% Return a CDMI object.
%% @end
-spec to_cdmi_object(cowboy_req:req(), cdmi_state()) -> {json_value() | notfound, cowboy_req:req(), cdmi_state()}.
to_cdmi_object(Req, State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    Response = to_cdmi_object_handler(Req, State, Path, binary_to_list(nebula2_utils:get_value(<<"parentURI">>, EnvMap))),
    pooler:return_member(riak_pool, Pid),
    Response.



%% ====================================================================
%% Internal functions
%% ====================================================================

%% Bail out
bail(Reason, Msg, Req, State) ->
    {ok, Reply} = cowboy_req:reply(Reason, [], Msg, Req),
    {halt, Reply, State}.

%% Basic Authorization
-spec basic(string(), cdmi_state()) -> {true, nonempty_string} | {false, string()}.
basic(Auth, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    [UserId, Rest] = string:tokens(base64:decode_to_string(Auth), ":"),
    P = string:tokens(Rest, ";"),
    Password = lists:nth(1, P),
    DomainUri = if
                    length(P) == 1 ->
                        nebula2_utils:get_value(<<"domainURI">>, EnvMap);
                    true ->
                        Opts = string:tokens(lists:nth(2, P), ","),
                        OptMap = parse_options(Opts),
                        Realm = maps:get("realm", OptMap, ""),
                        if
                            Realm == "" ->
                                nebula2_utils:get_value(<<"domainURI">>, EnvMap);
                            true ->
                                list_to_binary("/cdmi_domains/" ++ Realm ++ "/")
                        end
                end,
    ?nebFmt("Realm-based domain: ~p", [DomainUri]),
    Domain = nebula2_utils:get_domain_hash(DomainUri),
    SearchKey = Domain ++ binary_to_list(DomainUri) ++ "cdmi_domain_members/" ++ UserId,
    Result = case nebula2_db:search(SearchKey, State) of
                {ok, Json} ->
                    {true, Json};
                {error, _Status} ->
                    false
             end,
    case Result of
        false ->
            {false, ""};
        {true, Data} ->
            Value = nebula2_utils:get_value(<<"value">>, Data),
            VMap = jsx:decode(Value, [return_maps]),
            Creds = binary_to_list(nebula2_utils:get_value(<<"cdmi_member_credentials">>, VMap)),
            basic_auth_handler(Creds, UserId, Password)
    end.

-spec basic_auth_handler(list(), nonempty_string(), nonempty_string()) -> {true|false, nonempty_string()}.
basic_auth_handler(Creds, UserId, Password) ->
%    ?nebMsg("Entry"),
    <<Mac:160/integer>> = crypto:hmac(sha, UserId, Password),
    case Creds == lists:flatten(io_lib:format("~40.16.0b", [Mac])) of
        true ->
            {true, UserId};
        false ->
            {false, "Basic realm=\"default\""}
    end.

-spec get_domain(list(), string()) -> string().
get_domain(Maps, HostUrl) ->
%    ?nebMsg("Entry"),
    {ok, UrlParts} = http_uri:parse(HostUrl),
    Url = element(3, UrlParts),
    req_domain(Maps, Url).

-spec get_parent(binary() | string(), string(), cdmi_state()) -> {ok, map()} | {error, term()}.
get_parent(ParentUri, Domain, State) when is_binary(ParentUri), is_list(Domain), is_tuple(State) ->
    get_parent(binary_to_list(ParentUri), Domain, State);
get_parent(ParentUri, Domain, State) when is_list(ParentUri), is_list(Domain), is_tuple(State) ->
    case ParentUri of
        "" ->
            {error, notfound};
        Uri ->
            nebula2_db:search(Domain ++ Uri, State)
    end.

-spec get_pooler() -> pid() | error_no_members.
get_pooler() ->
    case pooler:take_member(riak_pool) of
        error_no_members ->
            Retries = 5,
            get_pooler_retry(Retries);
        Pid ->
            Pid
    end.

-spec get_pooler_retry(integer()) -> pid() | error_no_members.
get_pooler_retry(Retries) when Retries == 0 ->
%    ?nebMsg(error, "Out of pool connections."),
    error_no_members;
get_pooler_retry(Retries) ->
    timer:sleep(1000),        %% give pooler 1 second to get more connections.
    case get_pooler() of
        error_no_members ->
            get_pooler_retry(Retries - 1);
        Pid ->
            Pid
    end.

%% @doc Handle query string
-spec handle_query_string(map(), string()) -> map().
handle_query_string(Data, []) ->
    Data;
handle_query_string(Data, Qs) ->
    Parameters = string:tokens(Qs, ";"),
    map_build(Parameters, Data, maps:new()).

is_authorized_handler(undefined, Req, State) ->
%    ?nebMsg("Entry"),
    {{false, "Basic realm=\"default\""}, Req, State};
is_authorized_handler(AuthString, Req, State) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
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
            NewEnvMap = nebula2_utils:put_value(<<"auth_as">>, list_to_binary(UserId), EnvMap),
            {true, Req, {Pid, NewEnvMap}}
    end.

%% @doc Build the output data based on the query string.
-spec map_build(list(), map(), map()) -> map().
map_build([], _, NewMap) ->
    NewMap;
map_build([H|T], Map, NewMap) ->
    QueryItem = string:tokens(H, ":"),
    FieldName = list_to_binary(lists:nth(1, QueryItem)),
    Data = case length(QueryItem) of
               1 ->
                   nebula2_utils:get_value(FieldName, Map, "");
               2 ->
                   map_build_get_data(FieldName, lists:nth(2, QueryItem), Map)
           end,
    NewMap2 = nebula2_utils:put_value(FieldName, Data, NewMap),
    map_build(T, Map, NewMap2).

map_build_get_data(<<"children">>, Range, Map) ->
    [S, E] = string:tokens(Range, "-"),
    {Start, _} = string:to_integer(S),
    {End, _} = string:to_integer(E),
    Num = End - Start + 1,
    Data = nebula2_utils:get_value(<<"children">>, Map, ""),
    lists:sublist(Data, (Start+1), Num);
map_build_get_data(<<"metadata">>, MDField, Map) ->
    MetaData = nebula2_utils:get_value(<<"metadata">>, Map, ""),
    nebula2_utils:get_value(list_to_binary(MDField), MetaData, "");
map_build_get_data(FieldName, _, Map) ->
    nebula2_utils:get_value(FieldName, Map, "").

%% @doc Map the domain URI
-spec map_domain_uri(pid(), string()) -> string().
map_domain_uri(Pid, HostUrl) ->
%    ?nebMsg("Entry"),
    Maps = jsx:decode(nebula2_db:get_domain_maps(Pid)),
    D = get_domain(Maps, HostUrl),
    Domain = case nebula2_utils:beginswith(D, "/cdmi_domains") of
                 true ->
                     D;
                 false ->
                     case string:substr(D, 1, 1) == "/" of
                         true ->
                             "/cdmi_domains" ++ D;
                         false ->
                             "/cdmi_domains/" ++ D
                     end
             end,
%    ?nebFmt("Exit: ~p", [Domain]),
    Domain.

-spec multipart(cowboy_req:req(), cdmi_state()) -> {cowboy_req:req(), cdmi_state()}.
multipart(Req, BodyParts) ->
    case cowboy_req:part(Req) of
        {ok, _Headers, Req2} ->
            {ok, Body, Req3} = cowboy_req:part_body(Req2),
            BodyParts2 = lists:append(BodyParts, [Body]),
            multipart(Req3, BodyParts2);
        {done, Req2} ->
            {Req2, BodyParts}
    end.

-spec parse_options(list()) -> map().
parse_options(List) ->
    parse_options(List, maps:new()).

-spec parse_options(list(), map()) -> map().
parse_options([], Map) ->
    Map;
parse_options([H|T], Map) ->
    [Option, Value] = string:tokens(H, "="),
    Map2 = maps:put(Option, Value, Map),
    parse_options(T, Map2).


-spec req_domain(list(), string()) -> string().
req_domain([], _) ->
%    ?nebMsg("Entry"),
    "/cdmi_domains/system_domain/";
req_domain([H|T], HostUrl) ->
%    ?nebMsg("Entry"),
    {Re, Domain} = lists:nth(1, maps:to_list(H)),
    case re:run(HostUrl, binary_to_list(Re)) of
        {match, _} ->
            binary_to_list(Domain);
        _ ->
            req_domain(T, HostUrl)
    end.

resource_exists_handler("/cdmi_objectid/", Req, State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Oid = nebula2_utils:get_value(<<"objectName">>, EnvMap),
    case nebula2_db:read(Pid, Oid) of
        {error, _Status} ->
            {<<"false">>, Req, State};
        {ok, Data} ->
            nebula2_utils:set_cache(Data),
            {<<"true">>, Req, {Pid, nebula2_utils:put_value(<<"object_map">>, Data, EnvMap)}}
    end;
resource_exists_handler(_, Req, State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
%    ?nebFmt("EnvMap: ~p", [EnvMap]),
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    case Path of
        "/" ->
            {true, Req, State};
        _ ->
            SearchKey = nebula2_utils:make_search_key(EnvMap),
            case nebula2_db:search(SearchKey, State) of
                {ok, Data} ->
                    {<<"true">>, Req, {Pid, nebula2_utils:put_value(<<"object_map">>, Data, EnvMap)}};
                _ ->
                    {<<"false">>, Req, State}
            end
    end.

-spec to_cdmi_object_handler(cowboy_req:req(), cdmi_state(), string(), string()) -> {map(), term(), cdmi_state()} | {notfound, term(), cdmi_state()}.
to_cdmi_object_handler(Req, State, _, "/cdmi_objectid/") ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    Oid = nebula2_utils:get_value(<<"objectName">>, EnvMap),
    case nebula2_db:read(Pid, Oid) of
        {ok, Data} ->
            nebula2_utils:set_cache(Data),
            {jsx:encode(nebula2_db:unmarshall(Data)), Req, State};
        {error, Status} -> 
            {notfound, cowboy_req:reply(Status, Req, [{<<"content-type">>, <<"text/plain">>}]), State}
    end;
to_cdmi_object_handler(Req, State, _Path, "/cdmi_domains/") ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Key = nebula2_utils:make_search_key(EnvMap),
    case nebula2_db:search(Key, State) of
        {ok, Data} ->
            nebula2_utils:set_cache(Data),
            {jsx:encode(nebula2_db:unmarshall(Data)), Req, State};
        {error, Status} ->
            {notfound, cowboy_req:reply(Status, [{<<"content-type">>, <<"text/plain">>}], Req), State}
    end;
to_cdmi_object_handler(Req, State, _Path, "/cdmi_capabilities/") ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Key = nebula2_utils:make_search_key(EnvMap),
    case nebula2_db:search(Key, State) of
        {ok, Data} ->
            nebula2_utils:set_cache(Data),
            {jsx:encode(nebula2_db:unmarshall(Data)), Req, State};
        {error, Status} ->
            {notfound, cowboy_req:reply(Status, [{<<"content-type">>, <<"text/plain">>}, Req]), State}
    end;
to_cdmi_object_handler(Req, State, _, _) ->
%    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Key = nebula2_utils:make_search_key(EnvMap),
    case nebula2_db:search(Key, State) of
        {ok, Map} ->
            {_, EnvMap} = State,
            Qs = binary_to_list(nebula2_utils:get_value(<<"qs">>, EnvMap)),
            Map2 = handle_query_string(Map, Qs),
            CList = [<<"cdmi_atime">>,
                     <<"cdmi_acount">>],
            Map3 = nebula2_utils:update_data_system_metadata(CList, Map2, State),
            nebula2_utils:set_cache(Map3),
            {jsx:encode(nebula2_db:unmarshall(Map3)), Req, State};
        {error, Status} ->
            {notfound, cowboy_req:reply(Status, [{<<"content-type">>, <<"text/plain">>}], Req), State}
    end.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

cdmi_handler_test_() ->
    {foreach,
     fun() ->
            ok
     end,
     fun(_) ->
            ok
     end,
     [{"Test init/3",
       fun() ->
              ?assertMatch({upgrade, protocol, cowboy_rest}, init(term, req, opts))
       end
      }
     ]
    }.

-endif.
