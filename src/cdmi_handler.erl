-module(cdmi_handler).
-compile([{parse_transform, lager_transform}]).

-include("nebula.hrl").
  
-export([init/3]).
-export([content_types_provided/2]).
-export([get_html/2,
         forbidden/2,
         is_authorized/2,
         malformed_request/2,
         rest_init/2,
         service_available/2]).

init(_, _Req, _Opts) ->
    lager:debug("initing..."),
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, _State) ->
    PoolMember = pooler:take_member(riak_pool),
    lager:debug("PoolMember: ~p", [PoolMember]),
    {ok, Req, PoolMember}.

content_types_provided(Req, State) ->
    {[{{<<"text">>, <<"html">>, '*'}, get_html}], Req, State}.

forbidden(Req, State) ->
%% TODO: Check ACLs here
    {false, Req, State}.
%%    {true, Req, State}.
    
get_html(Req, State) ->
    lager:debug("get_json...~p", [State]),
    
    pooler:return_member(riak_pool, State),
    {<<"{\"jsondoc\": \"number1\"}">>, Req, State}.

is_authorized(Req, State) ->
%% TODO: Check credentials here
    {true, Req, State}.
%%    {{false, "You suck!!!"}, Req, State}.

%% Malformed request.
%% There must be an X-CDMI-Specification-Version header, and it
%% must request version 1.1
malformed_request(Req, State) ->
    lager:debug("In malformed_request..."),
    CDMIVersion = cowboy_req:header(<<?VERSION_HEADER>>, Req, error),
    lager:debug("Version header value: ~p", [CDMIVersion]),
    Valid = case CDMIVersion of
        {error, _} -> 
            lager:debug("Malformed request - no cdmi version header"),
                true;
        {BinaryVersion, _} ->
                Version = binary_to_list(BinaryVersion),
                lager:debug("Found an X-CDMI-Specification header: ~p", [Version]),
                L = re:replace(Version, "\\s+", "", [global,{return,list}]),
                lager:debug("L is ~p", [L]),
                CDMIVersions = string:tokens(L, ","),
                lager:debug("Acceptable Versions: ~p", [CDMIVersions]),
                not lists:member(?CDMI_VERSION, CDMIVersions)
    end,
    lager:debug("Malformed request: ~p", [Valid]),
    {Valid, Req, State}.
                                          
%% if pooler says no members, kick back a 503. I
%% do this here because a 503 seems to me the most
%% appropriate response if database connections are
%% <b>currently</b> unavailable.
service_available(Req, error_no_members) ->
    {false, Req, undefined};
service_available(Req, Conn) ->
    Available = case nebula2_riak:ping(Conn) of
        true ->
             lager:debug("Service available..."),
             true;
        _ -> lager:debug("Service unavailable..."),
             false
    end,
    {Available, Req, Conn}.