%% @author mmartin
%% @doc Handle CDMI domain objects.

-module(nebula2_domains).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            delete_domain/2,
            new_domain/2,
            update_domain/3
        ]).

%% @doc Delete a CDMI domain
%% TODO: Enforce CDMI domain deletion rules - i.e. move populated domains to metadata: cdmi_domain_delete_reassign
%%       if the domain contains objects and then delete the domain, or fail with 400 if the domain contains objects
%%       and metadata: cdmi_domain_delete_reassign is missing or points to a non-existent domain.
-spec nebula2_domains:delete_domain(cowboy_req:req(), cdmi_state()) -> ok | {error, term()}.
delete_domain(Req, State) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
    EnvMap2 = nebula2_utils:put_value(<<"domainURI">>, Path, EnvMap),
    State2 = {Pid, EnvMap2},    %% Domain URI is the path for new domains.
    SearchKey = nebula2_utils:get_domain_hash(Path) ++ Path,
    {ok, Data} = nebula2_db:search(SearchKey, State2),
    lager:debug("Data: ~p", [Data]),
    Oid = nebula2_utils:get_value(<<"objectID">>, Data),
    lager:debug("Oid: ~p", [Oid]),
    Metadata = nebula2_utils:get_value(<<"metadata">>, Data),
    lager:debug("Metadata: ~p", [Metadata]),
    ReassignTo = nebula2_utils:get_value(<<"cdmi_domain_delete_reassign">>, Metadata, nil),
    lager:debug("ReassignTo: ~p", [ReassignTo]),
    case nebula2_utils:get_value(<<"cdmi_domain_delete_reassign">>, Metadata, nil) of
        nil ->
            case nebula2_utils:get_value(<<"children">>, Data, []) of
                [] ->
                    handle_delete(nebula2_db:delete(Pid, Oid), Req, State2);
                _ ->
                    {error, 400}
            end;
        ReassignTo ->
            %% TODO: Implement cdmi_domain_delete_reassign.
            {error, 501}
    end.

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_domain(Req, State) ->
    lager:debug("Entry"),
    {_Pid, EnvMap} = State,
    DomainName = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, EnvMap)) ++ binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)),
    lager:debug("Domain name: ~p", [DomainName]),
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    nebula2_db:marshall(NewBody)
            catch
                error:badarg ->
                    throw(badjson)
            end,
    lager:debug("Body2: ~p", [Body2]),
    Response = case nebula2_utils:create_object(Req2, State, ObjectType, DomainName, Body2) of
                   {true, Req3, Data} ->
                       Data2 = nebula2_db:unmarshall(Data),
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req3), State};
                   false ->
                       {false, Req2, State}
               end,
    Response.
    

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) ->
    %% lager:debug("Entry"),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
handle_delete(ok, Req, State) ->
    %% lager:debug("Entry"),
    {true, Req, State};
handle_delete({error, _Error}, Req, State) ->
    %% lager:debug("Entry"),
    {false, Req, State}.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
