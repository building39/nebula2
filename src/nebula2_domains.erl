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
            delete_domain/1,
            new_domain/2,
            update_domain/3
        ]).

%% @doc Delete a CDMI domain
%% TODO: Enforce CDMI domain deletion rules - i.e. move populated domains to metadata: cdmi_domain_delete_reassign
%%       if the domain contains objects and then delete the domain, or fail with 400 if the domain contains objects
%%       and metadata: cdmi_domain_delete_reassign is missing or points to a non-existent domain.
-spec nebula2_domains:delete_domain(cdmi_state()) -> ok | {error, term()}.
delete_domain(State) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    lager:debug("EnvMap: ~p", [EnvMap]),
    case binary_to_list(nebula2_utils:get_value(<<"objectName">>, EnvMap)) == "system_domain/" of
        false ->
            Path = binary_to_list(nebula2_utils:get_value(<<"path">>, EnvMap)),
            EnvMap2 = nebula2_utils:put_value(<<"domainURI">>, Path, EnvMap),
            State2 = {Pid, EnvMap2},    %% Domain URI is the path for new domains.
            SearchKey = nebula2_utils:get_domain_hash(Path) ++ Path,
            {ok, Data} = nebula2_db:search(SearchKey, State2),
            Metadata = nebula2_utils:get_value(<<"metadata">>, Data),
            ReassignTo = nebula2_utils:get_value(<<"cdmi_domain_delete_reassign">>, Metadata, nil),
            case nebula2_utils:get_value(<<"cdmi_domain_delete_reassign">>, Metadata, nil) of
                nil ->
                    %% TODO: check for existence of child domains and domain members before deleting.
                    SChildren = lists:filtermap(fun(X) -> {true, binary_to_list(X)} end,
                                                nebula2_utils:get_value(<<"children">>, Data, [])),
                    Children = lists:filter(fun(X) -> true /= nebula2_utils:beginswith(X, "cdmi_domain_") end, SChildren),
                    case Children of
                        [] ->
                            ok = nebula2_utils:delete_child_from_parent(Pid,
                                                                        nebula2_utils:get_value(<<"parentID">>, Data),
                                                                        nebula2_utils:get_value(<<"objectName">>, Data)),
                            nebula2_utils:delete(State2);
                        _ ->
                            {error, 400}
                    end;
                ReassignTo ->
                    %% TODO: Implement cdmi_domain_delete_reassign.
                    {error, 501}
            end;
        true ->
            lager:error("Cannot delete system domain!"),
            throw(forbidden)
    end.

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_domain(Req, State) ->
    lager:debug("Entry"),
    {Pid, EnvMap} = State,
    lager:debug("EnvMap: ~p", [EnvMap]),
    DomainName = maps:get(<<"path">>, EnvMap),  %% A domain always belongs to itself.
    SearchKey = nebula2_utils:get_domain_hash(DomainName) ++ binary_to_list(DomainName),
    EM2 = maps:put(<<"domainURI">>, DomainName, EnvMap),
    EM3 = maps:put(<<"searchKey">>, SearchKey, EM2),
    State2 = {Pid, EM3},
    lager:debug("Domain name: ~p", [DomainName]),
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    nebula2_db:marshall(NewBody, SearchKey)
            catch
                error:badarg ->
                    throw(badjson)
            end,
    lager:debug("Body2: ~p", [Body2]),
    case nebula2_utils:create_object(State, ObjectType, DomainName, Body2) of
        {true, Data} ->
            Containers = [<<"cdmi_domain_members/">>, <<"cdmi_domain_summary/">>],
            case new_member_and_summary_containers(State, SearchKey, Containers) of
                true ->
                    Data2 = nebula2_db:unmarshall(Data),
                    {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req2), State2};
                false ->
                    {false, Req2, State2}
            end;
        false ->
            {false, Req2, State2}
    end.

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) ->
    %% lager:debug("Entry"),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec new_member_and_summary_containers(cdmi_state(), string(), list()) -> boolean().
new_member_and_summary_containers(State, SearchKey, Containers) ->
    lager:debug("Entry"),
    lager:debug("Search Key: ~p", [SearchKey]),
    {ok, Parent} = nebula2_db:search(SearchKey, State),
    case new_containers(State, SearchKey, Parent, Containers, {}) of
        true ->
            Containers2 = [<<"daily/">>, <<"weekly/">>, <<"monthly/">>, <<"yearly/">>],
            SearchKey2 = SearchKey ++ "cdmi_domain_summary/",
            {ok, Parent2} = nebula2_db:search(SearchKey2, State),
            new_containers(State, SearchKey, Parent2, Containers2, {});
        false ->
            false
    end.

-spec new_containers(cdmi_state(), binary(), map(), list(), boolean()) -> boolean().
new_containers(_State, _SearchKey, _Parent, [], Response) ->
    Response;
new_containers(State, SearchKey, Parent, [ObjectName|T], _Response) ->
    lager:debug("Entry"),
    PUri = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, Parent)),
    ParentUri = PUri ++ binary_to_list(nebula2_utils:get_value(<<"objectName">>, Parent)),
    Oid = nebula2_utils:make_key(),
    ParentId  = nebula2_utils:get_value(<<"objectID">>, Parent),
    Data = maps:from_list([
                          {<<"objectName">>, ObjectName},
                          {<<"objectType">>, <<?CONTENT_TYPE_CDMI_CONTAINER>>},
                          {<<"objectID">>, Oid},
                          {<<"parentID">>, ParentId},
                          {<<"parentURI">>, list_to_binary(ParentUri)},
                          {<<"capabilitiesURI">>, <<?CONTAINER_CAPABILITY_URI>>},
                          {<<"domainURI">>, nebula2_utils:get_value(<<"domainURI">>, Parent)},
                          {<<"completionStatus">>, <<"Complete">>},
                          {<<"metadata">>, nebula2_utils:get_value(<<"metadata">>, Parent)}
                         ]),
    lager:debug("Marshalling..."),
    lager:debug("Data: ~p", [Data]),
    Data2 = nebula2_db:marshall(Data),
    lager:debug("marshalling done."),
    {Pid, _} = State,
    case nebula2_db:create(Pid, nebula2_utils:get_value(<<"objectID">>, Data2), Data2) of
        {ok, _} ->
            lager:debug("new container created"),
            lager:debug("Updating parent: ~p", [ParentUri]),
            ok = nebula2_utils:update_parent(nebula2_utils:get_value(<<"objectID">>, Parent),
                                             ParentUri ++ binary_to_list(ObjectName),
                                             ?CONTENT_TYPE_CDMI_CONTAINER,
                                             Pid),
            new_containers(State, SearchKey, Parent, T, true);
        _Other ->
            lager:debug("new container create failed: ~p", [_Other]),
            new_containers(State, SearchKey, Parent, [], false)
    end.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
