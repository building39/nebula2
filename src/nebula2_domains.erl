%% @author mmartin
%% @doc Handle CDMI domain objects.

-module(nebula2_domains).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
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
delete_domain(State) when is_tuple(State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
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
                            ?nebErrFmt("Cannot delete non-empty domain ~p", [Path]),
                            {error, 400}
                    end;
                ReassignTo ->
                    %% TODO: Implement cdmi_domain_delete_reassign.
                    {error, 501}
            end;
        true ->
            ?nebErrMsg("Cannot delete system domain!"),
            throw(forbidden)
    end.

%% @doc Create a new CDMI domain
-spec nebula2_domains:new_domain(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_domain(Req, State) when is_tuple(State) ->
%    ?nebMsg("Entry"),
    {Pid, EnvMap} = State,
    DomainName = maps:get(<<"path">>, EnvMap),  %% A domain always belongs to itself.
    SearchKey = nebula2_utils:get_domain_hash(DomainName) ++ binary_to_list(DomainName),
    EM2 = maps:put(<<"domainURI">>, DomainName, EnvMap),
    EM3 = maps:put(<<"searchKey">>, SearchKey, EM2),
    State2 = {Pid, EM3},
    ObjectType = ?CONTENT_TYPE_CDMI_DOMAIN,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                     NewBody
            catch
                error:badarg ->
                    throw(badjson)
            end,
    case nebula2_utils:create_object(State, ObjectType, DomainName, Body2) of
        {true, Data} ->
            Containers = [<<"cdmi_domain_members/">>,
                          <<"cdmi_domain_summary/">>
                         ],
            case new_domain_root(State, DomainName) of
                ok ->
                    case new_member_and_summary_containers(State, SearchKey, Containers) of
                        true ->
                            Data2 = nebula2_db:unmarshall(Data),
                            {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req2), State2};
                        false ->
                            {false, Req2, State2}
                    end;
                _ ->
                    {false, Req2, State2}
            end;
        false ->
            {false, Req2, State2}
    end.

%% @doc Update a CDMI domain
-spec nebula2_domains:update_domain(pid(), object_oid(), map()) -> {ok, json_value()}.
update_domain(_Pid, _ObjectId, Data) when is_pid(_Pid), is_binary(_ObjectId), is_map(Data) ->
    %% ?nebMsg("Entry"),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================

-spec new_member_and_summary_containers(cdmi_state(), binary(), list()) -> boolean().
new_member_and_summary_containers(State, SearchKey, Containers) when is_tuple(State), is_list(SearchKey), is_list(Containers) ->
%    ?nebMsg("Entry"),
    {ok, Parent} = nebula2_db:search(SearchKey, State),
    case new_containers(State, Parent, Containers, {}) of
        true ->
            Containers2 = [<<"daily/">>, <<"weekly/">>, <<"monthly/">>, <<"yearly/">>],
            SearchKey2 = SearchKey ++ "cdmi_domain_summary/",
            {ok, Parent2} = nebula2_db:search(SearchKey2, State),
            new_containers(State, Parent2, Containers2, {});
        false ->
            false
    end.

-spec new_containers(cdmi_state(), map(), list(), boolean()) -> boolean().
new_containers(_State, _Parent, [], Response) ->
    Response;
new_containers(State, Parent, [ObjectName|T], _Response) ->
%    ?nebMsg("Entry"),
    PUri = binary_to_list(nebula2_utils:get_value(<<"parentURI">>, Parent)),
    ParentUri = PUri ++ binary_to_list(nebula2_utils:get_value(<<"objectName">>, Parent)),
    Oid = nebula2_utils:make_key(),
    ParentId  = nebula2_utils:get_value(<<"objectID">>, Parent),
    Data = maps:from_list([
                          {<<"objectName">>, ObjectName},
                          {<<"objectType">>, ?CONTENT_TYPE_CDMI_CONTAINER},
                          {<<"objectID">>, Oid},
                          {<<"parentID">>, ParentId},
                          {<<"parentURI">>, list_to_binary(ParentUri)},
                          {<<"capabilitiesURI">>, ?CONTAINER_CAPABILITY_URI},
                          {<<"domainURI">>, nebula2_utils:get_value(<<"domainURI">>, Parent)},
                          {<<"completionStatus">>, <<"Complete">>},
                          {<<"metadata">>, nebula2_utils:get_value(<<"metadata">>, Parent)}
                         ]),
    Data2 = nebula2_db:marshall(Data),
    {Pid, _} = State,
    case nebula2_db:create(Pid, nebula2_utils:get_value(<<"objectID">>, Data2), Data2) of
        {ok, _} ->
            {ok, _} = nebula2_utils:update_parent(nebula2_utils:get_value(<<"objectID">>, Parent),
                                                  ParentUri ++ binary_to_list(ObjectName),
                                                  ?CONTENT_TYPE_CDMI_CONTAINER,
                                                  Pid),
            new_containers(State, Parent, T, true);
        _Other ->
            ?nebErrFmt("new container create failed: ~p", [_Other]),
            new_containers(State, Parent, [], false)
    end.

-spec new_domain_root(cdmi_state(), binary()) -> ok | {error, string()}.
new_domain_root(State, DomainUri) ->
    {Pid, EnvMap} = State,
    {ok, SystemRoot} = nebula2_db:read(Pid, nebula2_utils:get_value(<<"root_oid">>, EnvMap)),
    Metadata = nebula2_utils:get_value(<<"metadata">>, SystemRoot),
    Oid = nebula2_utils:make_key(),
    Map = nebula2_db:marshall(maps:from_list([
                                                {<<"metadata">>, Metadata},
                                                {<<"objectName">>, <<"/">>},
                                                {<<"objectType">>, ?CONTENT_TYPE_CDMI_CONTAINER},
                                                {<<"objectID">>, Oid},
                                                {<<"capabilitiesURI">>, ?CONTAINER_CAPABILITY_URI},
                                                {<<"completionStatus">>, <<"Complete">>},
                                                {<<"domainURI">>, DomainUri}
                                              ])),
    case nebula2_db:create(Pid, Oid, Map) of
        {ok, _} ->
            ok;
        Reason ->
            ?nebErrFmt("Creation of root container for the new domain ~p failed: ~p", [DomainUri, Reason]),
            Reason
    end.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
nebula2_domains_test_() ->
    {foreach,
     fun() ->
             meck:new(cowboy_req, [non_strict]),
             meck:new(nebula2_db, [passthrough]),
             meck:new(nebula2_utils, [passthrough])
     end,
     fun(_) ->
             meck:unload(cowboy_req),
             meck:unload(nebula2_db),
             meck:unload(nebula2_utils)
     end,
     [
        {"delete_system_domain/1",
            fun() ->
                Domain = jsx:decode(?TestSystemDomain, [return_maps]),
                Domain2 = maps:put(<<"children">>, [<<"test_members">>], Domain),
                EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/system_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>},
                                         {<<"objectName">>, <<"system_domain/">>}
                                        ]),
                Pid = self(),
                State = {Pid, EnvMap},
                meck:sequence(nebula2_db, search, 2, [{ok, Domain},
                                                      {ok, Domain},
                                                      {ok, Domain2}]),
                meck:loop(nebula2_utils, delete_child_from_parent, 3, [ok]),
                meck:loop(nebula2_utils, delete, 1, [ok]),
                ?assertException(throw, forbidden, delete_domain(State)),
                EnvMap2 = maps:put(<<"objectName">>, <<"test_domain/">>, EnvMap),
                State2 = {Pid, EnvMap2},
                ?assertMatch(ok, delete_domain(State2)),
                ?assertException(error, function_clause, delete_domain(not_a_tuple)),
                ?assert(meck:validate(nebula2_db)),
                ?assert(meck:validate(nebula2_utils))
            end
        },
        {"delete_domain_no_children/1",
            fun() ->
                Domain = jsx:decode(?TestSystemDomain, [return_maps]),
                Domain2 = maps:remove(<<"children">>, Domain),
                EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
                Pid = self(),
                State = {Pid, EnvMap},
                meck:sequence(nebula2_db, search, 2, [{ok, Domain2}]),
                meck:loop(nebula2_utils, delete_child_from_parent, 3, [ok]),
                meck:loop(nebula2_utils, delete, 1, [ok]),
                ?assertMatch(ok, delete_domain(State)),
                ?assert(meck:validate(nebula2_db)),
                ?assert(meck:validate(nebula2_utils))
            end
        },
        {"delete_domain_with_children/1",
            fun() ->
                Domain = jsx:decode(?TestSystemDomain, [return_maps]),
                Domain2 = maps:put(<<"children">>, [<<"test_members">>], Domain),
                EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
                Pid = self(),
                State = {Pid, EnvMap},
                meck:sequence(nebula2_db, search, 2, [{ok, Domain2}]),
                meck:loop(nebula2_utils, delete_child_from_parent, 3, [ok]),
                meck:loop(nebula2_utils, delete, 1, [ok]),
                ?assertMatch({error, 400}, delete_domain(State)),
                ?assert(meck:validate(nebula2_db)),
                ?assert(meck:validate(nebula2_utils))
            end
        },
        {"delete_domain_with_reassign/1",
            fun() ->
                Domain = jsx:decode(?TestSystemDomain, [return_maps]),
                Domain2 = maps:put(<<"children">>, [<<"test_members">>], Domain),
                Metadata = maps:get(<<"metadata">>, Domain2),
                Domain3 = maps:put(<<"metadata">>, maps:put(<<"cdmi_domain_delete_reassign">>, <<"true">>, Metadata), Domain2),
                EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/system_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
                Pid = self(),
                State = {Pid, EnvMap},
                meck:sequence(nebula2_db, search, 2, [{ok, Domain3}]),
                meck:loop(nebula2_utils, delete_child_from_parent, 3, [ok]),
                meck:loop(nebula2_utils, delete, 1, [ok]),
                ?assertMatch({error, 501}, delete_domain(State)),
                ?assert(meck:validate(nebula2_db)),
                ?assert(meck:validate(nebula2_utils))
            end
        },
        {"delete_domain_contract/1",
            fun() ->
                ?assertException(error, function_clause, delete_domain(not_a_tuple))
            end
        },
        {"new_domain/2",
            fun() ->
               Body = <<"{\"metadata\": {\"cdmi_domain_enabled\": \"true\"}}">>,
               DomainHash = "domain_hash",
               Pid = self(),
               EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
               State = {Pid, EnvMap},
               ReturnedEnvMap = maps:put(<<"searchKey">>,
                                         DomainHash ++ binary_to_list(maps:get(<<"path">>, EnvMap)),
                                         EnvMap),
               ReturnedState = {Pid, ReturnedEnvMap},
               Req = "",
               TestMap = jsx:decode(?TestSystemDomain, [return_maps]),
               TestDomain = maps:from_list([{<<"cdmi">>, TestMap},
                                            {<<"sp">>, ?TestCreateContainerSearchPath}
                                           ]),
               TestRoot = jsx:decode(?TestRootObject, [return_maps]),
               meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
               meck:loop(cowboy_req, set_resp_body, 2, [Req]),
               meck:loop(nebula2_db, create, 3, [{ok, TestDomain}]),
               meck:loop(nebula2_db, read, 2, [{ok, TestRoot}]),
               meck:loop(nebula2_db, search, 2, [{ok, TestDomain}]),
               meck:loop(nebula2_utils, create_object, 4, [{true, TestDomain}]),
               meck:loop(nebula2_utils, get_domain_hash, 1, [DomainHash]),
               meck:loop(nebula2_utils, update_parent, 4, [{ok, TestDomain}]),
               ?assertMatch({true, Req, ReturnedState}, new_domain(Req, State)),
               ?assert(meck:validate(cowboy_req)),
               ?assert(meck:validate(nebula2_db)),
               ?assert(meck:validate(nebula2_utils))
            end
         },
         {"new_domain_domain_root_create_fail/2",
            fun() ->
               Body = <<"{\"metadata\": {\"cdmi_domain_enabled\": \"true\"}}">>,
               DomainHash = "domain_hash",
               Pid = self(),
               EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
               State = {Pid, EnvMap},
               ReturnedEnvMap = maps:put(<<"searchKey">>,
                                         DomainHash ++ binary_to_list(maps:get(<<"path">>, EnvMap)),
                                         EnvMap),
               ReturnedState = {Pid, ReturnedEnvMap},
               Req = "",
               TestMap = jsx:decode(?TestSystemDomain, [return_maps]),
               TestDomain = maps:from_list([{<<"cdmi">>, TestMap},
                                            {<<"sp">>, ?TestCreateContainerSearchPath}
                                           ]),
               TestRoot = jsx:decode(?TestRootObject, [return_maps]),
               meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
               meck:loop(cowboy_req, set_resp_body, 2, [Req]),
               meck:loop(nebula2_db, create, 3, [{fail, enoent}]),
               meck:loop(nebula2_db, read, 2, [{ok, TestRoot}]),
               meck:loop(nebula2_db, search, 2, [{ok, TestDomain}]),
               meck:loop(nebula2_utils, create_object, 4, [{true, TestDomain}]),
               meck:loop(nebula2_utils, get_domain_hash, 1, [DomainHash]),
               meck:loop(nebula2_utils, update_parent, 4, [{ok, TestDomain}]),
               ?assertMatch({false, Req, ReturnedState}, new_domain(Req, State)),
               ?assert(meck:validate(cowboy_req)),
               ?assert(meck:validate(nebula2_db)),
               ?assert(meck:validate(nebula2_utils))
            end
         },
         {"new_domain_create_fail/2",
            fun() ->
               Body = <<"{\"metadata\": {\"cdmi_domain_enabled\": \"true\"}}">>,
               DomainHash = "domain_hash",
               Pid = self(),
               EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
               State = {Pid, EnvMap},
               ReturnedEnvMap = maps:put(<<"searchKey">>,
                                         DomainHash ++ binary_to_list(maps:get(<<"path">>, EnvMap)),
                                         EnvMap),
               ReturnedState = {Pid, ReturnedEnvMap},
               Req = "",
               TestMap = jsx:decode(?TestSystemDomain, [return_maps]),
               TestDomain = maps:from_list([{<<"cdmi">>, TestMap},
                                            {<<"sp">>, ?TestCreateContainerSearchPath}
                                           ]),
               TestRoot = jsx:decode(?TestRootObject, [return_maps]),
               meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
               meck:loop(cowboy_req, set_resp_body, 2, [Req]),
               meck:loop(nebula2_db, create, 3, [{fail, enoent}]),
               meck:loop(nebula2_db, read, 2, [{ok, TestRoot}]),
               meck:loop(nebula2_db, search, 2, [{ok, TestDomain}]),
               meck:loop(nebula2_utils, create_object, 4, [false]),
               meck:loop(nebula2_utils, get_domain_hash, 1, [DomainHash]),
               meck:loop(nebula2_utils, update_parent, 4, [{ok, TestDomain}]),
               ?assertMatch({false, Req, ReturnedState}, new_domain(Req, State)),
               ?assert(meck:validate(cowboy_req)),
               ?assert(meck:validate(nebula2_db)),
               ?assert(meck:validate(nebula2_utils))
            end
         },
         {"new_domain_child_create_fail/2",
            fun() ->
               Body = <<"{\"metadata\": {\"cdmi_domain_enabled\": \"true\"}}">>,
               DomainHash = "domain_hash",
               Pid = self(),
               EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
               State = {Pid, EnvMap},
               ReturnedEnvMap = maps:put(<<"searchKey">>,
                                         DomainHash ++ binary_to_list(maps:get(<<"path">>, EnvMap)),
                                         EnvMap),
               ReturnedState = {Pid, ReturnedEnvMap},
               Req = "",
               TestMap = jsx:decode(?TestSystemDomain, [return_maps]),
               TestDomain = maps:from_list([{<<"cdmi">>, TestMap},
                                            {<<"sp">>, ?TestCreateContainerSearchPath}
                                           ]),
               TestRoot = jsx:decode(?TestRootObject, [return_maps]),
               meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
               meck:loop(cowboy_req, set_resp_body, 2, [Req]),
               meck:loop(nebula2_db, create, 3, [{ok, TestDomain}, {fail, enoent}]),
               meck:loop(nebula2_db, read, 2, [{ok, TestRoot}]),
               meck:loop(nebula2_db, search, 2, [{ok, TestDomain}]),
               meck:loop(nebula2_utils, create_object, 4, [{true, TestDomain}]),
               meck:loop(nebula2_utils, get_domain_hash, 1, [DomainHash]),
               meck:loop(nebula2_utils, update_parent, 4, [{ok, TestDomain}]),
               ?assertMatch({false, Req, ReturnedState}, new_domain(Req, State)),
               ?assert(meck:validate(cowboy_req)),
               ?assert(meck:validate(nebula2_db)),
               ?assert(meck:validate(nebula2_utils))
            end
         },
         {"new_domain_badjson/2",
            fun() ->
               Body = "junk",
               Pid = self(),
               Req = "",
               EnvMap = maps:from_list([{<<"path">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"auth_as">>, <<"MickeyMouse">>},
                                         {<<"domainURI">>, <<"/cdmi_domains/test_domain/">>},
                                         {<<"objectName">>, <<"test_domain/">>}
                                        ]),
               State = {Pid, EnvMap},
               meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
               ?assertException(throw, badjson, new_domain(Req, State)),
               ?assert(meck:validate(cowboy_req))
            end
         },
         {"new_domain_contract/2",
            fun() ->
               ?assertException(error, function_clause, new_domain("Req", not_a_tuple))
            end
        },
        {"update_domain/2",
            fun() ->
               Pid = self(),
               TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
               Oid = maps:get(<<"objectID">>, TestMap),
               TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
               ?assertMatch({ok, TestMapCDMI}, update_domain(Pid, Oid, TestMapCDMI)),
               ?assertException(error, function_clause, update_domain(not_a_pid, Oid, TestMapCDMI)),
               ?assertException(error, function_clause, update_domain(Pid, not_a_binary, TestMapCDMI)),
               ?assertException(error, function_clause, update_domain(Pid, Oid, not_a_map))
            end
        },
        {"update_domain_contract/2",
            fun() ->
               Pid = self(),
               TestMap = jsx:decode(?TestSystemConfiguration, [return_maps]),
               Oid = maps:get(<<"objectID">>, TestMap),
               TestMapCDMI = maps:from_list([{<<"cdmi">>, TestMap},
                                              {<<"sp">>, ?TestCreateContainerSearchPath}
                                             ]),
               ?assertException(error, function_clause, update_domain(not_a_pid, Oid, TestMapCDMI)),
               ?assertException(error, function_clause, update_domain(Pid, not_a_binary, TestMapCDMI)),
               ?assertException(error, function_clause, update_domain(Pid, Oid, not_a_map))
            end
        }
     ]
  }.
-endif.
