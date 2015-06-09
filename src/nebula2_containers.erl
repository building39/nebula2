%% @author mmartin
%% @doc Handle CDMI container objects.

-module(nebula2_containers).
-compile([{parse_transform, lager_transform}]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            get_container/2,
            new_container/2,
            update_container/3
        ]).

%% @doc Get a CDMI container
-spec nebula2_containers:get_container(pid(), object_oid()) -> {ok, json_value()}.
get_container(Pid, Oid) ->
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    maps:from_list(jsx:decode(list_to_binary(Data))).

%% @doc Create a new CDMI container
-spec nebula2_containers:new_container(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_container(Req, State) ->
    Oid = nebula2_utils:make_key(),
    lager:debug("make_key returned ~p", [Oid]),
    Tstamp = list_to_binary(nebula2_utils:get_time()),
    Location = list_to_binary(nebula2_app:cdmi_location()),
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("Path is ~p" , [Path]),
    ObjectName = case Path of
                    [] ->
                        "/";
                    U  -> 
                        "/" ++ nebula2_utils:build_path(U)
                 end,
    lager:debug("ObjectName is ~p", [ObjectName]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    Data = maps:from_list(jsx:decode(Body)),
    Metadata = [{<<"cdmi_atime">>, Tstamp},
                {<<"cdmi_ctime">>, Tstamp},
                {<<"cdmi_mtime">>, Tstamp},
                {<<"cdmi_versions_count_provided">>, <<"0">>},
                {<<"nebula_data_location">>, [Location]},
                {<<"nebula_modified_by">>, <<"">>}
               ],
    Metadata2 = case maps:find(<<"metadata">>, Data) of
                   {ok, MD} ->
                       lager:debug("found metadata ~p", [MD]),
                       lists:append(Metadata, MD);
                   _ -> 
                       lager:debug("did not find metadata"),
                       Metadata
               end,
    CapabilitiesURI = nebula2_utils:get_capabilities_uri(Pid, ObjectName),
    DomainURI = nebula2_utils:get_domain_uri(Pid, ObjectName),
    ObjectType = "application/cdmi-container",
    case nebula2_utils:get_parent(Pid, ObjectName) of
        {ok, ParentUri, ParentId} ->
            lager:debug("Creating new container. ParentUri: ~p ParentId: ~p", [ParentUri, ParentId]),
            lager:debug("                        Container Name: ~p", [ObjectName]),
            lager:debug("                        OID: ~p", Oid),
            Data2 = [{<<"objectType">>, list_to_binary(ObjectType)},
                     {<<"objectID">>, list_to_binary(Oid)},
                     {<<"objectName">>, list_to_binary(ObjectName)},
                     {<<"parentID">>, list_to_binary(ParentId)},
                     {<<"parentURI">>, list_to_binary(ParentUri)},
                     {<<"metadata">>, Metadata2},
                     {<<"capabilitiesURI">>, list_to_binary(CapabilitiesURI)},
                     {<<"domainURI">>, list_to_binary(DomainURI)},
                     {<<"completionStatus">>, <<"Complete">>}],
            {ok, Oid} = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
            ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            {true, Req2, State};
        {error, notfound, _} ->
            lager:debug("new_container: Did not find parent"),
            pooler:return_member(riak_pool, Pid),
            {false, Req2, State}
    end.

%% @doc Update a CDMI container
-spec nebula2_containers:update_container(pid(), object_oid(), map()) -> {boolean(), json_value()}.
update_container(Pid, ObjectId, Data) ->
    lager:debug("nebula2_containers:update_container: Pid: ~p ObjectId: ~p Data: ~p", [Pid, ObjectId, Data]),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.