%% @author mmartin
%% @doc Handle CDMI container objects.


-module(nebula2_containers).

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            new_container/2
        ]).

%% @doc Create a new CDMI container
-spec nebula2_containers:new_container(Req, State) -> {boolean(), Req, State}.
new_container(Req, State) ->
    Oid = nebula2_utils:make_key(),
    Tstamp = list_to_binary(nebula2_utils:get_time()),
    Location = list_to_binary(nebula2_app:cdmi_location()),
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path_info(Req),
    ObjectName = case Path of
                    [] -> "cdmi/";
                    U  -> "cdmi/" ++ U
                 end,
    {ParentUri, ParentId} = case ObjectName of
                                  "cdmi/" -> {"", ""};    %% root has no parent
                                  _Other  -> nebula2_utils:get_parent(Pid, ObjectName)
                            end,
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
                   _        -> 
                       lager:debug("did not find metadata"),
                       Metadata
               end,
    CapabilitiesURI = nebula2_utils:get_capabilities_uri(Pid, ObjectName),
    DomainURI = nebula2_utils:get_domain_uri(Pid, ObjectName),
    Data2 = [{<<"objectType">>, <<"application/cdmi-container">>},
                                {<<"objectID">>, list_to_binary(Oid)},
                                {<<"objectName">>, list_to_binary(ObjectName)},
                                {<<"parentID">>, list_to_binary(ParentId)},
                                {<<"parentURI">>, list_to_binary(ParentUri)},
                                {<<"metadata">>, Metadata2},
                                {<<"capabilitiesURI">>, list_to_binary(CapabilitiesURI)},
                                {<<"domainURI">>, list_to_binary(DomainURI)},
                                {<<"children">>, <<"">>},
                                {<<"childrenrange">>, <<"">>},
                                {<<"completionStatus">>, <<"Complete">>},
                                {<<"exports">>, <<"">>},
                                {<<"percentComplete">>, <<"">>},
                                {<<"snapshots">>, <<"">>}],
    Response = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
    pooler:return_member(riak_pool, Pid),
    {true, Req2, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================


