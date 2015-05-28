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
    lager:debug("new_container URI: ~p", [ObjectName]),
    {ParentUri, ParentId} = case ObjectName of
                                  "cdmi/" -> {"", ""};    %% root has no parent
                                  _Other  -> nebula2_utils:get_parent(Pid, ObjectName)
                            end,
    lager:debug("Parent: ~p ParentID: ~p", [ParentUri, ParentId]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    lager:debug("Body: ~p", [Body]),
    Data = maps:from_list(jsx:decode(Body)),
    lager:debug("Data: ~p", [Data]),
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
    lager:debug("MetaData2: ~p", [Metadata2]),
    CapabilitiesURI = nebula2_utils:get_capabilities_uri(Pid, ObjectName),
    DomainURI = nebula2_utils:get_domain_uri(Pid, ObjectName),
    lager:debug("CapabilitiesURI: ~p", [CapabilitiesURI]),
    lager:debug("DomainURI: ~p", [DomainURI]),
    lager:debug("Oid: ~p", [Oid]),
    lager:debug("ObjectName: ~p", [ObjectName]),
    lager:debug("ParentUri: ~p", [ParentUri]),
    lager:debug("ParentId: ~p", [ParentId]),
    lager:debug("Metadata2: ~p", [Metadata2]),
%%     {ok, Data2} = cdmi_containers_dtl:render([
%%                                {objectType, "application/cdmi-container"},
%%                                {objectID, Oid},
%%                                {objectName, ObjectName},
%%                                {parentID, ParentId},
%%                                {parentURI, ParentUri},
%%                                {metadata, Metadata2},
%%                                {capabilitiesURI, CapabilitiesURI},
%%                                {domainURI, DomainURI},
%%                                {children, ""},
%%                                {childrenrange, ""},
%%                                {completionStatus, "Complete"},
%%                                {exports, ""},
%%                                {percentComplete, ""},
%%                                {snapshots, ""}]),
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
    lager:debug("Data2: ~p", [Data2]),
%    Data3 = iolist_to_binary(Data2),
%    lager:debug("Data3: ~p", [Data3]),
%    Data4 = binary_to_list(Data2),
%    lager:debug("Data4: ~p", [Data4]),
%    Data5 = re:replace(Data4, "\\n", "", [global, {return, list}]),
%    Data6 = list_to_binary(re:replace(Data5, "\\\"", "\"", [global, {return, list}])),
%    Data6 = lists:flatten(Data4),
    Response = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
    lager:debug("nebula2_containers:new_container put returned ~p", [Response]),
    pooler:return_member(riak_pool, Pid),
    {true, Req2, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================


