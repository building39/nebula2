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
    Tstamp = list_to_binary(nebula2_utils:get_time()),
    Location = list_to_binary(nebula2_app:cdmi_location()),
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("Path is ~p" , [Path]),
    ObjectName = case Path of
                    [] ->
                        "cdmi/";
                    [U]  -> 
                        "cdmi/" ++ binary_to_list(U) ++ "/"
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
            lager:debug("ParentUri: ~p~n ParentId: ~p", [ParentUri, ParentId]),
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
            ok = update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            {true, Req2, State};
        {error, notfound, _} ->
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
    
update_parent("", _, _, _) ->
    %% Must be the root, since there is no parent.
    ok;
update_parent(ParentId, ObjectName, ObjectType, Pid) ->
    lager:debug("update_parent: ~p ~p ~p ~p", [ParentId, ObjectName, ObjectType, Pid]),
    N = lists:last(string:tokens(ObjectName, "/")),
    Name = case ObjectType of
               "application/cdmi-container" -> N ++ "/";
               _ -> 
                   N
           end,
    Parent = nebula2_containers:get_container(Pid, ParentId),
    lager:debug("update_parent got parent: ~p", [Parent]),
    lager:debug("updating parent with child: ~p", [Name]),
    Children = case maps:get(<<"children">>, Parent, "") of
                     "" ->
                         [list_to_binary(Name)];
                     [Ch] ->
                         lists:append([Ch], [list_to_binary(Name)])
                 end,
    lager:debug("Children is now: ~p", [Children]),
    ChildRange = case maps:get(<<"childrange">>, Parent, "") of
                     "" ->
                         "0-0";
                     Cr ->
                         {Num, []} = string:to_integer(lists:last(string:tokens(binary_to_list(Cr), "-"))),
                         lists:concat(["0-", Num + 1])
                 end,
    lager:debug("ChildRange is now: ~p", [ChildRange]),
    NewParent1 = maps:put(<<"children">>, Children, Parent),
    NewParent2 = maps:put(<<"childrange">>, list_to_binary(ChildRange), NewParent1),
    O = maps:to_list(NewParent2),
    lager:debug("NewParent2: ~p", [NewParent2]),
    lager:debug("To Encode: ~p", [O]),
    lager:debug("new parent: ~p", [NewParent2]),
    {ok, _Oid} = nebula2_riak:update(Pid, ParentId, NewParent2),
    ok.

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.