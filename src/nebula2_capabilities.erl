%% @author mmartin
%% @doc Handle CDMI capability objects.

-module(nebula2_capabilities).
-compile([{parse_transform, lager_transform}]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            get_capability/2,
            new_capability/2,
            update_capability/3
        ]).

%% @doc Get a CDMI capability
-spec nebula2_capabilities:get_capability(pid(), object_oid()) -> {ok, json_value()}.
get_capability(Pid, Oid) ->
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    maps:from_list(jsx:decode(list_to_binary(Data))).

%% @doc Create a new CDMI capability
-spec nebula2_capabilities:new_capability(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_capability(Req, State) ->
    _DefaultCapabilities = [{<<"cdmi_domains">>, true},
                           {<<"cdmi_dataobjects">>, true},
                           {<<"cdmi_object_access_by_ID">>, true},
                           {<<"cdmi_object_copy_from_local">>, true},
                           {<<"cdmi_object_move_from_ID">>, true},
                           {<<"cdmi_object_move_from_local">>, true}],
    Oid = nebula2_utils:make_key(),
    _Tstamp = list_to_binary(nebula2_utils:get_time()),
    _Location = list_to_binary(nebula2_app:cdmi_location()),
    {Pid, _Opts} = State,
    {Path, _} = cowboy_req:path_info(Req),
    lager:debug("Path is ~p" , [Path]),
    ObjectName = case Path of
                    [] ->
                        "/";
                    U  -> 
                        "/" ++ build_path(U)
                 end,
    lager:debug("ObjectName is ~p", [ObjectName]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    _Data = maps:from_list(jsx:decode(Body)),
    CapabilitiesURI = nebula2_utils:get_capabilities_uri(Pid, ObjectName),
    DomainURI = nebula2_utils:get_domain_uri(Pid, ObjectName),
    ObjectType = "application/cdmi-capability",
    case nebula2_utils:get_parent(Pid, ObjectName) of
        {ok, ParentUri, ParentId} ->
            lager:debug("Creating new capability. ParentUri: ~p ParentId: ~p", [ParentUri, ParentId]),
            lager:debug("                        Container Name: ~p", [ObjectName]),
            lager:debug("                        OID: ~p", Oid),
            Data2 = [{<<"objectType">>, list_to_binary(ObjectType)},
                     {<<"objectID">>, list_to_binary(Oid)},
                     {<<"objectName">>, list_to_binary(ObjectName)},
                     {<<"parentID">>, list_to_binary(ParentId)},
                     {<<"parentURI">>, list_to_binary(ParentUri)},
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

%% @doc Update a CDMI capability
-spec nebula2_capabilities:update_capability(pid(), object_oid(), map()) -> {boolean(), json_value()}.
update_capability(Pid, ObjectId, Data) ->
    lager:debug("nebula2_capabilities:update_capability: Pid: ~p ObjectId: ~p Data: ~p", [Pid, ObjectId, Data]),
    NewData = Data,
    {ok, NewData}.

%% ====================================================================
%% Internal functions
%% ====================================================================
    
build_path(L) ->
    build_path(L, []).
build_path([], Acc) ->
    Acc;
build_path([H|T], Acc) ->
    Acc2 = lists:append(Acc, binary_to_list(H) ++ "/"),
    build_path(T, Acc2).

update_parent("", _, _, _) ->
    %% Must be the root, since there is no parent.
    ok;
update_parent(ParentId, ObjectName, ObjectType, Pid) ->
    lager:debug("update_parent: ~p ~p ~p ~p", [ParentId, ObjectName, ObjectType, Pid]),
    N = lists:last(string:tokens(ObjectName, "/")),
    Name = case ObjectType of
               "application/cdmi-capability" -> N ++ "/";
               _ -> 
                   N
           end,
    Parent = nebula2_capabilities:get_capability(Pid, ParentId),
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