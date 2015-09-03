%% @author mmartin
%% @doc Handle CDMI capability objects.

-module(nebula2_capabilities).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

-define(DEFAULT_CAPABILITIES, [{<<"cdmi_dataobjects">>, <<"true">>},
                               {<<"cdmi_domains">>, <<"true">>},
                               {<<"cdmi_object_access_by_ID">>, <<"true">>},
                               {<<"cdmi_object_copy_from_local">>, <<"true">>},
                               {<<"cdmi_object_move_from_ID">>, <<"true">>},
                               {<<"cdmi_object_move_from_local">>, <<"true">>}
                              ]).
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
    lager:debug("Entry"),
    {ok, Data} = nebula2_riak:get(Pid, Oid),
    Data.

%% @doc Create a new CDMI capability
-spec nebula2_capabilities:new_capability(Req, State) -> {boolean(), Req, State}
        when Req::cowboy_req:req().
new_capability(Req, State) ->
    Oid = nebula2_utils:make_key(),
    {Pid, EnvMap} = State,
    {Path, _} = cowboy_req:path_info(Req),
    % lager:debug("Capability Path is ~p" , [Path]),
    ObjectName = case Path of
                    [] ->
                        "/";
                    U  -> 
                        "/" ++ build_path(U)
                 end,
    % lager:debug("ObjectName is ~p", [ObjectName]),
    {ok, Body, Req2} = cowboy_req:body(Req),
    Data = jsx:decode(Body, [return_maps]),
    ObjectType = ?CONTENT_TYPE_CDMI_CAPABILITY,
    case maps:get(<<"parentURI">>, EnvMap, undefined) of
        undefined ->
            pooler:return_member(riak_pool, Pid),
            {false, Req2, State};
        ParentUri ->
            ParentId = nebula2_utils:get_object_oid(State),
            % lager:debug("Creating new capability. ParentUri: ~p ParentId: ~p", [ParentUri, ParentId]),
            % lager:debug("                        Container Name: ~p", [ObjectName]),
            % lager:debug("                        OID: ~p", Oid),
            Data2 = maps:from_list([{<<"capabilities">>, Data},
                     {<<"objectType">>, list_to_binary(ObjectType)},
                     {<<"objectID">>, list_to_binary(Oid)},
                     {<<"objectName">>, list_to_binary(ObjectName)},
                     {<<"parentID">>, list_to_binary(ParentId)},
                     {<<"parentURI">>, list_to_binary(ParentUri)}
                    ]),
            {ok, Oid} = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
            ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            Req3 = cowboy_req:set_resp_body(list_to_binary(maps:to_list(Data2)), Req2),
            {true, Req3, State}
    end.

%% @doc Update a CDMI capability
-spec nebula2_capabilities:update_capability(pid(), object_oid(), map()) -> {ok, json_value()}.
update_capability(_Pid, _ObjectId, Data) ->
    % lager:debug("nebula2_capabilities:update_capability: Pid: ~p ObjectId: ~p Data: ~p", [Pid, ObjectId, Data]),
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

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.