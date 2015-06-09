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
    Oid = nebula2_utils:make_key(),
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
    _Data = maps:from_list(jsx:decode(Body)),
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
                     {<<"parentURI">>, list_to_binary(ParentUri)}],
            {ok, Oid} = nebula2_riak:put(Pid, ObjectName, Oid, Data2),
            ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
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

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.