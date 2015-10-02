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
-export([apply_metadata_capabilities/2,
         cdmi_acount/2,
         cdmi_atime/2,
         cdmi_assignedsize/2,
         cdmi_ctime/2,
         cdmi_data_autodelete/2,
         cdmi_data_dispersion/2,
         cdmi_data_holds/2,
         cdmi_data_redundancy/2,
         cdmi_data_retention/2,
         cdmi_encryption/2,
         cdmi_geographic_placement/2,
         cdmi_immediate_redundancy/2,
         cdmi_infrastructure_redundancy/2,
         cdmi_latency/2,
         cdmi_mcount/2,
         cdmi_mtime/2,
         cdmi_RPO/2,
         cdmi_RTO/2,
         cdmi_sanitization_method/2,
         cdmi_size/2,
         cdmi_throughput/2,
         cdmi_value_hash/2,
         get_capability/2,
         new_capability/2,
         update_capability/3
]).

%% @doc Get a CDMI capability
-spec nebula2_capabilities:get_capability(pid(), object_oid()) -> {ok, json_value()}.
get_capability(Pid, Oid) ->
    lager:debug("Entry"),
    {ok, Data} = nebula2_db:read(Pid, Oid),
    Data.

%% @doc Create a new CDMI capability
-spec nebula2_capabilities:new_capability(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_capability(Req, State) ->
    lager:debug("Entry"),
    Oid = nebula2_utils:make_key(),
    {Pid, EnvMap} = State,
    {Path, _} = cowboy_req:path_info(Req),
    ObjectName = case Path of
                    [] ->
                        "/";
                    U  -> 
                        "/" ++ build_path(U)
                 end,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Data = jsx:decode(Body, [return_maps]),
    ObjectType = ?CONTENT_TYPE_CDMI_CAPABILITY,
    case maps:get(<<"parentURI">>, EnvMap, undefined) of
        undefined ->
            pooler:return_member(riak_pool, Pid),
            {false, Req2, State};
        ParentUri ->
            ParentId = nebula2_utils:get_object_oid(State),
            Data2 = maps:from_list([{<<"capabilities">>, Data},
                     {<<"objectType">>, list_to_binary(ObjectType)},
                     {<<"objectID">>, list_to_binary(Oid)},
                     {<<"objectName">>, list_to_binary(ObjectName)},
                     {<<"parentID">>, list_to_binary(ParentId)},
                     {<<"parentURI">>, list_to_binary(ParentUri)}
                    ]),
            {ok, Oid} = nebula2_db:create(Pid, Oid, Data2),
            ok = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            Req3 = cowboy_req:set_resp_body(list_to_binary(maps:to_list(Data2)), Req2),
            {true, Req3, State}
    end.

%% @doc Update a CDMI capability
-spec nebula2_capabilities:update_capability(cowboy_req:req(), pid(), object_oid()) -> {ok, json_value()}.
update_capability(Req, State, Oid) ->
    lager:debug("Entry"),
    {Pid, _} = State,
    {ok, Body, Req2} = cowboy_req:body(Req),
    NewData = jsx:decode(Body, [return_maps]),
    NewCapabilities = maps:get(<<"capabilities">>, NewData),
    lager:debug("NewData: ~p", [NewData]),
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldCapabilities = maps:get(<<"capabilities">>, OldData),
    OldMetaData = maps:get(<<"metadata">>, OldData, maps:new()),
    NewMetaData = maps:get(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Capabilities = maps:merge(OldCapabilities, NewCapabilities),
    Data2 = maps:put(<<"capabilities">>, Capabilities, OldData),
    Data3 = maps:put(<<"metadata">>, MetaData, Data2),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>],
    CList2 = maps:to_list(maps:with(CList, maps:get(<<"capabilities">>, OldData))),
    Data4 = nebula2_capabilities:apply_metadata_capabilities(CList2, Data3),
    Response = case nebula2_db:update(Pid, Oid, Data4) of
                   ok ->
                       {true, Req2, State};
                   _  ->
                       {false, Req, State}
               end,
    lager:debug("Data4: ~p", [Data4]),
    Response.

%% @doc Apply CDMI capabilities
-spec nebula2_metadata_capabilities:apply_capabilities(list(), map()) -> map().
apply_metadata_capabilities([], Data) ->
    Data;
apply_metadata_capabilities([H|T], Data) ->
    {Func, Arg} = H,
    A = list_to_atom(string:to_lower(binary_to_list(Arg))),
    F = list_to_atom(binary_to_list(Func)),
    Data2 = try nebula2_capabilities:F(A, Data) of
                NewData -> NewData
            catch
                error:undef ->
                    lager:warning("No handler for metadata capability ~p with argument ~p", [Func, Arg]),
                    Data
            end,
    apply_metadata_capabilities(T, Data2).

%% ============================================================================
%% The following functions are for applying any CDMI capabilities on the object
%% ============================================================================

%% @doc Apply cdmi_acount
-spec cdmi_acount(string(), map()) -> map().
cdmi_acount(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            MD = maps:get(<<"metadata">>, Data),
            ACount = maps:get(<<"cdmi_acount">>, MD, 0) + 1,
            MD2 = maps:put(<<"cdmi_acount">>, ACount, MD),
            maps:put(<<"metadata">>, MD2, Data);
        false ->
            MD = maps:get(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_acount">>, MD),
            maps:put(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_atime
-spec cdmi_atime(string(), map()) -> map().
cdmi_atime(Doit, Data) ->
    lager:debug("Entry"),
    lager:debug("Doit: ~p", [Doit]),
    case Doit of
        true ->
            MD = maps:get(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = maps:put(<<"cdmi_atime">>, Tstamp, MD),
            maps:put(<<"metadata">>, MD2, Data);
        false ->
            MD = maps:get(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_atime">>, MD),
            maps:put(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_assignedsize
-spec cdmi_assignedsize(string(), map()) -> map().
cdmi_assignedsize(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_assignedsize processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_ctime
-spec cdmi_ctime(string(), map()) -> map().
cdmi_ctime(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            MD = maps:get(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = maps:put(<<"cdmi_ctime">>, Tstamp, MD),
            maps:put(<<"metadata">>, MD2, Data);
        false ->
            MD = maps:get(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_ctime">>, MD),
            maps:put(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_data_autodelete
-spec cdmi_data_autodelete(string(), map()) -> map().
cdmi_data_autodelete(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_data_autodelete processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_dispersion
-spec cdmi_data_dispersion(string(), map()) -> map().
cdmi_data_dispersion(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_data_dispersion processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_holds
-spec cdmi_data_holds(string(), map()) -> map().
cdmi_data_holds(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_data_holds processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_redundancy
-spec cdmi_data_redundancy(string(), map()) -> map().
cdmi_data_redundancy(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_data_redundancy processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_retention
-spec cdmi_data_retention(string(), map()) -> map().
cdmi_data_retention(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_data_retention processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_encryption
-spec cdmi_encryption(list(), map()) -> map().
cdmi_encryption(EncryptionMethods, Data) ->
    lager:debug("Entry"),
    lager:debug("Do cdmi_encryption processing here for ~p", [EncryptionMethods]),
    Data.

%% @doc Apply cdmi_geographic_placement
-spec cdmi_geographic_placement(string(), map()) -> map().
cdmi_geographic_placement(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_geographic_placement processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_immediate_redundancy
-spec cdmi_immediate_redundancy(string(), map()) -> map().
cdmi_immediate_redundancy(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        "" ->
            Data;
        _ ->
            lager:debug("Do cdmi_immediate_redundancy processing here"),
            Data
    end.

%% @doc Apply cdmi_infrastructure_redundancy
-spec cdmi_infrastructure_redundancy(string(), map()) -> map().
cdmi_infrastructure_redundancy(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        "" ->
            Data;
        _ ->
            lager:debug("Do cdmi_infrastructure_redundancy processing here"),
            Data
    end.

%% @doc Apply cdmi_latency
-spec cdmi_latency(string(), map()) -> map().
cdmi_latency(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_latency processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_mcount
-spec cdmi_mcount(string(), map()) -> map().
cdmi_mcount(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            MD = maps:get(<<"metadata">>, Data),
            ACount = maps:get(<<"cdmi_mcount">>, MD, 0) + 1,
            MD2 = maps:put(<<"cdmi_mcount">>, ACount, MD),
            maps:put(<<"metadata">>, MD2, Data);
        false ->
            MD = maps:get(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_mcount">>, MD),
            maps:put(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_mtime
-spec cdmi_mtime(string(), map()) -> map().
cdmi_mtime(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            MD = maps:get(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = maps:put(<<"cdmi_mtime">>, Tstamp, MD),
            Map2 = maps:put(<<"metadata">>, MD2, Data),
            Map2;
        false ->
            MD = maps:get(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_mtime">>, MD),
            maps:put(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_RPO
-spec cdmi_RPO(string(), map()) -> map().
cdmi_RPO(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_RPO processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_RTO
-spec cdmi_RTO(string(), map()) -> map().
cdmi_RTO(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_RTO processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_sanitization_method
-spec cdmi_sanitization_method(list(), map()) -> map().
cdmi_sanitization_method(Methods, Data) ->
    lager:debug("Entry"),
    lager:debug("Do cdmi_sanitization_method processing here for ~p", [Methods]),
    Data.

%% @doc Apply cdmi_size
-spec cdmi_size(string(), map()) -> map().
cdmi_size(Doit, Data) ->
    lager:debug("Entry"),
    case binary_to_list(maps:get(<<"objectType">>, Data)) of
        ?CONTENT_TYPE_CDMI_DATAOBJECT ->
            case Doit of
                true ->
                    Value = binary_to_list(maps:get(<<"value">>, Data)),
                    Size = string:len(Value),
                    MD = maps:get(<<"metadata">>, Data),
                    MD2 = maps:put(<<"cdmi_size">>, Size, MD),
                    maps:put(<<"metadata">>, MD2, Data);
                false ->
                    lager:debug("false"),
                    MD = maps:get(<<"metadata">>, Data),
                    MD2 = maps:remove(<<"cdmi_size">>, MD),
                    maps:put(<<"metadata">>, MD2, Data)
            end;
        _ ->
            Data
    end.

%% @doc Apply cdmi_throughput
-spec cdmi_throughput(string(), map()) -> map().
cdmi_throughput(Doit, Data) ->
    lager:debug("Entry"),
    case Doit of
        true ->
            lager:debug("Do cdmi_throughput processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_value_hash
-spec cdmi_value_hash(list(), map()) -> map().
cdmi_value_hash(ValueHash, Data) ->
    lager:debug("Entry"),
    lager:debug("Do cdmi_value_hash processing here for ~p", [ValueHash]),
    Data.

%% ====================================================================
%% Internal functions
%% ====================================================================
build_path(L) ->
    lager:debug("Entry"),
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