%% @author mmartin
%% @doc
%% Handle CDMI capability objects.
%% @end

-module(nebula2_capabilities).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
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
-spec nebula2_capabilities:get_capability(pid(), object_oid()) -> map().
get_capability(Pid, Oid) ->
    ?nebMsg("Entry"),
    {ok, Data} = nebula2_db:read(Pid, Oid),
    Data.

%% @doc
%% Create a new CDMI capability
%% @throws badjson
%% @end

-spec nebula2_capabilities:new_capability(cowboy_req:req(), cdmi_state()) -> {boolean(), cowboy_req:req(), cdmi_state()}.
new_capability(Req, State) when is_tuple(State) ->
%    ?nebMsg("Entry"),
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
    Data = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    NewBody
            catch
                error:badarg ->
                    throw(badjson)
            end,
    ObjectType = ?CONTENT_TYPE_CDMI_CAPABILITY,
    case nebula2_utils:get_value(<<"parentURI">>, EnvMap, <<"undefined">>) of
        <<"undefined">> ->
            pooler:return_member(riak_pool, Pid),
            {false, Req2, State};
        ParentUri ->
            {ok, ParentId} = nebula2_utils:get_object_oid(ParentUri, State),
            Data2 = maps:from_list([{<<"capabilities">>, Data},
                     {<<"objectType">>, ObjectType},
                     {<<"objectID">>, Oid},
                     {<<"objectName">>, list_to_binary(ObjectName)},
                     {<<"parentID">>, ParentId},
                     {<<"parentURI">>, ParentUri}
                    ]),
            {ok, Oid} = nebula2_db:create(Pid, Oid, Data2),
            {ok, _} = nebula2_utils:update_parent(ParentId, ObjectName, ObjectType, Pid),
            pooler:return_member(riak_pool, Pid),
            Req3 = cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req2),
            {true, Req3, State}
    end.

%% @doc
%% Update a CDMI capability
%% @throws badjson
%% @end
-spec nebula2_capabilities:update_capability(cowboy_req:req(), cdmi_state(), object_oid()) ->
          {boolean(), cowboy_req:req(), cdmi_state()}.
update_capability(Req, State, Oid) when is_tuple(State), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    {Pid, _} = State,
    {ok, Body, Req2} = cowboy_req:body(Req),
    NewData = try jsx:decode(Body, [return_maps]) of
                  NewBody ->
                      nebula2_db:marshall(NewBody)
              catch
                  error:badarg ->
                      throw(badjson)
              end,
    NewCapabilities = nebula2_utils:get_value(<<"capabilities">>, NewData),
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldCapabilities = nebula2_utils:get_value(<<"capabilities">>, OldData),
    OldMetaData = nebula2_utils:get_value(<<"metadata">>, OldData, maps:new()),
    NewMetaData = nebula2_utils:get_value(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Capabilities = maps:merge(OldCapabilities, NewCapabilities),
    Data2 = nebula2_utils:put_value(<<"capabilities">>, Capabilities, OldData),
    Data3 = nebula2_utils:put_value(<<"metadata">>, MetaData, Data2),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>],
    CList2 = maps:to_list(maps:with(CList, nebula2_utils:get_value(<<"capabilities">>, OldData))),
    Data4 = nebula2_capabilities:apply_metadata_capabilities(CList2, Data3),
    Response = case nebula2_db:update(Pid, Oid, Data4) of
                   {ok, _} ->
                       {true, Req2, State};
                   _  ->
                       {false, Req, State}
               end,
    Response.

%% @doc Apply CDMI capabilities
-spec apply_metadata_capabilities(list(), map()) -> map().
apply_metadata_capabilities([], Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
    Data;
apply_metadata_capabilities([H|T], Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
    {Func, Arg} = H,
    A = list_to_atom(string:to_lower(binary_to_list(Arg))),
    F = list_to_atom(binary_to_list(Func)),
    Data2 = try nebula2_capabilities:F(A, Data) of
                NewData -> NewData
            catch
                error:undef ->
                    ?nebWarnFmt("No handler for metadata capability ~p with argument ~p", [Func, Arg]),
                    Data
            end,
    apply_metadata_capabilities(T, Data2).

%% ============================================================================
%% The following functions are for applying any CDMI capabilities on the object
%% ============================================================================

%% @doc Apply cdmi_acount
-spec cdmi_acount(boolean(), map()) -> map().
cdmi_acount(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    case Doit of
        true ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            ACount = nebula2_utils:get_value(<<"cdmi_acount">>, MD, 0) + 1,
            MD2 = nebula2_utils:put_value(<<"cdmi_acount">>, ACount, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data);
        false ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_acount">>, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_assignedsize
-spec cdmi_assignedsize(boolean(), map()) -> map().
cdmi_assignedsize(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_assignedsize processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_atime
-spec cdmi_atime(boolean(), map()) -> map().
cdmi_atime(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    case Doit of
        true ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = nebula2_utils:put_value(<<"cdmi_atime">>, Tstamp, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data);
        false ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_atime">>, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_ctime
-spec cdmi_ctime(boolean(), map()) -> map().
cdmi_ctime(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = nebula2_utils:put_value(<<"cdmi_ctime">>, Tstamp, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data);
        false ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_ctime">>, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_data_autodelete
-spec cdmi_data_autodelete(boolean(), map()) -> map().
cdmi_data_autodelete(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_data_autodelete processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_dispersion
-spec cdmi_data_dispersion(boolean(), map()) -> map().
cdmi_data_dispersion(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_data_dispersion processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_holds
-spec cdmi_data_holds(boolean(), map()) -> map().
cdmi_data_holds(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_data_holds processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_redundancy
-spec cdmi_data_redundancy(boolean(), map()) -> map().
cdmi_data_redundancy(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            ?nebMsg("Do cdmi_data_redundancy processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_data_retention
-spec cdmi_data_retention(boolean(), map()) -> map().
cdmi_data_retention(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            ?nebMsg("Do cdmi_data_retention processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_encryption
-spec cdmi_encryption(list(), map()) -> map().
cdmi_encryption(EncryptionMethods, Data) when is_list(EncryptionMethods), is_map(Data) ->
    %% ?nebMsg("Entry"),
    ?nebFmt("Do cdmi_encryption processing here for ~p", [EncryptionMethods]),
    Data.

%% @doc Apply cdmi_geographic_placement
-spec cdmi_geographic_placement(boolean(), map()) -> map().
cdmi_geographic_placement(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_geographic_placement processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_immediate_redundancy
-spec cdmi_immediate_redundancy(boolean(), map()) -> map().
cdmi_immediate_redundancy(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            Data;
        _ ->
            %% ?nebMsg("Do cdmi_immediate_redundancy processing here"),
            Data
    end.

%% @doc Apply cdmi_infrastructure_redundancy
-spec cdmi_infrastructure_redundancy(boolean(), map()) -> map().
cdmi_infrastructure_redundancy(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            Data;
        _ ->
            %% ?nebMsg("Do cdmi_infrastructure_redundancy processing here"),
            Data
    end.

%% @doc Apply cdmi_latency
-spec cdmi_latency(boolean(), map()) -> map().
cdmi_latency(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            %% ?nebMsg("Do cdmi_latency processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_mcount
-spec cdmi_mcount(boolean(), map()) -> map().
cdmi_mcount(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            ACount = nebula2_utils:get_value(<<"cdmi_mcount">>, MD, 0) + 1,
            MD2 = nebula2_utils:put_value(<<"cdmi_mcount">>, ACount, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data);
        false ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_mcount">>, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_mtime
-spec cdmi_mtime(boolean(), map()) -> map().
cdmi_mtime(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            Tstamp = list_to_binary(nebula2_utils:get_time()),
            MD2 = nebula2_utils:put_value(<<"cdmi_mtime">>, Tstamp, MD),
            Map2 = nebula2_utils:put_value(<<"metadata">>, MD2, Data),
            Map2;
        false ->
            MD = nebula2_utils:get_value(<<"metadata">>, Data),
            MD2 = maps:remove(<<"cdmi_mtime">>, MD),
            nebula2_utils:put_value(<<"metadata">>, MD2, Data)
    end.

%% @doc Apply cdmi_RPO
-spec cdmi_RPO(boolean(), map()) -> map().
cdmi_RPO(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            ?nebMsg("Do cdmi_RPO processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_RTO
-spec cdmi_RTO(boolean(), map()) -> map().
cdmi_RTO(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            ?nebMsg("Do cdmi_RTO processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_sanitization_method
-spec cdmi_sanitization_method(list(), map()) -> map().
cdmi_sanitization_method(Methods, Data) when is_list(Methods), is_map(Data) ->
    %% ?nebMsg("Entry"),
    ?nebFmt("Do cdmi_sanitization_method processing here for ~p", [Methods]),
    Data.

%% @doc Apply cdmi_size
-spec cdmi_size(boolean(), map()) -> map().
cdmi_size(Doit, Data) when is_boolean(Doit), is_map(Data) ->
%    ?nebMsg("Entry"),
    case nebula2_utils:get_value(<<"objectType">>, Data) of
        ?CONTENT_TYPE_CDMI_DATAOBJECT ->
            case Doit of
                true ->
                    Val = nebula2_utils:get_value(<<"value">>, Data),
                    Value = if
                                is_binary(Val) ->
                                    binary_to_list(Val);
                                is_list(Val) ->
                                    Val;
                                true ->
                                    ?nebErrFmt("Type of value is ~p", [nebula2_utils:type_of(Val)]),
                                    throw(badjson)
                            end,
                    Size = string:len(Value),
                    MD = nebula2_utils:get_value(<<"metadata">>, Data),
                    MD2 = nebula2_utils:put_value(<<"cdmi_size">>, Size, MD),
                    R = nebula2_utils:put_value(<<"metadata">>, MD2, Data),
                    R;
                false ->
                    MD = nebula2_utils:get_value(<<"metadata">>, Data),
                    MD2 = maps:remove(<<"cdmi_size">>, MD),
                    nebula2_utils:put_value(<<"metadata">>, MD2, Data)
            end;
        _ ->
            Data
    end.

%% @doc Apply cdmi_throughput
-spec cdmi_throughput(boolean(), map()) -> map().
cdmi_throughput(Doit, Data) when is_boolean(Doit), is_map(Data) ->
    %% ?nebMsg("Entry"),
    case Doit of
        true ->
            ?nebMsg("Do cdmi_throughput processing here"),
            Data;
        false ->
            Data
    end.

%% @doc Apply cdmi_value_hash
-spec cdmi_value_hash(list(), map()) -> map().
cdmi_value_hash(ValueHash, Data) when is_list(ValueHash), is_map(Data) ->
    % ?nebMsg("Entry"),
    ?nebFmt("Do cdmi_value_hash processing here for ~p", [ValueHash]),
    Data.

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
nebula2_capabilities_test_() ->
    {foreach,
        fun() ->
            meck:new(cowboy_req, [non_strict]),
            meck:new(pooler, [non_strict]),
            meck:new(nebula2_db, [passthrough]),
            meck:new(nebula2_utils, [passthrough])
        end,
        fun(_) ->
            meck:unload(cowboy_req),
            meck:unload(pooler),
            meck:unload(nebula2_db),
            meck:unload(nebula2_utils)
        end,
        [
            {
                "apply_metadata_capabilities/2",
                fun() ->
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Capabilities = maps:get(<<"capabilities">>, TestCapabilities),
                    CList = maps:to_list(maps:with([<<"cdmi_acount">>], Capabilities)),
                    TestCreateContainer = jsx:decode(?TestCreateContainer, [return_maps]),
                    NewData = maps:remove(<<"metadata">>, TestCreateContainer),
                    OriginalData = maps:put(<<"metadata">>, maps:from_list([{<<"cdmi_acount">>, 0}]), NewData),
                    UpdatedData = maps:put(<<"metadata">>, maps:from_list([{<<"cdmi_acount">>, 1}]), NewData),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    ?assertMatch(UpdatedData, apply_metadata_capabilities(CList, OriginalData)),
                    ?assert(meck:validate(nebula2_db))
                end
            },
            {
                "apply_metadata_capabilities_unknown_handler/2",
                fun() ->
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Capabilities = maps:get(<<"capabilities">>, TestCapabilities),
                    CList = maps:to_list(maps:with([<<"unknown_handler">>], Capabilities)),
                    TestCreateContainer = jsx:decode(?TestCreateContainer, [return_maps]),
                    NewData = maps:remove(<<"metadata">>, TestCreateContainer),
                    OriginalData = maps:put(<<"metadata">>, maps:from_list([{<<"cdmi_acount">>, 0}]), NewData),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    ?assertMatch(OriginalData, apply_metadata_capabilities(CList, OriginalData)),
                    ?assert(meck:validate(nebula2_db))
                end
            },
            {
                "apply_metadata_capabilities_contract/2",
                fun() ->
                    ?assertException(error, function_clause, apply_metadata_capabilities(not_a_list, maps:new())),
                    ?assertException(error, function_clause, apply_metadata_capabilities(self(), not_a_map))
                end
            },
            {
                "cdmi_acount/2",
                fun() ->
                    Count = 0,
                    Count0 = maps:from_list([{<<"cdmi_acount">>, Count}]),
                    Count1 = maps:from_list([{<<"cdmi_acount">>, Count + 1}]),
                    MapNone = maps:from_list([{<<"metadata">>, maps:new()}]),
                    Map = maps:from_list([{<<"metadata">>, Count0}]),
                    ReturnedData = maps:from_list([{<<"metadata">>, Count1}]),
                    ?assertMatch(ReturnedData, cdmi_acount(true, Map)),
                    ?assertMatch(MapNone, cdmi_acount(false, Map))
                end
            },
            {
                "cdmi_acount_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_acount(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_acount(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_acount(false, not_a_map))
                end
            },
            {
                "cdmi_assignedsize/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_assignedsize(true, Map)),
                    ?assertMatch(Map, cdmi_assignedsize(false, Map))
                end
            },
            {
                "cdmi_assignedsize_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_assignedsize(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_assignedsize(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_assignedsize(false, not_a_map))
                end
            },
            {
                "cdmi_atime/2",
                fun() ->
                    New_time = <<"20151122">>,
                    Time0 = maps:from_list([{<<"cdmi_atime">>, <<"00000000">>}]),
                    Time1 = maps:from_list([{<<"cdmi_atime">>, New_time}]),
                    MapNone = maps:from_list([{<<"metadata">>, maps:new()}]),
                    Map = maps:from_list([{<<"metadata">>, Time0}]),
                    ReturnedData = maps:from_list([{<<"metadata">>, Time1}]),
                    meck:sequence(nebula2_utils, get_time, 0, [binary_to_list(New_time)]),
                    ?assertMatch(ReturnedData, cdmi_atime(true, Map)),
                    ?assertMatch(MapNone, cdmi_atime(false, Map)),
                    ?assert(meck:validate(nebula2_utils))
                end
            },
            {
                "cdmi_atime_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_atime(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_atime(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_atime(false, not_a_map))
                end
            },
            {
                "cdmi_ctime/2",
                fun() ->
                    New_time = <<"20151122">>,
                    Time0 = maps:from_list([{<<"cdmi_ctime">>, <<"00000000">>}]),
                    Time1 = maps:from_list([{<<"cdmi_ctime">>, New_time}]),
                    MapNone = maps:from_list([{<<"metadata">>, maps:new()}]),
                    Map = maps:from_list([{<<"metadata">>, Time0}]),
                    ReturnedData = maps:from_list([{<<"metadata">>, Time1}]),
                    meck:sequence(nebula2_utils, get_time, 0, [binary_to_list(New_time)]),
                    ?assertMatch(ReturnedData, cdmi_ctime(true, Map)),
                    ?assertMatch(MapNone, cdmi_ctime(false, Map)),
                    ?assert(meck:validate(nebula2_utils))
                end
            },
            {
                "cdmi_ctime_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_ctime(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_ctime(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_ctime(false, not_a_map))
                end
            },
            {
                "cdmi_data_autodelete/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_data_autodelete(true, Map)),
                    ?assertMatch(Map, cdmi_data_autodelete(false, Map))
                end
            },
            {
                "cdmi_data_autodelete_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_data_autodelete(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_data_autodelete(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_data_autodelete(false, not_a_map))
                end
            },
            {
                "cdmi_data_dispersion/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_data_dispersion(true, Map)),
                    ?assertMatch(Map, cdmi_data_dispersion(false, Map))
                end
            },
            {
                "cdmi_data_dispersion_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_data_dispersion(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_data_dispersion(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_data_dispersion(false, not_a_map))
                end
            },
            {
                "cdmi_data_holds/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_data_holds(true, Map)),
                    ?assertMatch(Map, cdmi_data_holds(false, Map))
                end
            },
            {
                "cdmi_data_holds_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_data_holds(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_data_holds(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_data_holds(false, not_a_map))
                end
            },
            {
                "cdmi_data_redundancy/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_data_redundancy(true, Map)),
                    ?assertMatch(Map, cdmi_data_redundancy(false, Map))
                end
            },
            {
                "cdmi_data_redundancy_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_data_redundancy(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_data_redundancy(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_data_redundancy(false, not_a_map))
                end
            },
            {
                "cdmi_data_retention/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_data_retention(true, Map)),
                    ?assertMatch(Map, cdmi_data_retention(false, Map))
                end
            },
            {
                "cdmi_data_retention_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_data_retention(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_data_retention(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_data_retention(false, not_a_map))
                end
            },
            {
                "cdmi_encryption/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_encryption([], Map))
                end
            },
            {
                "cdmi_encryption_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_encryption(not_a_list, Map)),
                    ?assertException(error, function_clause, cdmi_encryption([], not_a_map))
                end
            },
            {
                "cdmi_geographic_placement/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_geographic_placement(true, Map)),
                    ?assertMatch(Map, cdmi_geographic_placement(false, Map))
                end
            },
            {
                "cdmi_geographic_placement_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_geographic_placement(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_geographic_placement(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_geographic_placement(false, not_a_map))
                end
            },
            {
                "cdmi_immediate_redundancy/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_immediate_redundancy(true, Map)),
                    ?assertMatch(Map, cdmi_immediate_redundancy(false, Map))
                end
            },
            {
                "cdmi_immediate_redundancy_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_immediate_redundancy(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_immediate_redundancy(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_immediate_redundancy(false, not_a_map))
                end
            },
            {
                "cdmi_infrastructure_redundancy/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_infrastructure_redundancy(true, Map)),
                    ?assertMatch(Map, cdmi_infrastructure_redundancy(false, Map))
                end
            },
            {
                "cdmi_infrastructure_redundancy_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_infrastructure_redundancy(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_infrastructure_redundancy(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_infrastructure_redundancy(false, not_a_map))
                end
            },
            {
                "cdmi_latency/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_latency(true, Map)),
                    ?assertMatch(Map, cdmi_latency(false, Map))
                end
            },
            {
                "cdmi_latency_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_latency(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_latency(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_latency(false, not_a_map))
                end
            },
            {
                "cdmi_mcount/2",
                fun() ->
                    Count = 0,
                    Count0 = maps:from_list([{<<"cdmi_mcount">>, Count}]),
                    Count1 = maps:from_list([{<<"cdmi_mcount">>, Count + 1}]),
                    MapNone = maps:from_list([{<<"metadata">>, maps:new()}]),
                    Map = maps:from_list([{<<"metadata">>, Count0}]),
                    ReturnedData = maps:from_list([{<<"metadata">>, Count1}]),
                    ?assertMatch(ReturnedData, cdmi_mcount(true, Map)),
                    ?assertMatch(MapNone, cdmi_mcount(false, Map))
                end
            },
            {
                "cdmi_mcount_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_mcount(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_mcount(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_mcount(false, not_a_map))
                end
            },
            {
                "cdmi_mtime/2",
                fun() ->
                    New_time = <<"20151122">>,
                    Time0 = maps:from_list([{<<"cdmi_mtime">>, <<"00000000">>}]),
                    Time1 = maps:from_list([{<<"cdmi_mtime">>, New_time}]),
                    MapNone = maps:from_list([{<<"metadata">>, maps:new()}]),
                    Map = maps:from_list([{<<"metadata">>, Time0}]),
                    ReturnedData = maps:from_list([{<<"metadata">>, Time1}]),
                    meck:sequence(nebula2_utils, get_time, 0, [binary_to_list(New_time)]),
                    ?assertMatch(ReturnedData, cdmi_mtime(true, Map)),
                    ?assertMatch(MapNone, cdmi_mtime(false, Map)),
                    ?assert(meck:validate(nebula2_utils))
                end
            },
            {
                "cdmi_mtime_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_mtime(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_mtime(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_mtime(false, not_a_map))
                end
            },
            {
                "cdmi_RPO/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_RPO(true, Map)),
                    ?assertMatch(Map, cdmi_RPO(false, Map))
                end
            },
            {
                "cdmi_RPO_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_RPO(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_RPO(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_RPO(false, not_a_map))
                end
            },
            {
                "cdmi_RTO/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_RTO(true, Map)),
                    ?assertMatch(Map, cdmi_RTO(false, Map))
                end
            },
            {
                "cdmi_RTO_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_RTO(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_RTO(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_RTO(false, not_a_map))
                end
            },
            {
                "cdmi_sanitization_method/2",
                fun() ->
                    List = ["method1", "method2"],
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_sanitization_method(List, Map))
                end
            },
            {
                "cdmi_sanitization_method_contract/2",
                fun() ->
                    List = ["method1", "method2"],
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_sanitization_method(not_a_list, Map)),
                    ?assertException(error, function_clause, cdmi_sanitization_method(List, not_a_map))
                end
            },
            {
                "cdmi_size_dataobject_binary/2",
                fun() ->
                    Value = "value",
                    EmptyMetadata = maps:new(),
                    ReturnedMetadata = maps:from_list([{<<"cdmi_size">>, string:len(Value)}]),
                    Map = maps:from_list([{<<"objectType">>, ?CONTENT_TYPE_CDMI_DATAOBJECT},
                                          {<<"value">>, list_to_binary(Value)}]),
                    Data = maps:put(<<"metadata">>, EmptyMetadata, Map),
                    ReturnData = maps:put(<<"metadata">>, ReturnedMetadata, Map),
                    ?assertMatch(ReturnData, cdmi_size(true, Data)),
                    ?assertMatch(Data, cdmi_size(false, Data))
                end
            },
            {
                "cdmi_size_dataobject_list/2",
                fun() ->
                    Value = "value",
                    EmptyMetadata = maps:new(),
                    ReturnedMetadata = maps:from_list([{<<"cdmi_size">>, string:len(Value)}]),
                    Map = maps:from_list([{<<"objectType">>, ?CONTENT_TYPE_CDMI_DATAOBJECT},
                                          {<<"value">>, Value}]),
                    Data = maps:put(<<"metadata">>, EmptyMetadata, Map),
                    ReturnData = maps:put(<<"metadata">>, ReturnedMetadata, Map),
                    ?assertMatch(ReturnData, cdmi_size(true, Data)),
                    ?assertMatch(Data, cdmi_size(false, Data))
                end
            },
            {
                "cdmi_size_dataobject_bad_data/2",
                fun() ->
                    Value = false,
                    EmptyMetadata = maps:new(),
                    Map = maps:from_list([{<<"objectType">>, ?CONTENT_TYPE_CDMI_DATAOBJECT},
                                          {<<"value">>, Value}]),
                    Data = maps:put(<<"metadata">>, EmptyMetadata, Map),
                    ?assertException(throw, badjson, cdmi_size(true, Data))
                end
            },
            {
                "cdmi_size_not_a_dataobject/2",
                fun() ->
                    Value = false,
                    EmptyMetadata = maps:new(),
                    Map = maps:from_list([{<<"objectType">>, ?CONTENT_TYPE_CDMI_CONTAINER},
                                          {<<"value">>, Value}]),
                    Data = maps:put(<<"metadata">>, EmptyMetadata, Map),
                    ?assertMatch(Data, cdmi_size(true, Data))
                end
            },
            {
                "cdmi_size_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_size(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_size(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_size(false, not_a_map))
                end
            },
            {
                "cdmi_throughput/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_throughput(true, Map)),
                    ?assertMatch(Map, cdmi_throughput(false, Map))
                end
            },
            {
                "cdmi_throughput_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_throughput(not_a_boolean, Map)),
                    ?assertException(error, function_clause, cdmi_throughput(true, not_a_map)),
                    ?assertException(error, function_clause, cdmi_throughput(false, not_a_map))
                end
            },
            {
                "cdmi_value_hash/2",
                fun() ->
                    Map = maps:new(),
                    ?assertMatch(Map, cdmi_value_hash("Hash", Map))
                end
            },
            {
                "cdmi_value_hash_contract/2",
                fun() ->
                    Map = maps:new(),
                    ?assertException(error, function_clause, cdmi_value_hash(not_a_list, Map)),
                    ?assertException(error, function_clause, cdmi_value_hash("Hash", not_a_map))
                end
            },
            {
                "get_capability/2",
                fun() ->
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    ?assertMatch(TestCapabilities, get_capability(Pid, Oid)),
                    ?assert(meck:validate(nebula2_db))
                end
            },
            {
                "get_capability_contract/2",
                fun() ->
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    ?assertException(error, function_clause, get_capability(not_a_pid, Oid)),
                    ?assertException(error, function_clause, get_capability(Pid, not_a_binary))
                end
            },
            {
                "new_capability/2",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"parentURI">>, maps:get(<<"parentURI">>, TestCapabilities)},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Capabilities = maps:from_list([{<<"capabilities">>, maps:get(<<"capabilities">>, TestCapabilities)}]),
                    Body = jsx:encode(Capabilities),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    NewOid = <<"new_oid">>,
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[Path], Req}]),
                    meck:loop(cowboy_req, set_resp_body, 2, [Req]),
                    meck:sequence(nebula2_db, create, 3, [{ok, NewOid}]),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    meck:sequence(nebula2_utils, get_object_oid, 2, [{ok, Oid}]),
                    meck:sequence(nebula2_utils, make_key, 0, [NewOid]),
                    meck:sequence(nebula2_utils, update_parent, 4, [{ok, Oid}]),
                    meck:sequence(pooler, return_member, 2, []),
                    ?assertMatch({true, Req, State}, new_capability(Req, State)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(nebula2_utils)),
                    ?assert(meck:validate(pooler))
                end
            },
            {
                "new_capability_no_path/2",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"parentURI">>, maps:get(<<"parentURI">>, TestCapabilities)},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Capabilities = maps:from_list([{<<"capabilities">>, maps:get(<<"capabilities">>, TestCapabilities)}]),
                    Body = jsx:encode(Capabilities),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    NewOid = <<"new_oid">>,
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[], Req}]),
                    meck:loop(cowboy_req, set_resp_body, 2, [Req]),
                    meck:sequence(nebula2_db, create, 3, [{ok, NewOid}]),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    meck:sequence(nebula2_utils, get_object_oid, 2, [{ok, Oid}]),
                    meck:sequence(nebula2_utils, make_key, 0, [NewOid]),
                    meck:sequence(nebula2_utils, update_parent, 4, [{ok, Oid}]),
                    meck:sequence(pooler, return_member, 2, []),
                    ?assertMatch({true, Req, State}, new_capability(Req, State)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(nebula2_utils)),
                    ?assert(meck:validate(pooler))
                    
                end
            },
            {
                "new_capability_no_parent/2",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Capabilities = maps:from_list([{<<"capabilities">>, maps:get(<<"capabilities">>, TestCapabilities)}]),
                    Body = jsx:encode(Capabilities),
                    NewOid = <<"new_oid">>,
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[Path], Req}]),
                    meck:sequence(nebula2_utils, make_key, 0, [NewOid]),
                    meck:sequence(pooler, return_member, 2, []),
                    ?assertMatch({false, Req, State}, new_capability(Req, State)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_utils)),
                    ?assert(meck:validate(pooler))
                end
            },
            {
                "new_capability_bad_json/2",
                fun() ->
                    Req = "",
                    Pid = self(),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Body = "junk",
                    NewOid = <<"new_oid">>,
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[Path], Req}]),
                    meck:sequence(nebula2_utils, make_key, 0, [NewOid]),
                    meck:sequence(pooler, return_member, 2, []),
                    ?assertException(throw, badjson, new_capability(Req, State)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_utils)),
                    ?assert(meck:validate(pooler))
                end
            },
            {
                "new_capability_contract/2",
                fun() ->
                    ?assertException(error, function_clause, new_capability("Req", not_a_tuple))
                end
            },
            {
                "update_capability/3",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"parentURI">>, maps:get(<<"parentURI">>, TestCapabilities)},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Capabilities = maps:from_list([{<<"capabilities">>, maps:get(<<"capabilities">>, TestCapabilities)}]),
                    Body = jsx:encode(Capabilities),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[Path], Req}]),
                    meck:loop(cowboy_req, set_resp_body, 2, [Req]),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    meck:sequence(nebula2_utils, get_object_oid, 2, [{ok, Oid}]),
                    meck:sequence(nebula2_db, update, 3, [{ok, TestCapabilities}]),
                    ?assertMatch({true, Req, State}, update_capability(Req, State, Oid)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(nebula2_utils))
                end
            },
            {
                "update_capability_fail/3",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"parentURI">>, maps:get(<<"parentURI">>, TestCapabilities)},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Capabilities = maps:from_list([{<<"capabilities">>, maps:get(<<"capabilities">>, TestCapabilities)}]),
                    Body = jsx:encode(Capabilities),
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    meck:loop(cowboy_req, path_info, 1, [{[Path], Req}]),
                    meck:loop(cowboy_req, set_resp_body, 2, [Req]),
                    meck:sequence(nebula2_db, read, 2, [{ok, TestCapabilities}]),
                    meck:sequence(nebula2_utils, get_object_oid, 2, [{ok, Oid}]),
                    meck:sequence(nebula2_db, update, 3, [{error, notfound}]),
                    ?assertMatch({false, Req, State}, update_capability(Req, State, Oid)),
                    ?assert(meck:validate(cowboy_req)),
                    ?assert(meck:validate(nebula2_db)),
                    ?assert(meck:validate(nebula2_utils))
                end
            },
            {
                "update_capability_bad_json/3",
                fun() ->
                    Req = "",
                    Pid = self(),
                    TestCapabilities = jsx:decode(?TestSystemCapabilities, [return_maps]),
                    Path = <<"/cdmi_capabilities/">>,
                    EnvMap = maps:from_list([{<<"path">>, Path},
                                             {<<"auth_as">>, <<"MickeyMouse">>},
                                             {<<"parentURI">>, maps:get(<<"parentURI">>, TestCapabilities)},
                                             {<<"objectName">>, <<"cdmi_capabilities/">>}
                                            ]),
                    State = {Pid, EnvMap},
                    Body = "junk",
                    Oid = maps:get(<<"objectID">>, TestCapabilities),
                    meck:loop(cowboy_req, body, 1, [{ok, Body, Req}]),
                    ?assertException(throw, badjson, update_capability(Req, State, Oid)),
                    ?assert(meck:validate(cowboy_req))
                end
            },
            {
                "update_capability_contract/3",
                fun() ->
                    Req = "",
                    ?assertException(error, function_clause, update_capability(Req, not_a_tuple, <<"oid">>)),
                    ?assertException(error, function_clause, update_capability(Req, {req, state}, not_a_binary))
                end
            }
        ]
    }.
-endif.
