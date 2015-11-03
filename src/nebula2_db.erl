%% @author mmartin
%% @doc Handles all CRUD requests to the CDMI metadata backend.
%% @doc Initial backend is riak. This module could be replaced to
%% @doc make use of some other storage backend.

-module(nebula2_db).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("nebula2_test.hrl").
-endif.

-include("nebula.hrl").
%% ====================================================================
%% API functions
%% ====================================================================
-export([available/1,
         create/3,
         delete/2,
         get_domain_maps/1,
         marshall/1,
         marshall/2,
         read/2,
         search/2,
         unmarshall/1,
         update/3]).

%% @doc Check for the availability of the metadata backend.
-spec nebula2_db:available(pid()) -> boolean().
available(Pid) when is_pid(Pid) ->
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    Mod:available(Pid).
    
%% @doc Create an object.
-spec create(pid(),
             object_oid(),   %% Oid
             map()           %% Data to store
            ) -> {'error', _} | {'ok', _}.
create(Pid, Oid, Data) when is_pid(Pid); is_binary(Oid); is_map(Data) ->
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    ?nebMsg("Entry"),
    Response = Mod:put(Pid, Oid, Data),
    nebula2_utils:set_cache(Data),
    Response.
    
%% @doc Delete an object
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    nebula2_utils:delete_cache(Oid),
    Mod:delete(Pid, Oid).

%% @doc Get the domain maps.
-spec nebula2_db:get_domain_maps(pid()) -> list().
get_domain_maps(Pid) when is_pid(Pid) ->
    ?nebMsg("Entry"),
    Domain = nebula2_utils:get_domain_hash(?SYSTEM_DOMAIN_URI),
    Path = Domain ++ "/system_configuration/"++ "domain_maps",
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            ?nebFmt("1 Cache Hit: Path: ~p", [Path]),
            nebula2_utils:get_value(<<"value">>, Data, <<"[]">>);
        _ ->
            ?nebFmt("1 Cache Miss: ~p", [Path]),
            case Mod:get_domain_maps(Pid, Path) of
                {ok, DomainMaps} ->
                    nebula2_utils:set_cache(DomainMaps),
                    D = nebula2_utils:get_value(<<"value">>, DomainMaps, <<"[]">>),
                    ?nebFmt("Found domain maps: ~p", [D]),
                    D;
                _ ->
                    <<"[]">>
            end
    end.

-spec marshall(map()) -> map().
marshall(Data) when is_map(Data) ->
    ?nebMsg("Entry"),
    SearchKey = nebula2_utils:make_search_key(Data),
    marshall(Data, SearchKey).

-spec marshall(map(), binary()) -> map().
marshall(Data, SearchKey) when is_map(Data), is_binary(SearchKey) ->
    ?nebMsg("Entry"),
    maps:from_list([{<<"cdmi">>, Data}, {<<"sp">>, SearchKey}]).

%% @doc Read an object
-spec read(pid(), object_oid()) -> {ok, map()}|{error, term()}.
read(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    ?nebFmt("Oid: ~p", [Oid]),
    case nebula2_utils:get_cache(Oid) of
        {ok, Data} ->
            ?nebFmt("Cache Hit: Oid: ~p", [Oid]),
            {ok, Data};
        _ ->
            ?nebFmt("Cache Miss: Oid: ~p", [Oid]),
            case Mod:get(Pid, Oid) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
            end
    end.

%% @doc Search an index for objects.
-spec search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) when is_list(Path), is_tuple(State) ->
    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            ?nebFmt("2 Cache Hit: Key: ~p", [Path]),
            {ok, Data};
        _ ->
            ?nebFmt("2 Cache Miss: ~p", [Path]),
            case Mod:search(Path, State) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                Response ->
                    Response
            end
    end.

-spec unmarshall(map()) -> map().
unmarshall(Data) when is_map(Data) ->
    ?nebMsg("Entry"),
    maps:get(<<"cdmi">>, Data).

%% @doc Update an object.
-spec update(pid(),
             object_oid(),      %% Oid
             map()              %% Data to store
            ) -> ok | {error, term()}.
update(Pid, Oid, Data) when is_pid(Pid); is_binary(Oid); is_map(Oid) ->
    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    Data2 = jsx:encode(Data),
    case Mod:update(Pid, Oid, Data2) of
        ok ->
            nebula2_utils:set_cache(Data),
            ok;
        Failure ->
            Failure
    end.
    
%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).

available_test() ->
    Pid = self(),
    Mod = ?TestMetadataModule,
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, available, [Pid], true),
    ?assert(available(Pid)),
    meck:expect(Mod, available, [Pid], false),
    ?assertNot(available(Pid)),
    ?assertException(error, function_clause, available(not_a_pid)),
    meck:unload(Mod).

create_test() ->
    Pid = self(),
    TestMap = jsx:decode(?TestBinary, [return_maps]),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, put, [Pid, ?TestOid, TestMap], {ok, ?TestOid}),
    ?assertMatch({ok, ?TestOid}, create(Pid, ?TestOid, TestMap)),
    meck:expect(Mod, put, [Pid, ?TestOid, TestMap], {error,ioerror}),
    ?assertMatch({error,ioerror}, create(Pid, ?TestOid, TestMap)),
    ?assertException(error, function_clause, create(not_a_pid, ?TestOid, TestMap)),
    ?assertException(error, function_clause, create(Pid, not_an_oid, TestMap)),
    ?assertException(error, function_clause, create(Pid, ?TestOid, not_a_map)),
    meck:unload(Mod).

delete_test() ->
    Pid = self(),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_strict]),
    meck:expect(Mod, delete, [Pid, ?TestOid], ok),
    ?assertMatch(ok, delete(Pid, ?TestOid)),
    meck:expect(Mod, delete, [Pid, ?TestOid], {error,ioerror}),
    meck:expect(nebula2_utils, delete_cache, [?TestOid], ok),
    ?assertMatch({error,ioerror}, delete(Pid, ?TestOid)),
    ?assertException(error, function_clause, delete(not_a_pid, ?TestOid)),
    ?assertException(error, function_clause, delete(Pid, not_a_binary)),
    meck:unload(nebula2_utils),
    meck:unload(Mod).

get_domain_maps_test() ->
    Pid = self(),
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    Path = ?TestSystemDomainHash ++ "/system_configuration/"++ "domain_maps",
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_strict]),
    meck:new(nebula2_utils, [non_strict]),
    meck:expect(nebula2_utils, get_domain_hash, [?SYSTEM_DOMAIN_URI], ?TestSystemDomainHash),
    meck:expect(nebula2_utils, get_cache, [Path], {ok, TestMap}),
    meck:expect(nebula2_utils, get_value, [<<"value">>, TestMap, <<"[]">>], ?TestDomainMapsValue),
    Value = ?TestDomainMapsValue,
    ?assertMatch(Value, get_domain_maps(Pid)),
    meck:expect(nebula2_utils, get_cache, [Path], {error, noconn}),
    meck:expect(Mod, get_domain_maps, [Pid, Path], {ok, TestMap}),
    meck:expect(nebula2_utils, set_cache, [TestMap], {error, noconn}),
    ?assertMatch(Value, get_domain_maps(Pid)),
    meck:expect(Mod, get_domain_maps, [Pid, Path], {error, not_found}),
    ?assertMatch(<<"[]">>, get_domain_maps(Pid)),
    ?assertException(error, function_clause, get_domain_maps(not_a_pid)),
    meck:unload(nebula2_utils),
    meck:unload(Mod).

marshall_test() ->
    meck:new(nebula2_utils, [non_strict]),
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
    meck:expect(nebula2_utils, make_search_key, [TestMap], ?TestSearchKey),
    ?assertMatch(CdmiMap, marshall(TestMap)),
    ?assertException(error, function_clause, marshall(not_a_binary)),
    ?assertException(error, function_clause, marshall(TestMap, not_a_binary)),
    meck:unload(nebula2_utils).

read_test() ->
    Pid = self(),
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_strict]),
    meck:new(nebula2_utils, [non_strict]),
    meck:expect(nebula2_utils, get_cache, [?TestOid], {ok, TestMap}),
    meck:expect(Mod, get, [Pid, ?TestOid], {ok, TestMap}),
    ?assertMatch({ok, TestMap}, read(Pid, ?TestOid)),
    meck:expect(nebula2_utils, get_cache, [?TestOid], {error, deleted}),
    meck:expect(nebula2_utils, set_cache, [TestMap], ok),
    meck:expect(Mod, get, [Pid, ?TestOid], {ok, TestMap}),
    ?assertMatch({ok, TestMap}, read(Pid, ?TestOid)),
    meck:expect(Mod, get, [Pid, ?TestOid], {error, enomem}),
    ?assertMatch({error, enomem}, read(Pid, ?TestOid)),
    ?assertException(error, function_clause, read(not_a_pid, ?TestBinary)),
    ?assertException(error, function_clause, read(Pid, not_a_binary)),
    meck:unload(nebula2_utils),
    meck:unload(Mod).

search_test() ->
    Pid = self(),
    Path = ?TestSystemDomainHash ++ "/system_configuration/"++ "domain_maps",
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    State = {Pid, maps:new()},
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_script]),
    meck:new(nebula2_utils, [non_strict]),
    meck:expect(nebula2_utils, get_cache, [Path], {ok, TestMap}),
    ?assertMatch({ok, TestMap}, search(Path, State)),
    meck:expect(nebula2_utils, get_cache, [Path], {error, not_found}),
    meck:expect(Mod, search, [Path, State], {ok, TestMap}),
    meck:expect(nebula2_utils, set_cache, ['_'], {ok, TestMap}),
    ?assertMatch({ok, TestMap}, search(Path, {Pid, maps:new()})),
    meck:expect(Mod, search, [Path, State], {error, not_found}),
    ?assertMatch({error, not_found}, search(Path, {Pid, maps:new()})),
    meck:unload(nebula2_utils),
    meck:unload(Mod).
    
unmarshall_test() ->
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
    ?assertMatch(TestMap, unmarshall(CdmiMap)),
    ?assertException(error, function_clause, unmarshall(not_a_map)).

update_test() ->
    Pid = self(),
    TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
    CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
    TestOid = ?TestOid,
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    meck:new(Mod, [non_script]),
    meck:new(nebula2_utils, [non_strict]),
    meck:expect(Mod, update, ['_', '_', '_'], ok),
    meck:expect(nebula2_utils, set_cache, ['_'], {ok, TestMap}),
    ?assertMatch(ok, update(Pid, TestOid, CdmiMap)),
    meck:expect(Mod, update, ['_', '_', '_'], {error, term}),
    ?assertMatch({error, term}, update(Pid, TestOid, CdmiMap)),

    meck:unload(nebula2_utils),
    meck:unload(Mod).
-endif.
