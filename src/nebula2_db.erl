%% @author mmartin
%% @doc
%% Handles all CRUD requests to the CDMI metadata backend.
%% Initial backend is riak. This module could be replaced to
%% make use of some other storage backend.
%% @end

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
            ) -> {error, term()} | {ok, binary()}.
create(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_map(Data) ->
%%    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    Response = Mod:put(Pid, Oid, Data),
    nebula2_utils:set_cache(Data),
    Response.
    
%% @doc Delete an object
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%%    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    nebula2_utils:delete_cache(Oid),
    Mod:delete(Pid, Oid).

%% @doc Get the domain maps.
-spec nebula2_db:get_domain_maps(pid()) -> binary().
get_domain_maps(Pid) when is_pid(Pid) ->
%    ?nebMsg("Entry"),
    Domain = nebula2_utils:get_domain_hash(?SYSTEM_DOMAIN_URI),
    Path = Domain ++ "/system_configuration/"++ "domain_maps",
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            nebula2_utils:get_value(<<"value">>, Data, <<"[]">>);
        _ ->
            case Mod:get_domain_maps(Pid, Path) of
                {ok, DomainMaps} ->
                    nebula2_utils:set_cache(DomainMaps),
                    D = nebula2_utils:get_value(<<"value">>, DomainMaps, <<"[]">>),
                    D;
                _ ->
                    <<"[]">>
            end
    end.

%% @doc
%% Marshall the metadata into a form suitable for storage and indexing.
%% @end
-spec marshall(map()) -> map().
marshall(Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
    SearchKey = nebula2_utils:make_search_key(Data),
    marshall(Data, SearchKey).

%% @doc
%% Marshall the metadata into a form suitable for storage and indexing.
%% @end
-spec marshall(map(), list()) -> map().
marshall(Data, SearchKey) when is_map(Data), is_list(SearchKey) ->
%    ?nebMsg("Entry"),
    maps:from_list([{<<"cdmi">>, Data}, {<<"sp">>, list_to_binary(SearchKey)}]).

%% @doc Read an object
-spec read(pid(), object_oid()) -> {ok, map()}|{error, term()}.
read(Pid, Oid) when is_pid(Pid), is_binary(Oid) ->
%    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Oid) of
        {ok, Data} ->
            {ok, Data};
        _ ->
            case Mod:get(Pid, Oid) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
            end
    end.

%% @doc Search an index for objects.
-spec search(string(), cdmi_state()) -> {error, term()} | {ok, map()}.
search(Path, State) when is_list(Path), is_tuple(State) ->
%    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            {ok, Data};
        _ ->
            case Mod:search(Path, State) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                Response ->
                    Response
            end
    end.

%% @doc
%% Unarshall metadata from the backend for user consumption.
%% @end
-spec unmarshall(map()) -> map().
unmarshall(Data) when is_map(Data) ->
%    ?nebMsg("Entry"),
    maps:get(<<"cdmi">>, Data).

%% @doc Update an object.
-spec update(pid(),
             object_oid(),      %% Oid
             map()              %% Data to store
            ) -> {ok, map()} | {error, term()}.
update(Pid, Oid, Data) when is_pid(Pid), is_binary(Oid), is_map(Data) ->
%    ?nebMsg("Entry"),
    {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
    case Mod:update(Pid, Oid, Data) of
        ok ->
            nebula2_utils:set_cache(Data),
            {ok, Data};
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

nebula2_db_test_() ->
    {foreach,
     fun() ->
             Mod = ?TestMetadataModule,
             meck:new(jsx, [passthrough]),
             meck:new(Mod, [non_strict]),
             meck:new(nebula2_utils)
     end,
     fun(_) ->
             Mod = ?TestMetadataModule,
             meck:unload(jsx),
             meck:unload(Mod),
             meck:unload(nebula2_utils)
     end,
     [{"Test available/1",
       fun() ->
            Pid = self(),
            Mod = ?TestMetadataModule,
            meck:expect(Mod, available, [Pid], true),
            ?assert(available(Pid)),
            meck:expect(Mod, available, [Pid], false),
            ?assertNot(available(Pid)),
            ?assertException(error, function_clause, available(not_a_pid)),
            ?assert(meck:validate(Mod))
       end
      },
      {"Test create/3",
       fun() ->
            Pid = self(),
            TestMap = jsx:decode(?TestBinary, [return_maps]),
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
            meck:sequence(Mod, put, 3, [{ok, ?TestOid},
                                        {error,ioerror}
                                       ]),
            meck:expect(nebula2_utils, set_cache, ['_'], '_'),
            ?assertMatch({ok, ?TestOid},  create(Pid, ?TestOid, TestMap)),
            ?assertMatch({error,ioerror}, create(Pid, ?TestOid, TestMap)),
            ?assertException(error, function_clause, create(not_a_pid, ?TestOid, TestMap)),
            ?assertException(error, function_clause, create(Pid, not_an_oid, TestMap)),
            ?assertException(error, function_clause, create(Pid, ?TestOid, not_a_map)),
            ?assert(meck:validate(Mod)),
            ?assert(meck:validate(nebula2_utils))
       end
      },
      {"Test delete/2",
       fun() ->
            Pid = self(),
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
            meck:expect(Mod, delete, [Pid, ?TestOid], ok),
            meck:expect(nebula2_utils, delete_cache, ['_'], '_'),
            ?assertMatch(ok, delete(Pid, ?TestOid)),
            meck:expect(Mod, delete, [Pid, ?TestOid], {error,ioerror}),
            ?assertMatch({error,ioerror}, delete(Pid, ?TestOid)),
            ?assertException(error, function_clause, delete(not_a_pid, ?TestOid)),
            ?assertException(error, function_clause, delete(Pid, not_a_binary)),
            ?assert(meck:validate(Mod)),
            ?assert(meck:validate(nebula2_utils))
       end
      },
      {"Test get_domain_maps/1",
       fun() ->
            Pid = self(),
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            Path = ?TestSystemDomainHash ++ "/system_configuration/"++ "domain_maps",
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
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
            ?assert(meck:validate(Mod)),
            ?assert(meck:validate(nebula2_utils))
       end
      },
      {"Test marshall/1",
       fun() ->
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            Key = binary_to_list(?TestSearchKey),
            CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
            meck:expect(nebula2_utils, make_search_key, [TestMap], Key),
            ?assertMatch(CdmiMap, marshall(TestMap)),
            ?assertMatch(CdmiMap, marshall(TestMap, Key)),
            ?assertException(error, function_clause, marshall(not_a_map)),
            ?assertException(error, function_clause, marshall(TestMap, not_a_list)),
            ?assert(meck:validate(nebula2_utils))
       end
      },
      {"Test read/2",
       fun() ->
            Pid = self(),
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
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
            ?assert(meck:validate(nebula2_utils)),
            ?assert(meck:validate(Mod))
       end
      },
      {"Test search/2",
       fun() ->
            Pid = self(),
            Path = ?TestSystemDomainHash ++ "/system_configuration/"++ "domain_maps",
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            State = {Pid, maps:new()},
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
            meck:expect(nebula2_utils, get_cache, [Path], {ok, TestMap}),
            ?assertMatch({ok, TestMap}, search(Path, State)),
            meck:expect(nebula2_utils, get_cache, [Path], {error, not_found}),
            meck:expect(Mod, search, [Path, State], {ok, TestMap}),
            meck:expect(nebula2_utils, set_cache, ['_'], {ok, TestMap}),
            ?assertMatch({ok, TestMap}, search(Path, {Pid, maps:new()})),
            meck:expect(Mod, search, [Path, State], {error, not_found}),
            ?assertMatch({error, not_found}, search(Path, {Pid, maps:new()})),
            ?assertException(error, function_clause, search(not_a_list, State)),
            ?assertException(error, function_clause, search(Path, not_a_tuple)),
            ?assert(meck:validate(nebula2_utils)),
            ?assert(meck:validate(Mod))
       end
      },
      {"Test unmarshall/1",
       fun() ->
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
            ?assertMatch(TestMap, unmarshall(CdmiMap)),
            ?assertException(error, function_clause, unmarshall(not_a_map))
       end
      },
      {"Test update/3 success",
       fun() ->
            Pid = self(),
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
            meck:expect(Mod, update, [Pid, ?TestContainer7Oid, '_'], ok),
            meck:expect(nebula2_utils, set_cache, [CdmiMap], {ok, TestMap}),
            ?assertMatch({ok, CdmiMap}, update(Pid, ?TestContainer7Oid, CdmiMap)),
            ?assert(meck:validate(nebula2_utils)),
            ?assert(meck:validate(Mod))
       end
      },
      {"Test update/3 fail",
       fun() ->
            Pid = self(),
            TestMap = jsx:decode(?TestDomainMaps, [return_maps]),
            CdmiMap = maps:from_list([{<<"cdmi">>, TestMap}, {<<"sp">>, ?TestSearchKey}]),
            {ok, Mod} = ?GET_ENV(nebula2, cdmi_metadata_module),
            meck:expect(Mod, update, [Pid, ?TestContainer7Oid, '_'], {error, notfound}),
            ?assertMatch({error, notfound}, update(Pid, ?TestContainer7Oid, CdmiMap)),
            ?assert(meck:validate(Mod))
       end
      },
      {"Test update/3 contract",
       fun() ->
            Pid = self(),
            ?assertException(error, function_clause, update(not_a_pid, ?TestContainer7Oid, maps:new())),
            ?assertException(error, function_clause, update(Pid, not_a_binary, maps:new())),
            ?assertException(error, function_clause, update(Pid, ?TestContainer7Oid, not_a_map))
       end
      }
     ]
    }.

-endif.
