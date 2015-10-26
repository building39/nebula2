%% @author mmartin
%% @doc Handles all CRUD requests to the CDMI metadata backend.
%% @doc Initial backend is riak. This module could be replaced to
%% @doc make use of some other storage backend.

-module(nebula2_db).

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
available(Pid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    Mod:available(Pid).
    
%% @doc Create an object.
-spec create(pid(),
                      object_oid(),   %% Oid
                      map()           %% Data to store
                     ) -> {'error', _} | {'ok', _}.
create(Pid, Oid, Data) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    Response = Mod:put(Pid, Oid, Data),
    nebula2_utils:set_cache(Data),
    Response.
    
%% @doc Delete an object
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    nebula2_utils:delete_cache(Oid),
    Mod:delete(Pid, Oid).

%% @doc Get the domain maps.
-spec nebula2_db:get_domain_maps(pid()) -> list().
get_domain_maps(Pid) ->
    lager:debug("Entry"),
    Domain = nebula2_utils:get_domain_hash(?SYSTEM_DOMAIN_URI),
    Path = Domain ++ "/system_configuration/"++ "domain_maps",
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            lager:debug("1 Cache Hit: Path: ~p", [Path]),
            nebula2_utils:get_value(<<"value">>, Data, <<"[]">>);
        _ ->
            lager:debug("1 Cache Miss: ~p", [Path]),
            case Mod:get_domain_maps(Pid, Path) of
                {ok, DomainMaps} ->
                    nebula2_utils:set_cache(DomainMaps),
                    nebula2_utils:get_value(<<"value">>, DomainMaps, <<"[]">>);
                _ ->
                    <<"[]">>
            end
    end.

-spec marshall(map()) -> map().
marshall(Data) ->
    lager:debug("Entry"),
    SearchKey = nebula2_utils:make_search_key(Data),
    marshall(Data, SearchKey).

-spec marshall(map(), binary()) -> map().
marshall(Data, SearchKey) ->
    lager:debug("Entry"),
    Data2 = maps:new(),
    ObjectId = nebula2_utils:get_value(<<"objectID">>, Data),
    Data3 = nebula2_utils:put_value(<<"k">>, ObjectId, Data2),
    Data4 = nebula2_utils:put_value(<<"sp">>, SearchKey, Data3),
    maps:put(<<"cdmi">>, Data, Data4).

%% @doc Read an object
-spec read(pid(), object_oid()) -> {ok, map()}|{error, term()}.
read(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    read(Pid, binary_to_list(Oid));
read(Pid, Oid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    case nebula2_utils:get_cache(Oid) of
        {ok, Data} ->
            lager:debug("Cache Hit: Oid: ~p", [Oid]),
            {ok, Data};
        _ ->
            lager:debug("Cache Miss: Oid: ~p", [Oid]),
            case Mod:get(Pid, list_to_binary(Oid)) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                {error, Term} ->
                    {error, Term}
            end
    end.

%% @doc Search an index for objects.
-spec search(string(), cdmi_state()) -> {error, 404|500}|{ok, map()}.
search(Path, State) ->
    lager:debug("Entry"),
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    case nebula2_utils:get_cache(Path) of
        {ok, Data} ->
            lager:debug("2 Cache Hit: Key: ~p", [Path]),
            {ok, Data};
        _ ->
            lager:debug("2 Cache Miss: ~p", [Path]),
            %% die("bail"),
            case Mod:search(Path, State) of
                {ok, Data} ->
                    nebula2_utils:set_cache(Data),
                    {ok, Data};
                Response ->
                    Response
            end
    end.


-spec unmarshall(map()) -> map().
unmarshall(Data) ->
    lager:debug("Entry"),
    maps:get(<<"cdmi">>, Data).

%% @doc Update an object.
-spec update(pid(),
             object_oid(),      %% Oid
             map()              %% Data to store
            ) -> ok | {error, term()}.
update(Pid, Oid, Data) when is_binary(Oid) ->
    lager:debug("Entry"),
    update(Pid, binary_to_list(Oid), Data);
update(Pid, Oid, Data) ->
    lager:debug("Entry"),
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    case Mod:update(Pid, Oid, jsx:encode(Data)) of
        ok ->
            nebula2_utils:set_cache(Data),
            ok;
        Failure ->
            Failure
    end.
    
%% ====================================================================
%% Internal functions
%% ====================================================================
%%die(Dagger) when is_binary(Dagger) ->
%%    ok.
