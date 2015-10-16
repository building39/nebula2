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
         read/2,
         search/2,
         search/3,
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
    Mod:put(Pid, Oid, Data).
    
%% @doc Delete an object
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    mcd:delete(?MEMCACHE, Oid),
    Mod:delete(Pid, Oid).

%% @doc Get the domain maps.
-spec nebula2_db:get_domain_maps(pid()) -> list().
get_domain_maps(Pid) ->
    lager:debug("Entry"),
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    Mod:get_domain_maps(Pid).

%% @doc Read an object
-spec read(pid(), object_oid()) -> {ok, map()}|{error, term()}.
read(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    read(Pid, binary_to_list(Oid));
read(Pid, Oid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            {ok, Data};
        _ ->
            case Mod:get(Pid, list_to_binary(Oid)) of
                {ok, Object} ->
                    Data = jsx:decode(Object, [return_maps]),
                    {ok, _R} = mcd:set(?MEMCACHE, Oid, Data, ?MEMCACHE_EXPIRY),
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
    case Mod:search(Path, State) of
        {ok, Data} ->
            {ok, jsx:decode(Data, [return_maps])};
        Response ->
            Response
    end.

%% @doc Search an index for objects, but don't search on domainUri
-spec search(string(), cdmi_state(), nodomain) -> {error, 404|500}|{ok, map()}.
search(Path, State, nodomain) ->
    lager:debug("Entry"),
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    case Mod:search(Path, State, nodomain) of
        {ok, Data} ->
            {ok, jsx:decode(Data, [return_maps])};
        Response ->
            Response
    end.

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
        {ok, _} ->
            mcd:set(?MEMCACHE, Oid, Data, ?MEMCACHE_EXPIRY),
            ok;
        Failure ->
            Failure
    end.
    
%% ====================================================================
%% Internal functions
%% ====================================================================


