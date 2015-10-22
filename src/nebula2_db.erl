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
    set_cache(Data),
    Mod:put(Pid, Oid, marshall(Data)).
    
%% @doc Delete an object
-spec delete(pid(), object_oid()) -> ok | {error, term()}.
delete(Pid, Oid) when is_binary(Oid) ->
    lager:debug("Entry"),
    delete(Pid, binary_to_list(Oid));
delete(Pid, Oid) ->
    {ok, Mod} = application:get_env(nebula2, cdmi_metadata_module),
    lager:debug("Entry"),
    delete_cache(Oid),
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
    case get_cache(Oid) of
        {ok, Data} ->
            lager:debug("Cache Hit: Data: ~p", [Data]),
            {ok, Data};
        _ ->
            case Mod:get(Pid, list_to_binary(Oid)) of
                {ok, Object} ->
                    Data = unmarshall(Object),
                    lager:debug("Data: ~p", [Data]),
                    set_cache(Data),
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
            lager:debug("Data: ~p", [Data]),
            {ok, unmarshall(Data)};
        Response ->
            lager:debug("Response: ~p", [Response]),
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
    case Mod:update(Pid, Oid, jsx:encode(marshall(Data))) of
        {ok, _} ->
            set_cache(Data),
            ok;
        Failure ->
            Failure
    end.
    
%% ====================================================================
%% Internal functions
%% ====================================================================


         
-spec marshall(map()) -> map().
marshall(Data) ->
    lager:debug("Entry"),
    lager:debug("Data: ~p", [Data]),
    Data2 = maps:new(),
    ObjectId = maps:get(<<"objectID">>, Data),
    Data3 = maps:put(<<"k">>, ObjectId, Data2),
    SearchKey = nebula2_utils:make_search_key(Data),
    lager:debug("Search Key: ~p:", [SearchKey]),
    Data4 = maps:put(<<"sp">>, SearchKey, Data3),
    % D = maps:put(<<"cdmi">>, jsx:encode(Data), Data4),
    D = maps:put(<<"cdmi">>, Data, Data4),
    lager:debug("Exit: ~p", [D]),
    D.

-spec unmarshall(map()) -> map().
unmarshall(Data) ->
    lager:debug("Entry"),
    lager:debug("Data: ~p", [Data]),
    % D = jsx:decode(maps:get(<<"cdmi">>, Data), [return_maps]),
    D = maps:get(<<"cdmi">>, Data),
    lager:debug("Exit: ~p", [D]),
    D.

-spec delete_cache(object_oid()) -> {ok | error, deleted | notfound}.
delete_cache(Oid) ->
    lager:debug("Entry"),
    lager:debug("Oid: ~p", [Oid]),
    case mcd:get(?MEMCACHE, Oid) of
        {ok, Data} ->
            SearchKey = newbula2_utils:make_search_key(Data),
            mcd:delete(?MEMCACHE, SearchKey),
            mcd:delete(?MEMCACHE, Oid);
        _ ->
            {error, notfound}
    end.

-spec get_cache(object_oid()) -> {ok | error, deleted | notfound}.
get_cache(_Oid) ->
    {error, notfound}.

-spec set_cache(map()) -> {ok, map()}.
set_cache(Data) ->
    lager:debug("Entry"),
    lager:debug("Data: ~p", [Data]),
    SearchKey = nebula2_utils:make_search_key(Data),
    ObjectId = maps:get(<<"objectID">>, Data),
    mcd:set(?MEMCACHE, ObjectId, Data, ?MEMCACHE_EXPIRY),
    mcd:set(?MEMCACHE, SearchKey, Data, ?MEMCACHE_EXPIRY).
