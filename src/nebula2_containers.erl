%% @author mmartin
%% @doc Handle CDMI container objects.

-module(nebula2_containers).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([
            new_container/2,
            update_container/3
        ]).

%% @doc Create a new CDMI container
-spec new_container(cowboy_req:req(), cdmi_state()) -> {boolean(), Req, cdmi_state()}
        when Req::cowboy_req:req().
new_container(Req, State) ->
    ?nebMsg("Entry"),
    {_, EnvMap} = State,
    Domain = nebula2_utils:get_value(<<"domainURI">>, EnvMap),
    lager:debug("Domain: ~p", [Domain]),
    ObjectType = ?CONTENT_TYPE_CDMI_CONTAINER,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    NewBody
            catch
                error:badarg ->
                    throw(badjson)
            end,
    Response = case nebula2_utils:create_object(State, ObjectType, Body2) of
                   {true, Data} ->
                       Data2 = nebula2_db:unmarshall(Data),
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req2), State};
                   false ->
                       {false, Req2, State}
               end,
    Response.

%% @doc Update a CDMI container
-spec update_container(cowboy_req:req(), map(), object_oid()) -> {ok, json_value()}.
update_container(Req, State, Oid) when is_map(State); is_binary(Oid)->
    ?nebMsg("Entry"),
    {Pid, _} = State,
    {ok, Body, Req2} = cowboy_req:body(Req),
    NewData = try jsx:decode(Body, [return_maps]) of
                  NewBody -> NewBody
              catch
                  error:badarg ->
                      throw(badjson)
              end,
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldData2 = nebula2_db:unmarshall(OldData),
    OldMetaData = nebula2_utils:get_value(<<"metadata">>, OldData2, maps:new()),
    NewMetaData = nebula2_utils:get_value(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData2, NewData),
    Data2 = nebula2_utils:put_value(<<"metadata">>, MetaData, Data),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>],
    Data3 = nebula2_utils:update_data_system_metadata(CList, Data2, State),
    ?nebFmt("Data3: ~p", [Data3]),
    Response = case nebula2_db:update(Pid, Oid, nebula2_db:marshall(Data3)) of
                   ok ->
                       {true, Req2, State};
                   _  ->
                       {false, Req, State}
               end,
    Response.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% ====================================================================
%% eunit tests
%% ====================================================================
-ifdef(EUNIT).
-endif.
