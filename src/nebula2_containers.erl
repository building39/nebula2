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
-spec nebula2_containers:new_container(cowboy_req:req(), cdmi_state()) -> {boolean(), Req, cdmi_state()}
        when Req::cowboy_req:req().
new_container(Req, State) ->
    lager:debug("Entry"),
    ObjectType = ?CONTENT_TYPE_CDMI_CONTAINER,
    {ok, Body, Req2} = cowboy_req:body(Req),
    Body2 = try jsx:decode(Body, [return_maps]) of
                NewBody ->
                    nebula2_db:marshall(NewBody)
            catch
                error:badarg ->
                    throw(badjson)
            end,
    lager:debug("Body2: ~p", [Body2]),
    Response = case nebula2_utils:create_object(Req2, State, ObjectType, Body2) of
                   {true, Req3, Data} ->
                       Data2 = nebula2_db:unmarshall(Data),
                       {true, cowboy_req:set_resp_body(jsx:encode(maps:to_list(Data2)), Req3), State};
                   false ->
                       {false, Req2, State}
               end,
    Response.

%% @doc Update a CDMI container
-spec nebula2_containers:update_container(cowboy_req:req(), pid(), object_oid()) -> {ok, json_value()}.
update_container(Req, State, Oid) ->
    lager:debug("Entry"),
    {Pid, _} = State,
    {ok, B, Req2} = cowboy_req:body(Req),
    Body = nebula2_db:marshall(B),
    NewData = try jsx:decode(Body, [return_maps]) of
                  NewBody -> NewBody
              catch
                  error:badarg ->
                      throw(badjson)
              end,
    {ok, OldData} = nebula2_db:read(Pid, Oid),
    OldMetaData = nebula2_utils:get_value(<<"metadata">>, OldData, maps:new()),
    NewMetaData = nebula2_utils:get_value(<<"metadata">>, NewData, maps:new()),
    MetaData = maps:merge(OldMetaData, NewMetaData),
    Data = maps:merge(OldData, NewData),
    Data2 = nebula2_utils:put_value(<<"metadata">>, MetaData, Data),
    CList = [<<"cdmi_atime">>,
             <<"cdmi_mtime">>,
             <<"cdmi_acount">>,
             <<"cdmi_mcount">>],
    Data3 = nebula2_utils:update_data_system_metadata(CList, Data2, State),
    Response = case nebula2_db:update(Pid, Oid, Data3) of
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
